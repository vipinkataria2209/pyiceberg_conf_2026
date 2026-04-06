# Apache Iceberg Write Path — Ultra-Detailed Architecture

This document traces every step of the write path in PyIceberg at source-code level, from user API calls through data file generation, manifest creation, snapshot management, and atomic catalog commit.

---

## Complete Write Path Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│  USER CODE                                                               │
│  table.append(df)  /  table.overwrite(df)  /  table.delete(filter)      │
└──────────────┬───────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  1. TRANSACTION LAYER                                                    │
│     Transaction() → stage updates → _apply()                            │
└──────────────┬───────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  2. SCHEMA VALIDATION                                                    │
│     _check_pyarrow_schema_compatible()                                  │
└──────────────┬───────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  3. SNAPSHOT PRODUCER (context manager)                                   │
│     _FastAppendFiles / _MergeAppendFiles / _OverwriteFiles / _Delete    │
└──────────────┬───────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  4. DATA FILE GENERATION                                                 │
│     4a. Partition Determination  (_determine_partitions)                 │
│     4b. Bin Packing              (bin_pack_arrow_table)                  │
│     4c. Parallel Parquet Writing (write_file → write_parquet)           │
│     4d. Statistics Collection    (parquet metadata → DataFile)           │
└──────────────┬───────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  5. MANIFEST CREATION                                                    │
│     5a. Write new manifest for ADDED files                              │
│     5b. Carry forward EXISTING manifests from parent snapshot           │
│     5c. Write manifest for DELETED files (if any)                       │
│     (all 3 happen in parallel via concurrent.futures)                   │
└──────────────┬───────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  6. MANIFEST LIST CREATION                                               │
│     snap-{snapshot_id}-{attempt}-{uuid}.avro                            │
└──────────────┬───────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  7. SNAPSHOT OBJECT CREATION                                             │
│     Snapshot(id, parent_id, sequence_number, timestamp, summary)        │
└──────────────┬───────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  8. ATOMIC CATALOG COMMIT                                                │
│     updates[] + requirements[] → Catalog.commit_table()                 │
│     → new metadata.json written                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Entry Points — Four Write Operations

**Source**: `pyiceberg/table/__init__.py`

### 1a. table.append(df)

```python
def append(self, df: pa.Table, snapshot_properties: dict[str, str] = EMPTY_DICT,
           branch: str | None = MAIN_BRANCH) -> None:
```

**What it does**:
1. Creates a `Transaction` with `_autocommit=True`
2. Calls `transaction.append(df)`
3. Transaction auto-commits on completion

**Flow**:
```
table.append(df)
    │
    ├── with self.transaction() as txn:    ← autocommit=True
    │       txn.append(df)
    │
    └── Transaction.append():
        ├── Validate df is not empty
        ├── _check_pyarrow_schema_compatible(table_schema, df.schema)
        ├── Downcast timestamps ns→µs if configured
        ├── Create _FastAppendFiles snapshot producer (context manager)
        │   └── with _FastAppendFiles(...) as append:
        │       ├── data_files = _dataframe_to_data_files(...)
        │       └── for data_file in data_files:
        │               append.append_data_file(data_file)
        └── On context exit → _commit()
```

### 1b. table.overwrite(df, overwrite_filter)

```python
def overwrite(self, df: pa.Table, overwrite_filter: BooleanExpression = ALWAYS_TRUE,
              snapshot_properties: dict[str, str] = EMPTY_DICT,
              branch: str | None = MAIN_BRANCH) -> None:
```

**What it does**:
1. If `overwrite_filter` is provided: first deletes matching rows, then appends new data
2. If `overwrite_filter` is `ALWAYS_TRUE`: replaces ALL data (full table overwrite)

**Flow**:
```
table.overwrite(df, filter)
    │
    ├── with self.transaction() as txn:
    │       txn.overwrite(df, filter)
    │
    └── Transaction.overwrite():
        ├── If filter != ALWAYS_TRUE and snapshot exists:
        │   └── txn.delete(filter)         ← delete matching rows first
        ├── Validate df schema
        ├── Create _OverwriteFiles snapshot producer
        │   └── with _OverwriteFiles(...) as overwrite:
        │       ├── data_files = _dataframe_to_data_files(...)
        │       └── for data_file in data_files:
        │               overwrite.append_data_file(data_file)
        └── On context exit → _commit()
```

### 1c. table.delete(delete_filter)

```python
def delete(self, delete_filter: BooleanExpression | str,
           snapshot_properties: dict[str, str] = EMPTY_DICT,
           branch: str | None = MAIN_BRANCH) -> None:
```

**What it does**: Deletes rows matching the filter. Uses **copy-on-write** — rewrites files that partially match.

**Flow**:
```
table.delete(filter)
    │
    └── Transaction.delete():
        ├── Bind filter to table schema
        ├── Create _DeleteFiles snapshot producer
        │   └── with _DeleteFiles(...) as delete_snap:
        │       │
        │       ├── For each existing FileScanTask:
        │       │   ├── STRICT metrics evaluator: "Does EVERY row match?"
        │       │   │   └── YES → delete entire file (no rewrite needed)
        │       │   │
        │       │   ├── INCLUSIVE metrics evaluator: "Could ANY row match?"
        │       │   │   └── NO → skip file entirely (keep as-is)
        │       │   │
        │       │   └── PARTIAL match → Copy-on-write:
        │       │       ├── Read file into Arrow table
        │       │       ├── Apply NOT(filter) to keep surviving rows
        │       │       ├── Write surviving rows as NEW data file(s)
        │       │       ├── delete_snap.delete_data_file(old_file)
        │       │       └── delete_snap.append_data_file(new_file)
        │       │
        └── On context exit → _commit()
```

### 1d. table.dynamic_partition_overwrite(df)

```python
def dynamic_partition_overwrite(self, df: pa.Table,
                                  snapshot_properties: dict[str, str] = EMPTY_DICT,
                                  branch: str | None = MAIN_BRANCH) -> None:
```

**What it does**: Replaces only the partitions present in the input DataFrame. Partitions NOT in the input are untouched.

**Flow**:
```
table.dynamic_partition_overwrite(df)
    │
    └── Transaction.dynamic_partition_overwrite():
        ├── Determine which partitions are in the input df
        ├── Build overwrite filter for just those partitions
        ├── Delete matching partitions
        └── Append new data
```

---

## 2. Transaction Architecture

**Source**: `pyiceberg/table/__init__.py :: Transaction`

### Transaction Object Structure

```
Transaction {
    _table:       Table              ← reference to the table
    _autocommit:  bool               ← True for table.append() shorthand
    _updates:     tuple[TableUpdate] ← staged metadata changes
    _requirements: tuple[TableReq]   ← preconditions for commit
}
```

### Transaction Lifecycle

```
┌─────────────────────────────────────────────────────────────────────┐
│                    TRANSACTION LIFECYCLE                              │
│                                                                      │
│  1. CREATION                                                         │
│     txn = Transaction(table, autocommit=True)                       │
│     • Takes snapshot of current table metadata                       │
│     • Initializes empty updates[] and requirements[]                 │
│                                                                      │
│  2. STAGING (one or more operations)                                 │
│     txn.append(df)          → stages AddSnapshotUpdate              │
│     txn.overwrite(df)       → stages AddSnapshotUpdate              │
│     txn.delete(filter)      → stages AddSnapshotUpdate              │
│     txn.update_schema()     → stages AddSchemaUpdate                │
│     txn.update_spec()       → stages AddPartitionSpecUpdate         │
│                                                                      │
│     _stage() method:                                                 │
│     • Appends updates to _updates tuple                              │
│     • Appends requirements to _requirements tuple                    │
│     • Deduplicates: same update type replaces previous               │
│                                                                      │
│  3. COMMIT                                                           │
│     txn.commit_transaction()                                        │
│     • Sends (updates, requirements) to catalog                      │
│     • Catalog validates requirements (CAS)                           │
│     • Catalog applies updates atomically                             │
│     • Returns new table metadata                                     │
│                                                                      │
│  4. AUTO-COMMIT (if autocommit=True)                                │
│     When Transaction context manager __exit__:                       │
│     • If no exception → commit_transaction()                        │
│     • If exception → rollback (no commit)                           │
└─────────────────────────────────────────────────────────────────────┘
```

### _stage() — How Updates Are Staged

```python
def _stage(self, update: TableUpdate, requirement: TableRequirement) -> None:
    # Deduplication: if same type of update already staged, replace it
    existing = [u for u in self._updates if type(u) != type(update)]
    existing.append(update)
    self._updates = tuple(existing)

    # Same for requirements
    existing_reqs = [r for r in self._requirements if type(r) != type(requirement)]
    existing_reqs.append(requirement)
    self._requirements = tuple(existing_reqs)
```

---

## 3. Schema Validation — Deep Dive

**Source**: `pyiceberg/io/pyarrow.py :: _check_pyarrow_schema_compatible()`

This is the gatekeeper. If validation fails, no data is written.

```
┌─────────────────────────────────────────────────────────────────────┐
│           SCHEMA VALIDATION PIPELINE                                 │
│                                                                      │
│  Input:                                                              │
│    table_schema:  Schema(id:int64, name:string, price:float64)      │
│    df_schema:     pa.schema(id:int64, name:utf8, price:float64)     │
│                                                                      │
│  Step 1: GET NAME MAPPING                                            │
│  ├── Check table properties for "schema.name-mapping.default"       │
│  ├── If exists → parse JSON name mapping                            │
│  └── If not → build from current table schema                       │
│                                                                      │
│  Step 2: CONVERT PYARROW SCHEMA → ICEBERG SCHEMA                   │
│  ├── pyarrow_to_schema(df.schema, name_mapping)                     │
│  ├── Maps PyArrow types to Iceberg types:                           │
│  │   pa.int64()    → LongType()                                     │
│  │   pa.utf8()     → StringType()                                   │
│  │   pa.float64()  → DoubleType()                                   │
│  │   pa.timestamp  → TimestampType()                                │
│  │   pa.list_()    → ListType()                                     │
│  │   pa.struct()   → StructType()                                   │
│  │   pa.map_()     → MapType()                                      │
│  └── Assigns field IDs from name mapping                            │
│                                                                      │
│  Step 3: CHECK COMPATIBILITY                                         │
│  ├── For each field in df_schema:                                    │
│  │   ├── Does it exist in table_schema? (by name or field ID)       │
│  │   ├── Is the type compatible?                                     │
│  │   │   ├── Exact match → OK                                       │
│  │   │   ├── Promotable (int32→int64, float32→float64) → OK        │
│  │   │   └── Incompatible → RAISE SchemaError                       │
│  │   └── Is it required but nullable in df? → WARNING               │
│  │                                                                   │
│  ├── Extra columns in df NOT in table? → RAISE SchemaError          │
│  │   "PyArrow table contains column 'extra_col' not in table schema"│
│  │                                                                   │
│  └── Missing columns in df that are in table?                       │
│      ├── If optional (nullable) → OK (will be filled with nulls)    │
│      └── If required → RAISE SchemaError                            │
│                                                                      │
│  Step 4: TIMESTAMP DOWNCAST CHECK                                    │
│  ├── Property: "write.parquet.timestamp.downcast-ns-to-us-on-write" │
│  ├── If True and df has timestamp[ns] columns:                      │
│  │   └── Will be downcast to timestamp[µs] before writing           │
│  └── If False and df has timestamp[ns]:                             │
│      └── RAISE error (Iceberg only supports µs precision)           │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 4. Snapshot Producer Architecture

**Source**: `pyiceberg/table/update/snapshot.py`

The snapshot producer is the core write engine. It uses the **context manager** protocol.

### 4a. Producer Class Hierarchy

```
_SnapshotProducer (abstract base)
    │
    ├── _FastAppendFiles
    │   • Used by: table.append()
    │   • Behavior: Creates NEW manifest, carries forward ALL existing manifests as-is
    │   • No manifest merging — fastest write path
    │
    ├── _MergeAppendFiles
    │   • Used by: table.append() when merge enabled
    │   • Behavior: Creates new manifest AND may merge small existing manifests
    │   • Reduces manifest count over time (compaction)
    │   • Controlled by: write.manifest.merge.enabled (default: false)
    │
    ├── _OverwriteFiles
    │   • Used by: table.overwrite()
    │   • Behavior: Can both ADD and DELETE files in one snapshot
    │   • Creates manifests for new files + delete entries for removed files
    │
    └── _DeleteFiles
        • Used by: table.delete()
        • Behavior: Only DELETE files (whole or rewritten)
        • Uses strict + inclusive metrics evaluators to decide strategy
```

### 4b. _SnapshotProducer Base Class — Internal State

```
_SnapshotProducer {
    _operation:       Operation          ← APPEND, OVERWRITE, DELETE
    _snapshot_id:     int                ← unique ID (generated)
    _parent_snapshot_id: int | None      ← previous snapshot
    _added_data_files:   list[DataFile]  ← files being added
    _deleted_data_files: list[DataFile]  ← files being removed
    _io:              FileIO             ← storage I/O
    _commit_uuid:     uuid4             ← unique per commit attempt
    _transaction:     Transaction        ← parent transaction
    _branch:          str                ← "main" or branch name
}
```

### 4c. Context Manager Protocol

```python
def __enter__(self):
    return self       # Return producer for chaining: with Producer() as p:

def __exit__(self, exc_type, exc_val, exc_tb):
    if exc_type is None:   # No exception → commit
        self._commit()
    # If exception → do nothing (rollback)
```

### 4d. append_data_file() / delete_data_file()

```python
def append_data_file(self, data_file: DataFile) -> None:
    self._added_data_files.append(data_file)

def delete_data_file(self, data_file: DataFile) -> None:
    self._deleted_data_files.append(data_file)
```

These just accumulate files. The actual work happens in `_commit()`.

---

## 5. Data File Generation — _dataframe_to_data_files()

**Source**: `pyiceberg/io/pyarrow.py`

This is the orchestrator that converts a PyArrow Table into Parquet files on storage.

```
┌─────────────────────────────────────────────────────────────────────┐
│         _dataframe_to_data_files() ORCHESTRATION                     │
│                                                                      │
│  Input:                                                              │
│    table_metadata: TableMetadata                                     │
│    df:             pa.Table (validated)                              │
│    io:             FileIO                                            │
│    write_uuid:     UUID                                              │
│                                                                      │
│  Step 1: READ CONFIGURATION                                         │
│  ├── target_file_size = properties["write.target-file-size-bytes"]  │
│  │   Default: 536,870,912 (512 MB)                                  │
│  │                                                                   │
│  ├── Get current partition spec from table metadata                  │
│  └── Get current schema from table metadata                         │
│                                                                      │
│  Step 2: CONVERT SCHEMA                                              │
│  ├── table_schema → pyarrow_schema (for writing)                    │
│  ├── Sanitize column names if needed                                │
│  │   (replace special chars with underscore)                        │
│  └── Apply timestamp downcast if configured                         │
│                                                                      │
│  Step 3: BRANCH — PARTITIONED OR NOT?                               │
│                                                                      │
│  ┌────────────────────────┐  ┌──────────────────────────────────┐  │
│  │  UNPARTITIONED          │  │  PARTITIONED                      │  │
│  │  (spec has 0 fields)    │  │  (spec has 1+ fields)             │  │
│  │                          │  │                                    │  │
│  │  1. bin_pack_arrow_table │  │  1. _determine_partitions(df)     │  │
│  │     (df, target_size)    │  │     → list[_TablePartition]       │  │
│  │     → list[WriteTask]    │  │                                    │  │
│  │                          │  │  2. For each partition:            │  │
│  │                          │  │     bin_pack_arrow_table(          │  │
│  │                          │  │       partition.arrow_table,       │  │
│  │                          │  │       target_size                  │  │
│  │                          │  │     ) → list[WriteTask]            │  │
│  └────────────┬─────────────┘  └──────────────┬───────────────────┘  │
│               │                                │                      │
│               └────────────┬───────────────────┘                     │
│                            ▼                                          │
│  Step 4: PARALLEL WRITE                                              │
│  write_file(io, table_metadata, tasks) → Iterator[DataFile]         │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 5a. Partition Determination — _determine_partitions()

**Source**: `pyiceberg/io/pyarrow.py`

```
┌─────────────────────────────────────────────────────────────────────┐
│         _determine_partitions() — DETAILED FLOW                      │
│                                                                      │
│  Input: pa.Table with columns [id, name, price, event_date]         │
│  Partition Spec: [year(event_date), bucket(id, 16)]                 │
│                                                                      │
│  Step 1: APPLY TRANSFORMS TO GET PARTITION COLUMNS                  │
│                                                                      │
│  For each partition field in spec:                                   │
│                                                                      │
│  ┌───────────────────────────────────────────────────────────┐      │
│  │ Partition Field: year(event_date)                          │      │
│  │                                                            │      │
│  │ Source column: event_date                                  │      │
│  │ Transform:     YearTransform                               │      │
│  │                                                            │      │
│  │ pyarrow_transform() applies:                               │      │
│  │   event_date: [2024-01-15, 2024-03-20, 2023-12-01]       │      │
│  │   → year_col: [54, 54, 53]  (years since 1970)            │      │
│  │                                                            │      │
│  │ Added as hidden column: __partition_0__                    │      │
│  └───────────────────────────────────────────────────────────┘      │
│  ┌───────────────────────────────────────────────────────────┐      │
│  │ Partition Field: bucket(id, 16)                            │      │
│  │                                                            │      │
│  │ Source column: id                                          │      │
│  │ Transform:     BucketTransform(num_buckets=16)             │      │
│  │                                                            │      │
│  │ pyarrow_transform() applies:                               │      │
│  │   id: [1001, 2002, 3003]                                  │      │
│  │   → bucket_col: [5, 12, 3]  (mmh3 hash % 16)             │      │
│  │                                                            │      │
│  │ Added as hidden column: __partition_1__                    │      │
│  └───────────────────────────────────────────────────────────┘      │
│                                                                      │
│  Step 2: GROUP BY UNIQUE PARTITION VALUE COMBINATIONS               │
│                                                                      │
│  df.group_by([__partition_0__, __partition_1__]).aggregate([])       │
│                                                                      │
│  Unique combinations:                                                │
│  ┌──────────────────┬───────────────────┐                           │
│  │ __partition_0__   │ __partition_1__    │                           │
│  ├──────────────────┼───────────────────┤                           │
│  │ 54 (year=2024)   │ 5 (bucket=5)      │                           │
│  │ 54 (year=2024)   │ 12 (bucket=12)    │                           │
│  │ 53 (year=2023)   │ 3 (bucket=3)      │                           │
│  └──────────────────┴───────────────────┘                           │
│                                                                      │
│  Step 3: FILTER DATA PER PARTITION                                  │
│                                                                      │
│  For each unique combo:                                              │
│    filtered_df = df.filter(                                          │
│      (__partition_0__ == 54) AND (__partition_1__ == 5)              │
│    )                                                                 │
│                                                                      │
│  Step 4: BUILD PARTITION KEY                                        │
│                                                                      │
│  PartitionKey(                                                       │
│    raw_partition_field_data = [54, 5],                               │
│    partition_spec = spec,                                            │
│    schema = table_schema                                             │
│  )                                                                   │
│  → Converts internal values to Iceberg partition record:            │
│    {event_date_year: 2024, id_bucket: 5}                            │
│                                                                      │
│  Step 5: RETURN _TablePartition OBJECTS                             │
│                                                                      │
│  [                                                                   │
│    _TablePartition(partition_key={year:2024, bucket:5},  table=...)  │
│    _TablePartition(partition_key={year:2024, bucket:12}, table=...)  │
│    _TablePartition(partition_key={year:2023, bucket:3},  table=...)  │
│  ]                                                                   │
└─────────────────────────────────────────────────────────────────────┘
```

### All Partition Transform Types

```
┌──────────────────────────────────────────────────────────────────────┐
│  TRANSFORM          │ INPUT           │ OUTPUT        │ EXAMPLE       │
├──────────────────────┼─────────────────┼───────────────┼───────────────┤
│ IdentityTransform    │ any value       │ same value    │ "US" → "US"   │
│ YearTransform        │ date/timestamp  │ int (yr-1970) │ 2024-03 → 54 │
│ MonthTransform       │ date/timestamp  │ int (mo-1970) │ 2024-03 → 650│
│ DayTransform         │ date/timestamp  │ int (day-1970)│ 2024-03-15→..│
│ HourTransform        │ timestamp       │ int (hr-1970) │ ts → hours    │
│ BucketTransform(N)   │ any hashable    │ int [0, N)    │ "abc" → 7     │
│ TruncateTransform(W) │ int/string      │ truncated val │ 1234 → 1200  │
│ VoidTransform        │ any             │ null          │ * → null      │
└──────────────────────┴─────────────────┴───────────────┴───────────────┘

BucketTransform details:
  1. Compute MurmurHash3 (mmh3) of value
  2. Result = (hash & INT_MAX) % num_buckets

TruncateTransform details:
  For integers:  value - (value % width)        → 1234 truncate(100) = 1200
  For strings:   value[:width]                  → "abcdef" truncate(3) = "abc"
```

### 5b. Bin Packing — bin_pack_arrow_table()

**Source**: `pyiceberg/io/pyarrow.py`

```
┌─────────────────────────────────────────────────────────────────────┐
│         bin_pack_arrow_table() — DETAILED FLOW                       │
│                                                                      │
│  Input:                                                              │
│    arrow_table:      pa.Table (one partition's data)                │
│    target_file_size: 536,870,912 bytes (512 MB)                     │
│    write_uuid:       UUID                                            │
│    counter:          AtomicCounter (for task_id)                    │
│    partition_key:    PartitionKey | None                              │
│                                                                      │
│  Algorithm:                                                          │
│                                                                      │
│  1. ESTIMATE bytes per row:                                          │
│     bytes_per_row = total_byte_size(arrow_table) / num_rows         │
│     (uses pa.Table.nbytes for estimation)                           │
│                                                                      │
│  2. CALCULATE target rows per file:                                  │
│     target_rows = target_file_size / bytes_per_row                  │
│                                                                      │
│  3. SPLIT into record batches of target_rows each:                  │
│                                                                      │
│     Input: 3,000,000 rows, target_rows = 1,000,000                  │
│                                                                      │
│     ┌─────────────────────────────────┐                             │
│     │ Arrow Table (3M rows, 1.5 GB)   │                             │
│     └─────────────────────────────────┘                             │
│              │                                                       │
│     table.to_batches(max_chunksize=1_000_000)                       │
│              │                                                       │
│     ┌──────────┐  ┌──────────┐  ┌──────────┐                       │
│     │ Batch 0   │  │ Batch 1   │  │ Batch 2   │                       │
│     │ 1M rows   │  │ 1M rows   │  │ 1M rows   │                       │
│     └──────────┘  └──────────┘  └──────────┘                       │
│                                                                      │
│  4. GROUP batches into WriteTasks:                                   │
│     Each WriteTask gets batches totaling ~target_file_size           │
│                                                                      │
│     ┌──────────────────────────────────────────────┐                │
│     │ WriteTask(                                    │                │
│     │   write_uuid  = "abc-123-def",               │                │
│     │   task_id     = 0,              ← from counter│                │
│     │   record_batches = [Batch 0],                │                │
│     │   partition_key  = {year:2024, bucket:5},    │                │
│     │ )                                             │                │
│     └──────────────────────────────────────────────┘                │
│     ┌──────────────────────────────────────────────┐                │
│     │ WriteTask(                                    │                │
│     │   write_uuid  = "abc-123-def",               │                │
│     │   task_id     = 1,                           │                │
│     │   record_batches = [Batch 1],                │                │
│     │   partition_key  = {year:2024, bucket:5},    │                │
│     │ )                                             │                │
│     └──────────────────────────────────────────────┘                │
│     ... and so on                                                   │
│                                                                      │
│  Note: write_uuid is SHARED across all tasks in one write operation │
│        task_id is UNIQUE per file (monotonic counter)               │
└─────────────────────────────────────────────────────────────────────┘
```

### 5c. Parallel Parquet Writing — write_file()

**Source**: `pyiceberg/io/pyarrow.py`

```
┌─────────────────────────────────────────────────────────────────────┐
│         write_file() — PARALLEL EXECUTION                            │
│                                                                      │
│  def write_file(io, table_metadata, tasks):                         │
│      executor = ExecutorFactory.get_or_create()                     │
│      return executor.map(                                            │
│          lambda task: write_parquet(io, table_metadata, task),       │
│          tasks                                                       │
│      )                                                               │
│                                                                      │
│  ExecutorFactory:                                                    │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │ • Singleton ThreadPoolExecutor                              │     │
│  │ • PID-aware: creates new executor after fork()             │     │
│  │ • Default max_workers: min(32, os.cpu_count() + 4)         │     │
│  │ • Reused across all parallel operations                    │     │
│  └────────────────────────────────────────────────────────────┘     │
│                                                                      │
│  Parallel execution:                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │ Thread 0  │  │ Thread 1  │  │ Thread 2  │  │ Thread 3  │          │
│  │ Task #0   │  │ Task #1   │  │ Task #2   │  │ Task #3   │          │
│  │ write_    │  │ write_    │  │ write_    │  │ write_    │          │
│  │ parquet() │  │ parquet() │  │ parquet() │  │ parquet() │          │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘          │
│        │              │              │              │                │
│        ▼              ▼              ▼              ▼                │
│   DataFile #0    DataFile #1    DataFile #2    DataFile #3          │
│                                                                      │
│  Each thread is fully independent — no shared state                 │
└─────────────────────────────────────────────────────────────────────┘
```

### 5d. write_parquet() — Single File Write (Deepest Level)

```
┌─────────────────────────────────────────────────────────────────────┐
│         write_parquet() — SINGLE FILE WRITE                          │
│                                                                      │
│  Input: WriteTask {write_uuid, task_id, record_batches, partition}  │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │ STEP A: GENERATE FILE PATH                                  │     │
│  │                                                             │     │
│  │ location_provider = load_location_provider(table_metadata)  │     │
│  │ file_name = f"00000-{task_id}-{write_uuid}.parquet"        │     │
│  │ file_path = location_provider.new_data_location(            │     │
│  │     file_name, partition_key                                │     │
│  │ )                                                           │     │
│  │                                                             │     │
│  │ Examples:                                                   │     │
│  │ Simple:  s3://bucket/db/t/data/year=2024/bucket=5/         │     │
│  │          00000-0-abc123.parquet                              │     │
│  │                                                             │     │
│  │ Object:  s3://bucket/db/t/data/a1b2/c3d4/e5f6/             │     │
│  │          00000-0-abc123.parquet                              │     │
│  └────────────────────────────────────────────────────────────┘     │
│                            │                                         │
│                            ▼                                         │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │ STEP B: BUILD PARQUET WRITER KWARGS                        │     │
│  │                                                             │     │
│  │ From table properties → ParquetWriter config:               │     │
│  │                                                             │     │
│  │ writer_kwargs = {                                           │     │
│  │   "compression":              "ZSTD",                      │     │
│  │   "compression_level":        None,  (codec default)       │     │
│  │   "write_batch_size":         1024,                        │     │
│  │   "dictionary_pagesize_limit": 2_097_152,  (2 MB)          │     │
│  │   "data_page_size":           1_048_576,   (1 MB)          │     │
│  │   "data_page_row_count_limit": 20_000,                     │     │
│  │ }                                                           │     │
│  │                                                             │     │
│  │ Compression codec mapping:                                  │     │
│  │   "zstd"         → "ZSTD"                                  │     │
│  │   "snappy"       → "SNAPPY"                                │     │
│  │   "gzip"         → "GZIP"                                  │     │
│  │   "lz4"          → "LZ4"                                   │     │
│  │   "brotli"       → "BROTLI"                                │     │
│  │   "uncompressed" → "NONE"                                  │     │
│  └────────────────────────────────────────────────────────────┘     │
│                            │                                         │
│                            ▼                                         │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │ STEP C: OPEN OUTPUT FILE                                    │     │
│  │                                                             │     │
│  │ fo = io.new_output(file_path)                               │     │
│  │ with fo.create(overwrite=True) as output_stream:            │     │
│  │                                                             │     │
│  │ FileIO dispatches based on URI scheme:                      │     │
│  │   s3://    → PyArrowFileIO → S3FileSystem                  │     │
│  │   gs://    → PyArrowFileIO → GcsFileSystem                 │     │
│  │   abfs://  → FsspecFileIO  → AzureBlobFileSystem           │     │
│  │   hdfs://  → PyArrowFileIO → HadoopFileSystem              │     │
│  │   file://  → PyArrowFileIO → LocalFileSystem               │     │
│  └────────────────────────────────────────────────────────────┘     │
│                            │                                         │
│                            ▼                                         │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │ STEP D: WRITE PARQUET DATA                                  │     │
│  │                                                             │     │
│  │ row_group_size = min(                                       │     │
│  │   properties["write.parquet.row-group-limit"],   (1048576) │     │
│  │   properties["write.parquet.row-group-size-bytes"] / bpr    │     │
│  │ )                                                           │     │
│  │                                                             │     │
│  │ writer = pq.ParquetWriter(                                  │     │
│  │   where      = output_stream,                               │     │
│  │   schema     = pyarrow_schema,                              │     │
│  │   **writer_kwargs                                           │     │
│  │ )                                                           │     │
│  │                                                             │     │
│  │ for batch in task.record_batches:                           │     │
│  │     arrow_table = pa.Table.from_batches([batch])            │     │
│  │     writer.write_table(arrow_table, row_group_size)         │     │
│  │                                                             │     │
│  │ writer.close()                                              │     │
│  │                                                             │     │
│  │ Internal Parquet structure written:                          │     │
│  │ ┌──────────────────────────────────────────────────┐       │     │
│  │ │ Parquet File                                      │       │     │
│  │ │ ┌──────────────────────────────────────────┐     │       │     │
│  │ │ │ Row Group 0 (up to 1M rows or 128 MB)    │     │       │     │
│  │ │ │ ┌─────────────┐ ┌─────────────┐          │     │       │     │
│  │ │ │ │ Column Chunk │ │ Column Chunk │ ...     │     │       │     │
│  │ │ │ │ (id)         │ │ (name)       │          │     │       │     │
│  │ │ │ │ ┌─────────┐ │ │ ┌─────────┐ │          │     │       │     │
│  │ │ │ │ │ Page 0   │ │ │ │ Page 0   │ │          │     │       │     │
│  │ │ │ │ │(≤20K row)│ │ │ │(≤20K row)│ │          │     │       │     │
│  │ │ │ │ ├─────────┤ │ │ ├─────────┤ │          │     │       │     │
│  │ │ │ │ │ Page 1   │ │ │ │ Page 1   │ │          │     │       │     │
│  │ │ │ │ └─────────┘ │ │ └─────────┘ │          │     │       │     │
│  │ │ │ └─────────────┘ └─────────────┘          │     │       │     │
│  │ │ └──────────────────────────────────────────┘     │       │     │
│  │ │ ┌──────────────────────────────────────────┐     │       │     │
│  │ │ │ Row Group 1 (next batch of rows)          │     │       │     │
│  │ │ │ ...                                       │     │       │     │
│  │ │ └──────────────────────────────────────────┘     │       │     │
│  │ │ ┌──────────────────────────────────────────┐     │       │     │
│  │ │ │ Footer (schema + row group metadata +     │     │       │     │
│  │ │ │         column statistics)                │     │       │     │
│  │ │ └──────────────────────────────────────────┘     │       │     │
│  │ └──────────────────────────────────────────────────┘       │     │
│  └────────────────────────────────────────────────────────────┘     │
│                            │                                         │
│                            ▼                                         │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │ STEP E: COLLECT STATISTICS FROM PARQUET METADATA           │     │
│  │                                                             │     │
│  │ parquet_metadata = writer.writer.metadata                   │     │
│  │                                                             │     │
│  │ For each row group in parquet_metadata:                     │     │
│  │   For each column in row group:                             │     │
│  │     ├── column_sizes[field_id]     += col.total_compressed  │     │
│  │     ├── value_counts[field_id]     += col.num_values        │     │
│  │     ├── null_value_counts[field_id]+= col.statistics.nulls  │     │
│  │     ├── nan_value_counts[field_id] += (NaN tracking)        │     │
│  │     ├── lower_bounds[field_id]     = min(all row groups)    │     │
│  │     └── upper_bounds[field_id]     = max(all row groups)    │     │
│  │                                                             │     │
│  │ Metrics mode (from write.metadata.metrics.default):         │     │
│  │   "none"         → no stats collected                       │     │
│  │   "counts"       → only null/value counts                  │     │
│  │   "truncate(N)"  → bounds truncated to N bytes (default 16)│     │
│  │   "full"         → complete untruncated bounds              │     │
│  │                                                             │     │
│  │ Bound serialization:                                        │     │
│  │   int/long   → little-endian bytes                          │     │
│  │   float/dbl  → IEEE 754 bytes                               │     │
│  │   string     → UTF-8 bytes (truncated if configured)        │     │
│  │   date       → int32 days since epoch → bytes               │     │
│  │   timestamp  → int64 microseconds since epoch → bytes       │     │
│  │   decimal    → unscaled bytes                               │     │
│  │   uuid       → 16-byte raw                                  │     │
│  │   binary     → raw bytes (truncated if configured)          │     │
│  └────────────────────────────────────────────────────────────┘     │
│                            │                                         │
│                            ▼                                         │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │ STEP F: CONSTRUCT DataFile OBJECT                          │     │
│  │                                                             │     │
│  │ DataFile(                                                   │     │
│  │   content            = DataFileContent.DATA,                │     │
│  │   file_path          = "s3://bucket/.../00000-0-abc.parq", │     │
│  │   file_format        = FileFormat.PARQUET,                  │     │
│  │   partition          = Record(year=2024, bucket=5),         │     │
│  │   record_count       = 1_048_576,                          │     │
│  │   file_size_in_bytes = 523_456_789,                        │     │
│  │   column_sizes       = {1: 102400, 2: 204800, ...},        │     │
│  │   value_counts       = {1: 1048576, 2: 1048576, ...},      │     │
│  │   null_value_counts  = {1: 0, 2: 42, ...},                │     │
│  │   nan_value_counts   = {1: 0, ...},                        │     │
│  │   lower_bounds       = {1: b'\x01\x00...', ...},           │     │
│  │   upper_bounds       = {1: b'\xff\x0f...', ...},           │     │
│  │   key_metadata       = None,                               │     │
│  │   split_offsets       = [row_group_offsets...],              │     │
│  │   equality_ids       = None,                               │     │
│  │   sort_order_id      = None,                               │     │
│  │   spec_id            = 0,                                  │     │
│  │ )                                                           │     │
│  │                                                             │     │
│  │ RETURN DataFile                                             │     │
│  └────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 6. Location Provider — Deep Dive

**Source**: `pyiceberg/table/locations.py`

### Provider Selection Logic

```
load_location_provider(table_metadata):
    │
    ├── Check "write.py-location-provider.impl" property
    │   └── If set → import and instantiate custom class
    │
    ├── Check "write.object-storage.enabled" property
    │   └── If True → ObjectStoreLocationProvider
    │
    └── Default → SimpleLocationProvider
```

### SimpleLocationProvider

```
┌─────────────────────────────────────────────────────────────────────┐
│  SimpleLocationProvider                                              │
│                                                                      │
│  data_path = properties.get("write.data.path",                      │
│              f"{table_location}/data")                               │
│                                                                      │
│  new_data_location(file_name, partition_key=None):                  │
│                                                                      │
│    If UNPARTITIONED:                                                 │
│      return f"{data_path}/{file_name}"                              │
│      → s3://warehouse/db/table/data/00000-0-abc.parquet             │
│                                                                      │
│    If PARTITIONED:                                                   │
│      partition_path = partition_key.to_path()                       │
│      return f"{data_path}/{partition_path}/{file_name}"             │
│      → s3://warehouse/db/table/data/year=2024/bucket=5/            │
│        00000-0-abc.parquet                                           │
│                                                                      │
│  partition_key.to_path() generates:                                  │
│    "field1=value1/field2=value2/..."                                │
│    e.g., "event_date_year=2024/id_bucket=5"                        │
└─────────────────────────────────────────────────────────────────────┘
```

### ObjectStoreLocationProvider

```
┌─────────────────────────────────────────────────────────────────────┐
│  ObjectStoreLocationProvider                                         │
│                                                                      │
│  Purpose: Avoid S3 hot-spotting by distributing files across        │
│  random prefixes instead of sequential partition directories.       │
│                                                                      │
│  Hash computation:                                                   │
│  1. Input: f"{file_name}{partition_path_if_enabled}"                │
│  2. Compute: mmh3.hash(input) → 32-bit integer                     │
│  3. Take top 20 bits: (hash >> 12) & 0xFFFFF                       │
│  4. Convert to hex: "a1b2c"                                         │
│  5. Split into directories: "a1b2/c"                                │
│     (HASH_PREFIX_LENGTH=4, HASH_DIR_DEPTH varies)                   │
│                                                                      │
│  new_data_location(file_name, partition_key=None):                  │
│                                                                      │
│    hash_prefix = compute_hash(file_name, partition_key)             │
│                                                                      │
│    If partitioned_paths enabled:                                     │
│      partition_path = partition_key.to_path()                       │
│      return f"{data_path}/{partition_path}/{hash_prefix}/{file}"    │
│      → s3://bucket/data/year=2024/a1b2/c3d4/00000-0-abc.parquet   │
│                                                                      │
│    Else:                                                             │
│      return f"{data_path}/{hash_prefix}/{file_name}"                │
│      → s3://bucket/data/a1b2/c3d4/e5f6/00000-0-abc.parquet        │
│                                                                      │
│  Why hash-based paths?                                               │
│  ┌────────────────────────────────────────────────────────┐         │
│  │ Problem: Sequential prefixes (date=2024-01-01/) cause   │         │
│  │ S3 request throttling on hot partitions.                │         │
│  │                                                         │         │
│  │ Solution: Random hash prefixes distribute requests      │         │
│  │ evenly across S3 partition servers.                     │         │
│  │                                                         │         │
│  │ date=2024-01-01/file1.parquet  ← all hit same S3 shard │         │
│  │ date=2024-01-01/file2.parquet  ← hot spot!             │         │
│  │                                                         │         │
│  │ a1b2/c3d4/file1.parquet        ← distributed           │         │
│  │ f7e8/d9c0/file2.parquet        ← across shards         │         │
│  └────────────────────────────────────────────────────────┘         │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 7. Manifest Creation — _commit() Deep Dive

**Source**: `pyiceberg/table/update/snapshot.py :: _SnapshotProducer._commit()`

This is where DataFiles become Manifests become a Snapshot.

### 7a. Three Manifest Streams (Written in Parallel)

```
┌─────────────────────────────────────────────────────────────────────┐
│         _manifests() — PARALLEL MANIFEST CREATION                    │
│                                                                      │
│  Uses concurrent.futures to write 3 types of manifests in parallel: │
│                                                                      │
│  ┌─────────────────┐ ┌──────────────────┐ ┌──────────────────────┐ │
│  │  Future 1:       │ │  Future 2:        │ │  Future 3:            │ │
│  │  ADDED manifests │ │  EXISTING manifs  │ │  DELETED manifests    │ │
│  │                  │ │                   │ │                       │ │
│  │  _write_added_   │ │  _fetch_existing_ │ │  _write_deleted_     │ │
│  │  manifest()      │ │  manifests()      │ │  manifest()          │ │
│  └────────┬─────────┘ └────────┬──────────┘ └────────┬──────────────┘ │
│           │                    │                      │              │
│           ▼                    ▼                      ▼              │
│    [ManifestFile]       [ManifestFile]         [ManifestFile]       │
│    (new data files)     (from parent snap)     (deleted files)      │
│           │                    │                      │              │
│           └────────────────────┼──────────────────────┘              │
│                                ▼                                     │
│                   Combined: all_manifests[]                          │
└─────────────────────────────────────────────────────────────────────┘
```

### 7b. Writing the ADDED Manifest — _write_added_manifest()

```
┌─────────────────────────────────────────────────────────────────────┐
│  _write_added_manifest()                                             │
│                                                                      │
│  Input: self._added_data_files (list of DataFile objects)           │
│                                                                      │
│  Step 1: WRAP each DataFile in a ManifestEntry                      │
│                                                                      │
│    ManifestEntry(                                                    │
│      status       = ManifestEntryStatus.ADDED,     ← key indicator  │
│      snapshot_id  = self._snapshot_id,                               │
│      data_file    = data_file,                                       │
│      sequence_number = None,  ← assigned at commit time             │
│    )                                                                 │
│                                                                      │
│  Step 2: CREATE ManifestWriter                                      │
│                                                                      │
│    manifest_path = f"{metadata_path}/{commit_uuid}-m{counter}.avro" │
│    writer = ManifestWriter(                                          │
│      spec         = partition_spec,                                  │
│      output_file  = io.new_output(manifest_path),                   │
│      snapshot_id  = self._snapshot_id,                               │
│    )                                                                 │
│                                                                      │
│  Step 3: WRITE entries to Avro manifest file                        │
│                                                                      │
│    for entry in manifest_entries:                                    │
│        writer.add_entry(entry)                                       │
│    manifest_file = writer.close()   ← returns ManifestFile          │
│                                                                      │
│  Step 4: ManifestFile object returned                               │
│                                                                      │
│    ManifestFile {                                                    │
│      manifest_path:      "s3://.../abc-m0.avro"                     │
│      manifest_length:    1_234_567  (bytes)                         │
│      partition_spec_id:  0                                           │
│      content:            ManifestContent.DATA                       │
│      added_files_count:  3                                           │
│      added_rows_count:   3_145_728                                  │
│      existing_files_count: 0                                         │
│      existing_rows_count:  0                                         │
│      deleted_files_count:  0                                         │
│      deleted_rows_count:   0                                         │
│      partitions: [                                                   │
│        PartitionFieldSummary {                                       │
│          contains_null: false,                                       │
│          contains_nan:  false,                                       │
│          lower_bound:   b'\x36\x00...',  ← min partition value      │
│          upper_bound:   b'\x36\x00...',  ← max partition value      │
│        }                                                             │
│      ]                                                               │
│      key_metadata: None                                              │
│    }                                                                 │
└─────────────────────────────────────────────────────────────────────┘
```

### 7c. Carrying Forward EXISTING Manifests — _fetch_existing_manifests()

```
┌─────────────────────────────────────────────────────────────────────┐
│  _fetch_existing_manifests()                                         │
│                                                                      │
│  For APPEND operations:                                              │
│  • ALL manifests from parent snapshot are carried forward as-is     │
│  • No rewriting needed — just reference the same manifest files     │
│                                                                      │
│  parent_snapshot = get_snapshot(parent_snapshot_id)                  │
│  existing_manifests = parent_snapshot.manifests(io)                  │
│                                                                      │
│  For _FastAppendFiles:                                               │
│    return existing_manifests  (ALL of them, unchanged)              │
│                                                                      │
│  For _MergeAppendFiles (when merge enabled):                        │
│    ┌──────────────────────────────────────────────────────┐         │
│    │ May MERGE small manifests together:                    │         │
│    │                                                        │         │
│    │ target_size = write.manifest.target-size-bytes         │         │
│    │ min_count   = write.manifest.min-count-to-merge        │         │
│    │                                                        │         │
│    │ If num_manifests > min_count:                          │         │
│    │   Group small manifests by partition spec              │         │
│    │   Rewrite groups into fewer, larger manifests          │         │
│    │   Carry forward already-large manifests as-is          │         │
│    └──────────────────────────────────────────────────────┘         │
│                                                                      │
│  For _OverwriteFiles / _DeleteFiles:                                │
│    • Manifests containing DELETED files are REWRITTEN               │
│    • Deleted entries marked with status=DELETED                     │
│    • Manifests with NO deleted files carried forward as-is          │
└─────────────────────────────────────────────────────────────────────┘
```

### 7d. Writing the DELETED Manifest — _write_deleted_manifest()

```
┌─────────────────────────────────────────────────────────────────────┐
│  _write_deleted_manifest()                                           │
│                                                                      │
│  Only created when self._deleted_data_files is non-empty            │
│  (overwrite and delete operations)                                  │
│                                                                      │
│  Same process as _write_added_manifest() but:                       │
│  • ManifestEntry.status = ManifestEntryStatus.DELETED               │
│  • Tracks which files were removed from the table                   │
│                                                                      │
│  Purpose: enables time travel (deleted files still referenced in    │
│  older snapshots) and auditing (who deleted what, when)             │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 8. Manifest List & Snapshot Creation

### 8a. Manifest List File

```
┌─────────────────────────────────────────────────────────────────────┐
│  MANIFEST LIST CREATION                                              │
│                                                                      │
│  Path: {metadata}/snap-{snapshot_id}-{attempt}-{commit_uuid}.avro   │
│                                                                      │
│  Example: s3://bucket/db/table/metadata/                            │
│           snap-8423947293847-0-a1b2c3d4.avro                        │
│                                                                      │
│  Format: Apache Avro                                                 │
│                                                                      │
│  Content: Array of ManifestFile references                           │
│  [                                                                   │
│    ManifestFile {   ← NEW manifest (added files)                    │
│      manifest_path:     "s3://.../abc-m0.avro",                     │
│      manifest_length:   1234567,                                     │
│      partition_spec_id: 0,                                           │
│      added_files_count: 3,                                           │
│      added_rows_count:  3145728,                                    │
│      partitions: [{lower: ..., upper: ...}],                        │
│    },                                                                │
│    ManifestFile {   ← EXISTING manifest (carried forward)           │
│      manifest_path:     "s3://.../def-m0.avro",                     │
│      manifest_length:   987654,                                      │
│      partition_spec_id: 0,                                           │
│      existing_files_count: 5,                                        │
│      existing_rows_count:  5242880,                                 │
│      partitions: [{lower: ..., upper: ...}],                        │
│    },                                                                │
│  ]                                                                   │
│                                                                      │
│  Written via:                                                        │
│    write_manifest_list(                                              │
│      format_version = table_metadata.format_version,                │
│      output_file    = io.new_output(manifest_list_path),            │
│      snapshot_id    = self._snapshot_id,                             │
│      parent_snapshot_id = self._parent_snapshot_id,                 │
│      sequence_number = next_sequence_number,                        │
│      manifests       = all_manifests,                               │
│    )                                                                 │
└─────────────────────────────────────────────────────────────────────┘
```

### 8b. Snapshot Object

```
┌─────────────────────────────────────────────────────────────────────┐
│  SNAPSHOT CREATION                                                   │
│                                                                      │
│  snapshot = Snapshot(                                                │
│    snapshot_id      = self._snapshot_id,       ← random int64       │
│    parent_snapshot_id = self._parent_snapshot_id,                   │
│    sequence_number  = next_sequence_number,     ← monotonic +1      │
│    timestamp_ms     = current_time_ms(),                            │
│    manifest_list    = manifest_list_path,                            │
│    summary          = Summary(                                       │
│      operation = self._operation,               ← see below         │
│      **computed_stats                                                │
│    ),                                                                │
│    schema_id        = table_metadata.current_schema_id,             │
│  )                                                                   │
│                                                                      │
│  Operation types:                                                    │
│    Operation.APPEND    ← table.append()                             │
│    Operation.OVERWRITE ← table.overwrite()                          │
│    Operation.REPLACE   ← dynamic_partition_overwrite()              │
│    Operation.DELETE    ← table.delete()                             │
└─────────────────────────────────────────────────────────────────────┘
```

### 8c. Summary Computation — _summary()

```
┌─────────────────────────────────────────────────────────────────────┐
│  SNAPSHOT SUMMARY COMPUTATION                                        │
│                                                                      │
│  Computes from manifest entries:                                     │
│                                                                      │
│  For ADDED files:                                                    │
│    added-data-files:      count of new DataFile objects              │
│    added-records:         sum of record_count across added files     │
│    added-files-size:      sum of file_size_in_bytes                 │
│                                                                      │
│  For DELETED files (overwrite/delete only):                         │
│    deleted-data-files:    count of removed DataFile objects          │
│    deleted-records:       sum of record_count across deleted files   │
│    removed-files-size:    sum of file_size_in_bytes                 │
│                                                                      │
│  Running totals (from parent + changes):                            │
│    total-data-files:      parent.total + added - deleted            │
│    total-records:         parent.total + added - deleted            │
│    total-files-size:      parent.total + added - deleted            │
│    total-delete-files:    count of active delete files              │
│    total-equality-deletes: count of equality delete files           │
│    total-position-deletes: count of position delete files           │
│                                                                      │
│  Partition summary:                                                  │
│    changed-partition-count: number of partitions modified            │
│                                                                      │
│  Example summary for an append of 3 files:                          │
│  {                                                                   │
│    "operation":          "append",                                  │
│    "added-data-files":   "3",                                       │
│    "added-records":      "3145728",                                 │
│    "added-files-size":   "1570370367",                              │
│    "total-data-files":   "15",                                      │
│    "total-records":      "15728640",                                │
│    "total-files-size":   "7851851835",                              │
│    "total-delete-files": "0",                                       │
│    "changed-partition-count": "2",                                  │
│  }                                                                   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 9. Atomic Catalog Commit — Deep Dive

**Source**: `pyiceberg/table/__init__.py :: Transaction.commit_transaction()`

### 9a. Building Updates and Requirements

```
┌─────────────────────────────────────────────────────────────────────┐
│  commit_transaction()                                                │
│                                                                      │
│  Step 1: BUILD UPDATES (what to change)                             │
│                                                                      │
│  updates = [                                                         │
│    AddSnapshotUpdate(                                                │
│      snapshot = Snapshot(...)    ← the new snapshot object           │
│    ),                                                                │
│    SetSnapshotRefUpdate(                                             │
│      ref_name    = "main",      ← branch being updated              │
│      type        = "branch",                                         │
│      snapshot_id = new_snapshot_id,                                  │
│      max_ref_age_ms    = None,                                      │
│      max_snapshot_age_ms = None,                                    │
│      min_snapshots_to_keep = None,                                  │
│    ),                                                                │
│  ]                                                                   │
│                                                                      │
│  Optional additional updates (if schema/spec changed):              │
│    AddSchemaUpdate(schema=...)                                      │
│    SetCurrentSchemaUpdate(schema_id=...)                             │
│    AddPartitionSpecUpdate(spec=...)                                 │
│    SetDefaultSpecUpdate(spec_id=...)                                │
│    AddSortOrderUpdate(sort_order=...)                               │
│    SetDefaultSortOrderUpdate(sort_order_id=...)                     │
│    SetPropertiesUpdate(updates={...})                               │
│    RemovePropertiesUpdate(removals=[...])                           │
│                                                                      │
│  Step 2: BUILD REQUIREMENTS (preconditions — optimistic locking)    │
│                                                                      │
│  requirements = [                                                    │
│    AssertTableUUID(                                                  │
│      uuid = table_metadata.table_uuid                               │
│      ← "This IS the table I think it is"                            │
│    ),                                                                │
│    AssertRefSnapshotId(                                              │
│      ref       = "main",                                             │
│      snapshot_id = parent_snapshot_id                                │
│      ← "No one else committed since I started"                     │
│    ),                                                                │
│    AssertCurrentSchemaId(                                            │
│      current_schema_id = schema_id                                  │
│      ← "Schema hasn't changed since I started"                     │
│    ),                                                                │
│  ]                                                                   │
│                                                                      │
│  Step 3: SEND TO CATALOG                                            │
│                                                                      │
│  response = catalog.commit_table(                                   │
│    CommitTableRequest(                                               │
│      identifier   = table_identifier,                               │
│      updates      = updates,                                        │
│      requirements = requirements,                                   │
│    )                                                                 │
│  )                                                                   │
└─────────────────────────────────────────────────────────────────────┘
```

### 9b. Catalog-Side Commit (varies by catalog type)

```
┌─────────────────────────────────────────────────────────────────────┐
│  CATALOG COMMIT — WHAT HAPPENS SERVER-SIDE                          │
│                                                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  1. VALIDATE REQUIREMENTS (CAS check)                          │  │
│  │                                                                │  │
│  │  For each requirement:                                         │  │
│  │    AssertTableUUID:                                            │  │
│  │      current_metadata.uuid == requirement.uuid?               │  │
│  │      NO → CommitFailedException("Table UUID mismatch")        │  │
│  │                                                                │  │
│  │    AssertRefSnapshotId:                                        │  │
│  │      current refs["main"].snapshot_id == requirement.id?      │  │
│  │      NO → CommitFailedException("Concurrent modification")    │  │
│  │      ← ANOTHER WRITER COMMITTED SINCE WE STARTED             │  │
│  │                                                                │  │
│  │    AssertCurrentSchemaId:                                      │  │
│  │      current_schema_id == requirement.schema_id?              │  │
│  │      NO → CommitFailedException("Schema changed")             │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                            │                                         │
│                     All pass? YES                                    │
│                            │                                         │
│                            ▼                                         │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  2. APPLY UPDATES (build new metadata)                         │  │
│  │                                                                │  │
│  │  new_metadata = current_metadata                               │  │
│  │                                                                │  │
│  │  For AddSnapshotUpdate:                                        │  │
│  │    new_metadata.snapshots.append(snapshot)                    │  │
│  │                                                                │  │
│  │  For SetSnapshotRefUpdate:                                     │  │
│  │    new_metadata.refs["main"] = SnapshotRef(snapshot_id)       │  │
│  │    new_metadata.current_snapshot_id = snapshot_id              │  │
│  │                                                                │  │
│  │  Increment last_sequence_number                               │  │
│  │  Update last_updated_ms                                       │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                            │                                         │
│                            ▼                                         │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  3. WRITE NEW METADATA FILE                                    │  │
│  │                                                                │  │
│  │  version = current_version + 1                                │  │
│  │  filename = f"{version:05d}-{uuid4()}.metadata.json"          │  │
│  │  path = f"{metadata_location}/{filename}"                     │  │
│  │                                                                │  │
│  │  Contents (JSON):                                              │  │
│  │  {                                                             │  │
│  │    "format-version": 2,                                       │  │
│  │    "table-uuid": "abc-123-def",                               │  │
│  │    "location": "s3://bucket/db/table",                        │  │
│  │    "last-sequence-number": 43,                                │  │
│  │    "last-updated-ms": 1705334400000,                          │  │
│  │    "last-column-id": 5,                                       │  │
│  │    "current-schema-id": 0,                                    │  │
│  │    "schemas": [...],                                          │  │
│  │    "default-spec-id": 0,                                      │  │
│  │    "partition-specs": [...],                                   │  │
│  │    "default-sort-order-id": 0,                                │  │
│  │    "sort-orders": [...],                                      │  │
│  │    "properties": {...},                                       │  │
│  │    "current-snapshot-id": 8423947293847,                      │  │
│  │    "snapshots": [                                             │  │
│  │      {old snapshots...},                                      │  │
│  │      {NEW snapshot}                                           │  │
│  │    ],                                                         │  │
│  │    "snapshot-log": [...],                                     │  │
│  │    "metadata-log": [                                          │  │
│  │      {"timestamp-ms": ..., "metadata-file": "prev.json"}     │  │
│  │    ],                                                         │  │
│  │    "refs": {                                                  │  │
│  │      "main": {                                                │  │
│  │        "snapshot-id": 8423947293847,                          │  │
│  │        "type": "branch"                                       │  │
│  │      }                                                        │  │
│  │    }                                                          │  │
│  │  }                                                             │  │
│  │                                                                │  │
│  │  Atomic write: overwrite=True on output stream                │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                            │                                         │
│                            ▼                                         │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  4. UPDATE CATALOG POINTER                                     │  │
│  │                                                                │  │
│  │  Per catalog type:                                             │  │
│  │                                                                │  │
│  │  REST:     POST /v1/tables/{id}/commit                        │  │
│  │            Server handles CAS internally                      │  │
│  │                                                                │  │
│  │  SQL:      UPDATE iceberg_tables                               │  │
│  │            SET metadata_location = '{new_path}'               │  │
│  │            WHERE table_id = '{id}'                             │  │
│  │            AND metadata_location = '{old_path}'               │  │
│  │            ← Row-level CAS via WHERE clause                   │  │
│  │                                                                │  │
│  │  Glue:     update_table() with conditional VersionId          │  │
│  │            ← AWS Glue built-in CAS                            │  │
│  │                                                                │  │
│  │  DynamoDB:  PutItem with ConditionExpression                  │  │
│  │            "metadata_location = :old_location"                │  │
│  │            ← DynamoDB conditional write                       │  │
│  │                                                                │  │
│  │  Hive:     LOCK table → update → UNLOCK                      │  │
│  │            ← Hive metastore locking (pessimistic)             │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                            │                                         │
│                            ▼                                         │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  5. RETURN CommitTableResponse                                 │  │
│  │                                                                │  │
│  │  response.metadata = new_metadata                              │  │
│  │  Table object updated with new metadata                       │  │
│  │                                                                │  │
│  │  If commit FAILED (concurrent modification):                  │  │
│  │    → CommitFailedException raised                             │  │
│  │    → Written data files become ORPHANS                        │  │
│  │    → Orphan files cleaned up by periodic maintenance          │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 10. Concurrent Modification & Retry

```
┌─────────────────────────────────────────────────────────────────────┐
│  WHAT HAPPENS WITH CONCURRENT WRITERS                                │
│                                                                      │
│  Writer A                          Writer B                          │
│  ─────────                          ─────────                          │
│  Read metadata v5                  Read metadata v5                  │
│  Write parquet files               Write parquet files               │
│  Write manifests                   Write manifests                   │
│  Commit: "update if v5"            Commit: "update if v5"           │
│       │                                  │                           │
│       ▼                                  ▼                           │
│  Catalog CAS:                      Catalog CAS:                     │
│  v5 == v5? YES                     v5 == v6? NO! (A already won)    │
│  → Commit succeeds (v6)           → CommitFailedException           │
│                                                                      │
│  Writer B's options:                                                 │
│  1. Retry: re-read metadata v6, rewrite manifests, try again       │
│  2. Fail: raise exception to user                                   │
│                                                                      │
│  Writer B's parquet files remain on storage as orphans              │
│  → Cleaned up by remove_orphan_files() maintenance operation        │
│                                                                      │
│  IMPORTANT: Data files are written BEFORE commit.                   │
│  This is safe because:                                               │
│  • Files have UUIDs in names → no conflicts                        │
│  • Files are invisible until metadata points to them                │
│  • Orphan cleanup handles failed commits                            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 11. Delete Operation — Copy-on-Write Deep Dive

```
┌─────────────────────────────────────────────────────────────────────┐
│  DELETE OPERATION — TWO-LEVEL FILE EVALUATION                        │
│                                                                      │
│  Filter: price > 500                                                │
│                                                                      │
│  For each existing DataFile:                                        │
│                                                                      │
│  LEVEL 1: STRICT Metrics Evaluator                                  │
│  "Do ALL rows in this file match the delete filter?"                │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │ DataFile X: lower_bounds[price]=600, upper_bounds[price]=900│    │
│  │ Strict: min(600) > 500? YES → ALL rows match               │    │
│  │ → DELETE entire file (no rewrite needed)                    │    │
│  └────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  LEVEL 2: INCLUSIVE Metrics Evaluator                                │
│  "Could ANY row in this file match the delete filter?"              │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │ DataFile Y: lower_bounds[price]=100, upper_bounds[price]=300│    │
│  │ Inclusive: max(300) > 500? NO → NO rows can match           │    │
│  │ → KEEP file as-is (skip entirely)                           │    │
│  └────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  LEVEL 3: COPY-ON-WRITE (partial match)                             │
│  "Some rows match, some don't — rewrite the file"                  │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │ DataFile Z: lower_bounds[price]=200, upper_bounds[price]=800│    │
│  │ Strict: min(200) > 500? NO                                  │    │
│  │ Inclusive: max(800) > 500? YES                              │    │
│  │ → Must READ file and REWRITE surviving rows                 │    │
│  │                                                              │    │
│  │ 1. Read DataFile Z into Arrow table                         │    │
│  │ 2. Apply NOT(price > 500) → keep rows where price ≤ 500    │    │
│  │ 3. Write surviving rows as NEW DataFile Z'                  │    │
│  │ 4. delete_snap.delete_data_file(Z)    ← mark old deleted   │    │
│  │ 5. delete_snap.append_data_file(Z')   ← add new file       │    │
│  └────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  Summary for this example:                                          │
│  • File X: deleted entirely (all matched)                           │
│  • File Y: kept as-is (none matched)                                │
│  • File Z: rewritten without matching rows (copy-on-write)          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 12. Final Storage Layout (Complete)

```
s3://warehouse/my_database/my_table/
│
├── metadata/
│   │
│   │  ── Metadata JSON files (one per commit) ──
│   ├── 00001-aaaa-1111.metadata.json          ← v1 (initial create)
│   ├── 00002-bbbb-2222.metadata.json          ← v2 (first append)
│   ├── 00003-cccc-3333.metadata.json          ← v3 (second append) CURRENT
│   │
│   │  ── Manifest List files (one per snapshot) ──
│   ├── snap-1000000001-0-dddd.avro            ← snapshot 1 manifest list
│   ├── snap-1000000002-0-eeee.avro            ← snapshot 2 manifest list
│   ├── snap-1000000003-0-ffff.avro            ← snapshot 3 manifest list
│   │
│   │  ── Manifest files (one or more per snapshot) ──
│   ├── aaaa-m0.avro       ← manifest: files added in snapshot 1
│   ├── bbbb-m0.avro       ← manifest: files added in snapshot 2
│   └── cccc-m0.avro       ← manifest: files added in snapshot 3
│
└── data/
    │
    │  ── Partitioned data files ──
    ├── event_date_year=2023/id_bucket=3/
    │   └── 00000-0-aaaa-1111.parquet          ← written in snapshot 1
    ├── event_date_year=2024/id_bucket=5/
    │   ├── 00000-0-bbbb-2222.parquet          ← written in snapshot 2
    │   └── 00000-0-cccc-3333.parquet          ← written in snapshot 3
    └── event_date_year=2024/id_bucket=12/
        └── 00000-1-bbbb-2222.parquet          ← written in snapshot 2
```

### How Metadata Points to Data (Top-Down)

```
00003-cccc.metadata.json
  │
  ├── current_snapshot_id: 1000000003
  │
  ├── snapshots: [
  │     snap_1 → snap-1000000001-0-dddd.avro (manifest list)
  │     snap_2 → snap-1000000002-0-eeee.avro (manifest list)
  │     snap_3 → snap-1000000003-0-ffff.avro (manifest list)  ← CURRENT
  │   ]
  │
  └── refs: {"main" → snapshot_id: 1000000003}

snap-1000000003-0-ffff.avro (manifest list for current snapshot)
  │
  ├── ManifestFile → cccc-m0.avro  (ADDED in this snapshot)
  │   └── ManifestEntry(ADDED):   DataFile → 00000-0-cccc-3333.parquet
  │
  ├── ManifestFile → bbbb-m0.avro  (EXISTING, carried from snapshot 2)
  │   ├── ManifestEntry(EXISTING): DataFile → 00000-0-bbbb-2222.parquet
  │   └── ManifestEntry(EXISTING): DataFile → 00000-1-bbbb-2222.parquet
  │
  └── ManifestFile → aaaa-m0.avro  (EXISTING, carried from snapshot 1)
      └── ManifestEntry(EXISTING): DataFile → 00000-0-aaaa-1111.parquet
```

---

## 13. Write Configuration — Complete Reference

### Data File Properties

| Property | Default | Description |
|----------|---------|-------------|
| `write.target-file-size-bytes` | 536,870,912 (512 MB) | Target size before splitting into new file |
| `write.format.default` | `parquet` | Output file format (parquet, avro, orc) |
| `write.data.path` | `{table_location}/data` | Base directory for data files |
| `write.metadata.path` | `{table_location}/metadata` | Base directory for metadata |

### Parquet Writer Properties

| Property | Default | Description |
|----------|---------|-------------|
| `write.parquet.compression-codec` | `zstd` | zstd, snappy, gzip, lz4, brotli, uncompressed |
| `write.parquet.compression-level` | codec default | Compression level (codec-specific) |
| `write.parquet.row-group-size-bytes` | 134,217,728 (128 MB) | Max bytes per row group |
| `write.parquet.row-group-limit` | 1,048,576 | Max rows per row group |
| `write.parquet.page-size-bytes` | 1,048,576 (1 MB) | Target page size in bytes |
| `write.parquet.page-row-limit` | 20,000 | Max rows per page |
| `write.parquet.dict-size-bytes` | 2,097,152 (2 MB) | Max dictionary page size |
| `write.parquet.bloom-filter-enabled.column.*` | false | Enable bloom filter for column |
| `write.parquet.bloom-filter-max-bytes` | 1,048,576 | Max bloom filter size |

### Statistics Properties

| Property | Default | Description |
|----------|---------|-------------|
| `write.metadata.metrics.default` | `truncate(16)` | Default metrics mode for all columns |
| `write.metadata.metrics.column.*` | (inherits default) | Per-column override |

Metrics modes:
- `none` — no statistics collected
- `counts` — only null/value counts, no bounds
- `truncate(N)` — bounds truncated to N bytes
- `full` — complete untruncated bounds

### Location Provider Properties

| Property | Default | Description |
|----------|---------|-------------|
| `write.object-storage.enabled` | `false` | Use hash-based ObjectStoreLocationProvider |
| `write.object-storage.partitioned-paths` | `false` | Include partition path in hash layout |
| `write.py-location-provider.impl` | None | Custom location provider class path |

### Manifest Properties

| Property | Default | Description |
|----------|---------|-------------|
| `write.manifest.merge.enabled` | `false` | Merge small manifests on commit |
| `write.manifest.target-size-bytes` | 8,388,608 (8 MB) | Target manifest file size |
| `write.manifest.min-count-to-merge` | 100 | Min manifest count to trigger merge |

### Metadata Retention Properties

| Property | Default | Description |
|----------|---------|-------------|
| `write.metadata.previous-versions-max` | 100 | Max previous metadata files to keep |
| `write.metadata.delete-after-commit.enabled` | `false` | Delete old metadata after commit |

### Delete Mode Properties

| Property | Default | Description |
|----------|---------|-------------|
| `write.delete.mode` | `copy-on-write` | Delete strategy |
| `write.update.mode` | `copy-on-write` | Update strategy |
| `write.merge.mode` | `copy-on-write` | Merge strategy |

Modes:
- `copy-on-write` — rewrite files excluding deleted rows (default, simpler)
- `merge-on-read` — write separate delete files, apply at read time (faster writes, slower reads)

### Timestamp Properties

| Property | Default | Description |
|----------|---------|-------------|
| `write.parquet.timestamp.downcast-ns-timestamp-to-us-on-write` | `false` | Auto-downcast nanosecond timestamps to microsecond |

---

## 14. Catalog Options — Commit Mechanisms

| Catalog | Backend | Commit Mechanism | Consistency |
|---------|---------|------------------|-------------|
| REST | HTTP API | Server-side CAS | Strong (server enforced) |
| SQL | PostgreSQL / SQLite | Row-level WHERE clause | Strong (DB transaction) |
| Glue | AWS Glue Data Catalog | Conditional VersionId update | Strong (AWS enforced) |
| DynamoDB | AWS DynamoDB | ConditionExpression on PutItem | Strong (DynamoDB CAS) |
| Hive | Hive Metastore (Thrift) | LOCK → update → UNLOCK | Strong (pessimistic lock) |
| BigQuery | Google BigQuery | BigQuery transactions | Strong (BQ enforced) |
| In-Memory | Python dict | Direct assignment | None (single-process only) |

---

## 15. End-to-End Timing Example

```
table.append(1M rows, 500MB)

 t=0ms     Transaction created
 t=1ms     Schema validation passes
 t=2ms     _FastAppendFiles context entered
 t=3ms     _determine_partitions() → 3 partitions found
 t=5ms     bin_pack_arrow_table() → 3 WriteTasks created (1 per partition)

 t=6ms     write_file() → 3 threads launched
 t=6ms     ├── Thread 0: write_parquet(task_0) starts
 t=6ms     ├── Thread 1: write_parquet(task_1) starts
 t=6ms     └── Thread 2: write_parquet(task_2) starts
 t=800ms   ├── Thread 0: parquet written, stats collected → DataFile 0
 t=900ms   ├── Thread 1: parquet written, stats collected → DataFile 1
 t=850ms   └── Thread 2: parquet written, stats collected → DataFile 2

 t=910ms   _commit() triggered (context exit)
 t=911ms   3 concurrent futures launched:
           ├── Future 1: write ADDED manifest (3 entries) → manifest.avro
           ├── Future 2: fetch EXISTING manifests from parent → [m1, m2]
           └── Future 3: write DELETED manifest (empty) → skip
 t=950ms   All futures complete → 3 manifests total

 t=951ms   Write manifest list → snap-xxx.avro
 t=960ms   Create Snapshot object with summary
 t=961ms   Build updates[] and requirements[]
 t=962ms   catalog.commit_table(updates, requirements)
 t=980ms   Catalog validates requirements (CAS check passes)
 t=985ms   Catalog writes 00003-xxx.metadata.json
 t=990ms   Catalog returns CommitTableResponse
 t=991ms   Table object updated with new metadata

 Total: ~1 second for 1M rows / 500MB / 3 partitions
```
