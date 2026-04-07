# Apache Iceberg Read Path — Detailed Architecture

This document traces the complete read path in PyIceberg, from `table.scan()` through every optimization layer down to returning Arrow/Pandas data.

---

## High-Level Flow

```
table.scan(row_filter, selected_fields, snapshot_id, limit)
    → Snapshot Resolution (time travel)
    → Schema Projection (column selection)
    → Manifest Pruning (partition summary stats)
    → Data File Pruning (partition values + column statistics)
    → Residual Filter Computation
    → Parallel Parquet Reading (with predicate pushdown)
    → Position Delete Application
    → Schema Evolution Projection
    → Result Assembly (Arrow / Pandas / DuckDB / Polars / Ray)
```

---

## Complete Read Path Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                          USER CODE                                    │
│                                                                       │
│   df = table.scan(                                                    │
│       row_filter="price > 100 AND region = 'US'",                    │
│       selected_fields=("id", "price", "region", "timestamp"),        │
│       snapshot_id=None,          # current snapshot (or time travel)  │
│       limit=10000,               # optional row limit                 │
│   ).to_arrow()                   # or to_pandas(), to_duckdb(), etc. │
│                                                                       │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│           1. DATASCAN CONSTRUCTION                                    │
│  pyiceberg/table/__init__.py :: Table.scan()                          │
│                                                                       │
│  Creates a DataScan object with:                                      │
│  ┌────────────────────────────────────────────────────────┐          │
│  │  table_metadata    → Full table metadata (all versions) │          │
│  │  io                → FileIO (S3/GCS/ADLS/local/HDFS)   │          │
│  │  row_filter        → BooleanExpression (unbound)        │          │
│  │  selected_fields   → ("id","price","region","timestamp")│          │
│  │  case_sensitive    → true (default)                     │          │
│  │  snapshot_id       → None (current) or specific ID      │          │
│  │  limit             → 10000 (optional)                   │          │
│  └────────────────────────────────────────────────────────┘          │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│           2. SNAPSHOT RESOLUTION                                      │
│  pyiceberg/table/__init__.py :: TableScan.snapshot()                  │
│                                                                       │
│  ┌─────────────────────────┐    ┌──────────────────────────────┐    │
│  │  snapshot_id = None      │    │  snapshot_id = 8423947293846 │    │
│  │                          │    │                               │    │
│  │  Uses current snapshot   │    │  Looks up specific snapshot   │    │
│  │  from refs["main"]       │    │  from metadata.snapshots[]    │    │
│  │  (latest committed       │    │  (TIME TRAVEL)                │    │
│  │   state)                 │    │                               │    │
│  └─────────────┬────────────┘    └──────────────┬────────────────┘    │
│                └──────────────┬──────────────────┘                    │
│                               ▼                                       │
│  Snapshot {                                                           │
│    snapshot_id:     8423947293847                                      │
│    manifest_list:   "s3://.../snap-842...-0-abc.avro"                │
│    schema_id:       0                                                 │
│    summary:         {total-records: 15728640, ...}                   │
│  }                                                                    │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│           3. SCHEMA PROJECTION                                        │
│  pyiceberg/table/__init__.py :: TableScan.projection()                │
│                                                                       │
│  Full Table Schema               Projected Schema                     │
│  ┌──────────────────┐           ┌──────────────────┐                │
│  │ id        int64   │           │ id        int64   │                │
│  │ name      string  │  ──→     │ price     float64 │                │
│  │ price     float64 │  prune   │ region    string  │                │
│  │ region    string  │  cols    │ timestamp ts_us   │                │
│  │ timestamp ts_us   │           └──────────────────┘                │
│  │ metadata  struct  │           Only columns in                      │
│  └──────────────────┘           selected_fields                      │
│                                                                       │
│  This projected schema determines which columns are read from Parquet │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│           4. SCAN PLANNING — plan_files()                             │
│  pyiceberg/table/__init__.py :: DataScan.plan_files()                 │
│                                                                       │
│  ┌──────────────────────────┐  ┌────────────────────────────────┐   │
│  │  Server-Side Planning     │  │  Local Planning (default)       │   │
│  │  (REST catalog only)      │  │                                 │   │
│  │                           │  │  scan_plan_helper()              │   │
│  │  Catalog does the         │  │  + _plan_files_local()          │   │
│  │  pruning server-side      │  │                                 │   │
│  └──────────────────────────┘  └──────────────┬──────────────────┘   │
│                                                │                      │
└────────────────────────────────────────────────┼─────────────────────┘
                                                 │
                    ┌────────────────────────────┘
                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│           5. MANIFEST PRUNING (Level 1 — coarsest)                    │
│  pyiceberg/table/__init__.py :: scan_plan_helper()                    │
│  pyiceberg/expressions/visitors.py :: _ManifestEvalVisitor            │
│                                                                       │
│  Read manifest list from snapshot:                                    │
│  snap-842...-0-abc.avro → [Manifest1, Manifest2, Manifest3, ...]     │
│                                                                       │
│  Each ManifestFile has partition_field_summary:                        │
│  ┌────────────────────────────────────────────────────┐              │
│  │ ManifestFile {                                      │              │
│  │   manifest_path: "s3://.../aaaa-m0.avro"           │              │
│  │   added_rows_count: 524288                          │              │
│  │   partition_field_summary: [                        │              │
│  │     {contains_null: false,                          │              │
│  │      lower_bound: "EU",      ← partition min       │              │
│  │      upper_bound: "EU"}      ← partition max       │              │
│  │   ]                                                 │              │
│  │ }                                                   │              │
│  └────────────────────────────────────────────────────┘              │
│                                                                       │
│  Filter: region = 'US'                                                │
│                                                                       │
│  Manifest1: region in [EU, EU]     → SKIP (cannot contain 'US')      │
│  Manifest2: region in [US, US]     → KEEP (might contain 'US')  ✓    │
│  Manifest3: region in [AP, US]     → KEEP (might contain 'US')  ✓    │
│                                                                       │
│  Result: 3 manifests → 2 manifests (33% pruned)                      │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│           6. MANIFEST READING (Parallel)                              │
│  pyiceberg/table/__init__.py :: _open_manifest()                      │
│                                                                       │
│  ExecutorFactory.map() reads surviving manifests in parallel:         │
│                                                                       │
│  ┌──────────────┐    ┌──────────────┐                                │
│  │ Thread 1      │    │ Thread 2      │                                │
│  │ Read Manifest2│    │ Read Manifest3│                                │
│  │ (.avro file)  │    │ (.avro file)  │                                │
│  └──────┬────────┘    └──────┬────────┘                                │
│         │                    │                                         │
│         ▼                    ▼                                         │
│  [ManifestEntry]      [ManifestEntry]                                 │
│   • DataFile A         • DataFile D                                   │
│   • DataFile B         • DataFile E                                   │
│   • DataFile C         • DataFile F                                   │
│                                                                       │
│  Each ManifestEntry contains:                                         │
│  { status: ADDED|EXISTING|DELETED, snapshot_id, data_file: DataFile } │
│                                                                       │
│  Only entries with status != DELETED are kept                         │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│           7. PARTITION PRUNING (Level 2)                               │
│  pyiceberg/table/__init__.py :: _build_partition_evaluator()          │
│  pyiceberg/expressions/visitors.py :: expression_evaluator()          │
│                                                                       │
│  For each DataFile, evaluate partition values against projected filter │
│                                                                       │
│  Filter projected to partitions: region = 'US'                        │
│                                                                       │
│  DataFile A: partition={region: 'US'}  → KEEP  ✓                     │
│  DataFile B: partition={region: 'EU'}  → SKIP  ✗                     │
│  DataFile C: partition={region: 'US'}  → KEEP  ✓                     │
│  DataFile D: partition={region: 'AP'}  → SKIP  ✗                     │
│  DataFile E: partition={region: 'US'}  → KEEP  ✓                     │
│  DataFile F: partition={region: 'US'}  → KEEP  ✓                     │
│                                                                       │
│  Result: 6 files → 4 files (33% pruned)                              │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│           8. METRICS / STATISTICS PRUNING (Level 3 — finest)          │
│  pyiceberg/expressions/visitors.py :: _InclusiveMetricsEvaluator      │
│                                                                       │
│  Uses per-file column statistics to skip files that cannot match:     │
│                                                                       │
│  Filter: price > 100                                                  │
│                                                                       │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │ DataFile A                                                    │    │
│  │   upper_bounds[price] = 250.0    → max=250 > 100 → KEEP  ✓  │    │
│  │   lower_bounds[price] = 50.0                                  │    │
│  │   null_value_counts[price] = 0                                │    │
│  ├──────────────────────────────────────────────────────────────┤    │
│  │ DataFile C                                                    │    │
│  │   upper_bounds[price] = 80.0     → max=80 ≤ 100 → SKIP  ✗   │    │
│  │   lower_bounds[price] = 10.0     (ALL values ≤ 100)          │    │
│  │   null_value_counts[price] = 0                                │    │
│  ├──────────────────────────────────────────────────────────────┤    │
│  │ DataFile E                                                    │    │
│  │   upper_bounds[price] = 500.0    → max=500 > 100 → KEEP  ✓  │    │
│  │   lower_bounds[price] = 99.0                                  │    │
│  │   null_value_counts[price] = 5                                │    │
│  ├──────────────────────────────────────────────────────────────┤    │
│  │ DataFile F                                                    │    │
│  │   upper_bounds[price] = 150.0    → max=150 > 100 → KEEP  ✓  │    │
│  │   lower_bounds[price] = 120.0                                 │    │
│  │   null_value_counts[price] = 0                                │    │
│  └──────────────────────────────────────────────────────────────┘    │
│                                                                       │
│  Statistics checked per predicate type:                                │
│  ┌─────────────────┬───────────────────────────────────────────┐     │
│  │ Predicate        │ Skip condition                            │     │
│  ├─────────────────┼───────────────────────────────────────────┤     │
│  │ col = V          │ V < lower_bound OR V > upper_bound        │     │
│  │ col != V         │ lower_bound = upper_bound = V (all same)  │     │
│  │ col < V          │ lower_bound >= V                          │     │
│  │ col <= V         │ lower_bound > V                           │     │
│  │ col > V          │ upper_bound <= V                          │     │
│  │ col >= V         │ upper_bound < V                           │     │
│  │ col IS NULL      │ null_count = 0                            │     │
│  │ col IS NOT NULL  │ all values are null                       │     │
│  │ col IN (V1,V2..) │ no Vi in [lower, upper] range            │     │
│  │ col NOT IN (..)  │ all values equal single member of set     │     │
│  └─────────────────┴───────────────────────────────────────────┘     │
│                                                                       │
│  Also checks:                                                         │
│  • contains_nulls_only → skip for non-null predicates                │
│  • contains_nans_only  → skip for numeric comparisons                │
│  • record_count = 0    → skip (unless include_empty_files=true)      │
│                                                                       │
│  Result: 4 files → 3 files (25% pruned)                              │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│           9. DELETE FILE ASSOCIATION                                   │
│  pyiceberg/table/delete_file_index.py :: DeleteFileIndex              │
│                                                                       │
│  Separates manifest entries into:                                     │
│  ┌──────────────────────────┐  ┌──────────────────────────────┐     │
│  │  DATA files               │  │  DELETE files                  │     │
│  │  (content = DATA)         │  │  (content = POSITION_DELETES  │     │
│  │  → FileScanTask targets   │  │   or EQUALITY_DELETES)        │     │
│  └──────────────────────────┘  └──────────────────────────────┘     │
│                                                                       │
│  DeleteFileIndex indexes deletes by:                                  │
│  1. Exact file path match (delete targets specific data file)         │
│  2. Partition key match (delete applies to all files in partition)     │
│                                                                       │
│  For each DataFile, finds applicable delete files:                    │
│  ┌───────────────────────────────────────────────────────┐           │
│  │ DataFile A → {PosDelete_1}        (1 delete file)      │           │
│  │ DataFile E → {}                   (no deletes)         │           │
│  │ DataFile F → {PosDelete_2, PosDelete_3}  (2 deletes)  │           │
│  └───────────────────────────────────────────────────────┘           │
│                                                                       │
│  Sequence number filtering:                                           │
│  • Delete files only apply to data files with LOWER sequence number  │
│  • Prevents deletes from applying to files written after them         │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│          10. RESIDUAL FILTER COMPUTATION                              │
│  pyiceberg/table/__init__.py :: _build_residual_evaluator()           │
│  pyiceberg/expressions/visitors.py :: ResidualEvaluator               │
│                                                                       │
│  Original filter: price > 100 AND region = 'US'                      │
│                                                                       │
│  For partitioned files (region is partition col):                      │
│  • region = 'US' was already evaluated during partition pruning       │
│  • Residual filter: price > 100                                       │
│    (only the non-partition predicates remain)                         │
│                                                                       │
│  For unpartitioned files:                                             │
│  • Residual filter: price > 100 AND region = 'US'                    │
│    (entire filter remains)                                            │
│                                                                       │
│  The residual is stored in each FileScanTask for row-level filtering  │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│          11. FILE SCAN TASK ASSEMBLY                                   │
│  pyiceberg/table/__init__.py :: FileScanTask                          │
│                                                                       │
│  Each surviving file becomes a FileScanTask:                          │
│                                                                       │
│  ┌───────────────────────────────────────────────────────────────┐   │
│  │ FileScanTask {                                                 │   │
│  │   file:          DataFile A                                    │   │
│  │   delete_files:  {PosDelete_1}                                │   │
│  │   residual:      price > 100                                   │   │
│  │ }                                                              │   │
│  ├───────────────────────────────────────────────────────────────┤   │
│  │ FileScanTask {                                                 │   │
│  │   file:          DataFile E                                    │   │
│  │   delete_files:  {}                                            │   │
│  │   residual:      price > 100                                   │   │
│  │ }                                                              │   │
│  ├───────────────────────────────────────────────────────────────┤   │
│  │ FileScanTask {                                                 │   │
│  │   file:          DataFile F                                    │   │
│  │   delete_files:  {PosDelete_2, PosDelete_3}                   │   │
│  │   residual:      price > 100                                   │   │
│  │ }                                                              │   │
│  └───────────────────────────────────────────────────────────────┘   │
│                                                                       │
│  3 FileScanTasks ready for parallel execution                         │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│          12. PARALLEL DELETE FILE PRE-LOADING                         │
│  pyiceberg/io/pyarrow.py :: _read_all_delete_files()                  │
│                                                                       │
│  Before reading data files, ALL applicable delete files are           │
│  read in parallel and indexed:                                        │
│                                                                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │  Thread 1     │  │  Thread 2     │  │  Thread 3     │              │
│  │  PosDelete_1  │  │  PosDelete_2  │  │  PosDelete_3  │              │
│  └──────┬────────┘  └──────┬────────┘  └──────┬────────┘              │
│         │                  │                  │                        │
│         ▼                  ▼                  ▼                        │
│  Each delete file contains:                                           │
│  ┌────────────────────────────────────┐                              │
│  │  file_path              │  pos     │                              │
│  ├─────────────────────────┼─────────┤                              │
│  │  s3://.../file_A.parquet│  42      │                              │
│  │  s3://.../file_A.parquet│  1337    │                              │
│  │  s3://.../file_F.parquet│  7       │                              │
│  └─────────────────────────┴─────────┘                              │
│                                                                       │
│  Indexed as: { "file_path" → [position_array] }                      │
│  Result: deletes_per_file dict for fast lookup during reading         │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│          13. PARALLEL PARQUET FILE READING                            │
│  pyiceberg/io/pyarrow.py :: ArrowScan.to_record_batches()             │
│  ExecutorFactory.map(batches_for_task, tasks)                         │
│                                                                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │  Thread 1     │  │  Thread 2     │  │  Thread 3     │              │
│  │  DataFile A   │  │  DataFile E   │  │  DataFile F   │              │
│  │  (with deletes│  │  (no deletes) │  │  (with deletes│              │
│  └──────┬────────┘  └──────┬────────┘  └──────┬────────┘              │
│         │                  │                  │                        │
│         └─────────────┬────┴──────────────────┘                       │
│                       ▼                                               │
│  Each thread executes _task_to_record_batches() (see next step)       │
│                                                                       │
│  Row limit tracking across threads:                                   │
│  • total_row_count accumulates across all yielded batches             │
│  • When limit reached, final batch is sliced to exact count           │
│  • Remaining tasks are abandoned                                      │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│          14. SINGLE FILE READING — _task_to_record_batches()          │
│  pyiceberg/io/pyarrow.py (core reading function)                      │
│                                                                       │
│  For each FileScanTask, 10 sub-steps execute:                         │
│                                                                       │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ STEP A: Open Parquet File                                      │  │
│  │                                                                │  │
│  │  arrow_format = ParquetFileFormat(                              │  │
│  │      pre_buffer=True,           ← prefetch metadata            │  │
│  │      buffer_size=8MB            ← read-ahead buffer            │  │
│  │  )                                                             │  │
│  │  fragment = arrow_format.make_fragment(file_handle)             │  │
│  │  physical_schema = fragment.physical_schema                     │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                       │                                               │
│                       ▼                                               │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ STEP B: Schema Conversion                                      │  │
│  │                                                                │  │
│  │  Convert Parquet physical schema → Iceberg schema              │  │
│  │  • Uses name_mapping if available (for files without field IDs)│  │
│  │  • Handles timestamp downcast (ns → µs) if configured          │  │
│  │  • Maps Parquet types to Iceberg types                         │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                       │                                               │
│                       ▼                                               │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ STEP C: Column Projection for Schema Evolution                 │  │
│  │                                                                │  │
│  │  Handles columns that exist in projected schema but NOT in file:│  │
│  │                                                                │  │
│  │  Projected Schema    File Schema (older)                       │  │
│  │  ┌─────────────┐    ┌─────────────┐                           │  │
│  │  │ id     int64 │    │ id     int64 │  ← exists               │  │
│  │  │ price  float │    │ price  float │  ← exists               │  │
│  │  │ region str   │    │ region str   │  ← exists               │  │
│  │  │ status str   │    │              │  ← MISSING (added later)│  │
│  │  └─────────────┘    └─────────────┘                           │  │
│  │                                                                │  │
│  │  For missing columns:                                          │  │
│  │  1. If identity-partitioned → use partition value              │  │
│  │  2. Otherwise → fill with nulls or schema default              │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                       │                                               │
│                       ▼                                               │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ STEP D: Filter Translation                                     │  │
│  │                                                                │  │
│  │  Iceberg BooleanExpression                                     │  │
│  │       │                                                        │  │
│  │       ├─ translate_column_names()                              │  │
│  │       │  (map table schema names → file schema names)          │  │
│  │       │  (handle renamed columns via field IDs)                │  │
│  │       │  (replace missing column refs with known values)       │  │
│  │       │                                                        │  │
│  │       ├─ bind() to file schema                                 │  │
│  │       │  (resolve field references to concrete column IDs)     │  │
│  │       │                                                        │  │
│  │       └─ expression_to_pyarrow()                               │  │
│  │          (convert to PyArrow compute expression)               │  │
│  │                                                                │  │
│  │  Result: pyarrow_filter for Parquet-level pushdown             │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                       │                                               │
│                       ▼                                               │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ STEP E: Column Pruning                                         │  │
│  │                                                                │  │
│  │  prune_columns(file_schema, projected_field_ids)               │  │
│  │                                                                │  │
│  │  Only columns needed by:                                       │  │
│  │  • selected_fields (user projection)                           │  │
│  │  • row_filter (predicate evaluation)                           │  │
│  │  are included in the read request                              │  │
│  │                                                                │  │
│  │  File has 20 columns → only 4 columns read from Parquet       │  │
│  │  (massive I/O savings on wide tables)                          │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                       │                                               │
│                       ▼                                               │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ STEP F: Create PyArrow Scanner                                 │  │
│  │                                                                │  │
│  │  ds.Scanner.from_fragment(                                     │  │
│  │      fragment   = parquet_fragment,                             │  │
│  │      schema     = physical_schema,                              │  │
│  │      filter     = pyarrow_filter,  ← PUSHDOWN (if no deletes) │  │
│  │      columns    = ["id","price","region","timestamp"],         │  │
│  │  )                                                             │  │
│  │                                                                │  │
│  │  IMPORTANT: If positional deletes exist for this file:         │  │
│  │  • filter is set to None (NOT pushed down)                     │  │
│  │  • Filter applied AFTER delete application (Step H)            │  │
│  │  • Reason: row positions would shift after delete removal      │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                       │                                               │
│                       ▼                                               │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ STEP G: Read Record Batches from Parquet                       │  │
│  │                                                                │  │
│  │  for batch in fragment_scanner.to_batches():                   │  │
│  │                                                                │  │
│  │  PyArrow/Parquet optimizations happening under the hood:       │  │
│  │  ┌──────────────────────────────────────────────────────┐     │  │
│  │  │ • Row group filtering: skip row groups where          │     │  │
│  │  │   Parquet statistics rule out matches                 │     │  │
│  │  │ • Column chunk reading: only read needed columns      │     │  │
│  │  │ • Page-level filtering (Parquet 2.0+): skip pages     │     │  │
│  │  │   using page index                                    │     │  │
│  │  │ • Pre-buffering: async prefetch for cloud storage     │     │  │
│  │  │ • Dictionary filtering: skip non-matching dict pages  │     │  │
│  │  └──────────────────────────────────────────────────────┘     │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                       │                                               │
│                       ▼                                               │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ STEP H: Apply Positional Deletes (if present)                  │  │
│  │                                                                │  │
│  │  Batch rows:     [row0, row1, row2, row3, row4, row5]         │  │
│  │  Delete positions: [1, 4]                                      │  │
│  │                                                                │  │
│  │  _combine_positional_deletes():                                │  │
│  │  1. Create full index range:  [0, 1, 2, 3, 4, 5]              │  │
│  │  2. Remove deleted positions: [0, 2, 3, 5]                    │  │
│  │  3. batch.take([0, 2, 3, 5])                                  │  │
│  │                                                                │  │
│  │  Result: [row0, row2, row3, row5]  (row1 & row4 deleted)     │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                       │                                               │
│                       ▼                                               │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ STEP I: Apply Row Filter (if not pushed down)                  │  │
│  │                                                                │  │
│  │  If positional deletes were present, filter was NOT pushed     │  │
│  │  down to Parquet. Apply it now on the batch:                   │  │
│  │                                                                │  │
│  │  table = pa.Table.from_batches([batch])                        │  │
│  │  table = table.filter(pyarrow_filter)   ← row-level filter    │  │
│  │  batch = table.to_batches()[0]                                 │  │
│  │                                                                │  │
│  │  If NO deletes: filter was already pushed down (Step F)        │  │
│  │  → this step is skipped                                        │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                       │                                               │
│                       ▼                                               │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ STEP J: Schema Evolution Projection                            │  │
│  │  _to_requested_schema()                                        │  │
│  │                                                                │  │
│  │  ArrowProjectionVisitor handles:                               │  │
│  │                                                                │  │
│  │  1. Type casting (e.g., int32 → int64 for widened columns)    │  │
│  │  2. Column reordering (match requested schema order)           │  │
│  │  3. Missing columns filled with:                               │  │
│  │     • Partition values (if identity transform)                 │  │
│  │     • Null arrays (for columns added after file was written)   │  │
│  │     • Default values (if schema specifies initial-default)     │  │
│  │  4. Timestamp tz/precision normalization                       │  │
│  │  5. Nested struct/list/map field alignment                     │  │
│  │                                                                │  │
│  │  File batch schema → Requested schema                          │  │
│  │  (id:int32, price:float32)  →  (id:int64, price:float64,     │  │
│  │                                  region:str, status:null)      │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                       │                                               │
│                       ▼                                               │
│               Yield pa.RecordBatch                                    │
└───────────────────────────────┬──────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│          15. RESULT ASSEMBLY                                          │
│  pyiceberg/io/pyarrow.py :: ArrowScan.to_table()                      │
│                                                                       │
│  Collect all record batches from all parallel readers:                 │
│                                                                       │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐           │
│  │Batch 1 │ │Batch 2 │ │Batch 3 │ │Batch 4 │ │Batch 5 │           │
│  │(File A)│ │(File A)│ │(File E)│ │(File F)│ │(File F)│           │
│  └───┬────┘ └───┬────┘ └───┬────┘ └───┬────┘ └───┬────┘           │
│      │          │          │          │          │                   │
│      └──────────┴──────────┴──────────┴──────────┘                  │
│                          │                                            │
│                          ▼                                            │
│              pa.concat_tables(                                        │
│                  batches,                                              │
│                  promote_options="permissive"   ← handles type diffs │
│              )                                                        │
│                          │                                            │
│                          ▼                                            │
│              ┌──────────────────────┐                                │
│              │   pa.Table (Arrow)    │                                │
│              │   Final result        │                                │
│              └──────────┬───────────┘                                │
│                         │                                             │
│           ┌─────────────┼──────────────┬──────────────┐              │
│           ▼             ▼              ▼              ▼              │
│     .to_arrow()   .to_pandas()  .to_duckdb()   .to_polars()        │
│     (direct)      (.to_pandas())  (register)    (pl.from_arrow)     │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Pruning Funnel — Data Elimination Summary

```
           ┌─────────────────────────────────────────┐
           │  ALL MANIFESTS IN SNAPSHOT               │
           │  (e.g., 100 manifest files)              │
           └────────────────┬────────────────────────┘
                            │
              Manifest Pruning (partition summaries)
                            │
           ┌────────────────▼────────────────────────┐
           │  SURVIVING MANIFESTS                     │
           │  (e.g., 30 manifests)                    │
           └────────────────┬────────────────────────┘
                            │
              Read manifest entries (parallel)
                            │
           ┌────────────────▼────────────────────────┐
           │  ALL DATA FILES                          │
           │  (e.g., 5000 data files)                 │
           └────────────────┬────────────────────────┘
                            │
              Partition Pruning (partition values)
                            │
           ┌────────────────▼────────────────────────┐
           │  PARTITION-MATCHED FILES                 │
           │  (e.g., 1200 files)                      │
           └────────────────┬────────────────────────┘
                            │
              Metrics Pruning (min/max/null stats)
                            │
           ┌────────────────▼────────────────────────┐
           │  STATS-MATCHED FILES                     │
           │  (e.g., 300 files)                       │
           └────────────────┬────────────────────────┘
                            │
              Parquet Row Group Filtering
              (Parquet-level stats)
                            │
           ┌────────────────▼────────────────────────┐
           │  MATCHING ROW GROUPS                     │
           │  (e.g., 150 row groups)                  │
           └────────────────┬────────────────────────┘
                            │
              Row-Level Filtering
              (predicate on actual values)
                            │
           ┌────────────────▼────────────────────────┐
           │  FINAL MATCHING ROWS                     │
           │  (e.g., 50,000 rows)                     │
           └─────────────────────────────────────────┘

  100 manifests → 30 manifests → 5000 files → 1200 files → 300 files
  → 150 row groups → 50,000 rows

  Without Iceberg: full scan of ALL 5000 files
  With Iceberg:    read only 300 files, skip 94% of data
```

---

## Time Travel

```
table.scan(snapshot_id=8423947293846).to_arrow()

metadata.json
├── snapshots: [
│     {snapshot_id: 8423947293845, timestamp: "2024-01-01T00:00:00"},  ← v1
│     {snapshot_id: 8423947293846, timestamp: "2024-01-15T00:00:00"},  ← v2 (selected)
│     {snapshot_id: 8423947293847, timestamp: "2024-02-01T00:00:00"},  ← v3 (current)
│   ]
└── refs: {"main": snapshot_id: 8423947293847}

Time travel reads snapshot v2's manifest list, seeing the table
as it existed on Jan 15. All subsequent steps are identical —
just operating on a different snapshot's file set.
```

---

## Schema Evolution on Read

```
Timeline:
  v1: Schema = {id: int32, name: string}
  v2: Schema = {id: int64, name: string, email: string}  ← widened id, added email
  v3: Schema = {id: int64, name: string, email: string, status: string}  ← added status

Reading with current schema (v3):

┌─────────────────────────────────────────────────────────────────────┐
│ File written at v1          │ File written at v2                     │
│ Has: {id:int32, name:str}   │ Has: {id:int64, name:str, email:str}  │
│                              │                                        │
│ Read as:                     │ Read as:                               │
│ id:    int32 → cast → int64 │ id:    int64 (as-is)                  │
│ name:  string (as-is)       │ name:  string (as-is)                  │
│ email: NULL (missing col)    │ email: string (as-is)                  │
│ status: NULL (missing col)  │ status: NULL (missing col)             │
└─────────────────────────────┴────────────────────────────────────────┘

ArrowProjectionVisitor handles all transformations:
• Type widening (int32 → int64, float32 → float64)
• Missing columns → null arrays
• Column reordering to match current schema
• Nested field evolution (struct/list/map)
```

---

## Delete Handling Strategies

### Position Deletes (v2 format)

```
Delete File: {file_path, pos}
┌───────────────────────────────┬─────┐
│ file_path                      │ pos │
├───────────────────────────────┼─────┤
│ s3://bucket/data/00000-0.parq │  42 │
│ s3://bucket/data/00000-0.parq │ 108 │
│ s3://bucket/data/00000-1.parq │   7 │
└───────────────────────────────┴─────┘

Application during read:
1. Read all rows from data file
2. Look up delete positions for this file path
3. Create keep-mask: all positions EXCEPT deleted ones
4. batch.take(keep_indices)    ← removes deleted rows
5. THEN apply row filter (cannot push down with deletes)
```

### Why Filters Can't Be Pushed Down with Deletes

```
Without deletes:
  Row 0: {price: 150}  ← matches price > 100
  Row 1: {price: 50}   ← doesn't match (filtered at Parquet level)
  Row 2: {price: 200}  ← matches

With position delete at pos=0:
  Row 0: {price: 150}  ← DELETED (but Parquet doesn't know this)
  Row 1: {price: 50}   ← doesn't match
  Row 2: {price: 200}  ← matches

If filter pushed down, Parquet returns rows 0 and 2.
After delete removal of pos=0, we'd incorrectly keep row 2
but using wrong position mapping.

Solution: read ALL rows → apply deletes → THEN filter
```

---

## Parallel Execution Points

| Stage | What's Parallelized | Executor |
|-------|---------------------|----------|
| Manifest reading | Each manifest file read in separate thread | `ExecutorFactory.map()` |
| Delete file reading | All delete files read in parallel | `ExecutorFactory.map()` |
| Data file reading | Each FileScanTask in separate thread | `ExecutorFactory.map()` |

```
                    ┌─────────────┐
                    │  Thread Pool │
                    │  (Executor)  │
                    └──────┬──────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
  ┌───────────┐     ┌───────────┐     ┌───────────┐
  │ Read       │     │ Read       │     │ Read       │
  │ Manifest 1 │     │ Manifest 2 │     │ Manifest 3 │
  └─────┬─────┘     └─────┬─────┘     └─────┬─────┘
        │                  │                  │
        ▼                  ▼                  ▼
  (pruning & filtering applied per manifest)
        │                  │                  │
        ▼                  ▼                  ▼
  ┌───────────┐     ┌───────────┐     ┌───────────┐
  │ Read       │     │ Read       │     │ Read       │
  │ Parquet A  │     │ Parquet E  │     │ Parquet F  │
  └─────┬─────┘     └─────┬─────┘     └─────┬─────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
                    Yield batches to
                    caller as available
```

---

## Read Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `read.split.target-size` | 134,217,728 (128 MB) | Target split size for planning |
| `read.split.metadata-target-size` | 33,554,432 (32 MB) | Target size for metadata splits |
| `read.parquet.vectorization.enabled` | `true` | Vectorized Parquet reading |
| `read.parquet.vectorization.batch-size` | 5000 | Batch size for vectorized reads |
| `include_empty_files` | `false` | Whether to include empty files in scan |

---

## Output Formats

| Method | Returns | Notes |
|--------|---------|-------|
| `to_arrow()` | `pa.Table` | Materializes all data in memory |
| `to_pandas()` | `pd.DataFrame` | Converts via Arrow (`to_arrow().to_pandas()`) |
| `to_duckdb(table_name, connection)` | DuckDB table | Registers Arrow table with DuckDB |
| `to_polars()` | `pl.DataFrame` | Converts via Arrow (`pl.from_arrow()`) |
| `to_ray()` | `ray.data.Dataset` | Converts via Arrow (`ray.data.from_arrow()`) |
| `to_record_batches()` | `Iterator[pa.RecordBatch]` | Streaming — memory efficient for large scans |
| `count()` | `int` | Row count (uses manifest stats when possible) |
