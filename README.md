# PyIceberg Conference 2026

Workshop materials and infrastructure for the **PyIceberg Write Path & Read Path Deep Dive** sessions at Iceberg Summit, April 8, 2026.

---

## What This Repo Contains

```
pyiceberg_conf_2026/
├── infra/
│   └── docker-compose.yml        # Local stack: MinIO + PostgreSQL + REST Catalog
└── workshop_materials/
    ├── write_path_deep_dive_vipin.ipynb   # Write path workshop
    ├── read_path_deep_dive.ipynb          # Read path workshop
    └── requirements.txt                   # Python dependencies
```

---

## Infrastructure

The `infra/docker-compose.yml` spins up a fully local Iceberg environment — no cloud accounts needed.

| Service | Port | Purpose |
|---------|------|---------|
| **REST Catalog** (`tabulario/iceberg-rest`) | `8181` | Iceberg REST catalog — tracks table locations and commits |
| **MinIO** (S3-compatible object storage) | `9000` (API) / `9001` (Console) | Stores Parquet data files + Avro metadata files |
| **PostgreSQL 16** | `5432` | Catalog backend — persists snapshot state and table pointers |
| **minio-init** | — | One-shot container that creates the `warehouse` bucket on startup |

**Start the stack:**
```bash
cd infra
docker-compose up -d
```

**Verify:**
- MinIO Console → [http://localhost:9001](http://localhost:9001) (admin / password)
- REST Catalog → [http://localhost:8181/v1/config](http://localhost:8181/v1/config)

---

## Workshop Materials

### 1. Write Path Deep Dive (`write_path_deep_dive_vipin.ipynb`)

Traces exactly what happens inside PyIceberg when you call `table.append()` — from in-memory schema validation all the way to the atomic catalog commit.

**What you'll build:** A `sensor_readings` table with synthetic Picarro gas-analyzer data (sensor_id, timestamp, co2_ppm, ch4_ppb).

| Section | What You'll Do | Key Concept |
|---------|---------------|-------------|
| Connect to Catalog | Connect to REST Catalog | Lazy connection — no HTTP until first use |
| Create Namespace | Create `pyiceberg_demo` | Metadata-only — nothing written to S3 |
| Create Table | Define schema, create empty table | Field IDs are permanent — not column names |
| Append | Append 3 batches, trace the 6-stage write path | Parquet → Manifest → Manifest List → Atomic Commit |
| Partitioned Append | Partition by `sensor_id`, see per-partition files | Rows binned before writing — 1 file per partition value |
| Overwrite | Replace rows matching a filter | ADDED + DELETED manifest entries, no in-place mutation |

**The 6-stage write path (what every `table.append()` does):**

```
Stage 1: Transaction open              (IN MEMORY — nothing written)
Stage 2: Schema validation             (IN MEMORY — PyArrow ↔ Iceberg type check)
Stage 3: DataFrame → Parquet → S3      (S3 WRITE #1 — your actual data)
Stage 4: Manifest file → S3            (S3 WRITE #2 — file index with column stats)
Stage 5: Manifest list + Snapshot → S3 (S3 WRITE #3 — snapshot table of contents)
Stage 6: Catalog commit                (ATOMIC GATE — HTTP POST with optimistic lock)
```

Nothing is visible to readers until Stage 6 completes.

---

### 2. Read Path Deep Dive (`read_path_deep_dive.ipynb`)

> Run the write-path notebook first — this one depends on the 3-snapshot table it creates.

Walks through how PyIceberg plans and executes a scan — from catalog lookup through two-level file pruning to final Arrow output.

| Section | What You'll See | Maps To |
|---------|----------------|---------|
| Connect & Load Table | Entry point — catalog lookup | Title slide |
| Metadata Hierarchy | Walk the 4-layer tree in code | Slide 2 |
| Full Scan (no filter) | `plan_files()` baseline — all files | Slide 4 |
| Filtered Scan | Two-level pruning proof | Slide 3 |
| Pruning Funnel | Column min/max elimination deep dive | Slide 5 |
| Time Travel | Read historical snapshots by ID | Slide 6 |
| Snapshot Isolation | Concurrent read safety — MVCC demo | Slide 6 |
| Column Projection | Read only needed columns | Slide 4 |
| Schema Evolution | Field IDs, type widening, NULLs on read | Slide 7 |
| Partition Evolution | Hidden partitioning — no rewrite needed | Slide 7 |
| Q&A Prep | `inspect.snapshots/manifests/files` | Slide 9 |

**The read path flow:**
```
table.scan(row_filter, selected_fields, snapshot_id)
    → Snapshot Resolution   (which version?)
    → Schema Projection     (which columns?)
    → Manifest Pruning      (skip files by partition summary)
    → Metrics Pruning       (skip files by column min/max)
    → Parallel Parquet Read (only surviving files, only needed columns)
    → Schema Evolution Proj (type cast, fill NULLs, reorder)
    → pa.Table / DataFrame
```

**Key stat:** Planning costs ~5 S3 reads (constant, regardless of table size). Only matching files are opened.

---

## Getting Started

**1. Install Python dependencies:**
```bash
pip install "pyiceberg[pyarrow]" pyarrow pandas
```

Or use the requirements file:
```bash
pip install -r workshop_materials/requirements.txt
```

**2. Start the Docker stack:**
```bash
cd infra && docker-compose up -d
```

**3. Run the notebooks in order:**
1. `write_path_deep_dive_vipin.ipynb` — creates the table and 3 snapshots
2. `read_path_deep_dive.ipynb` — reads and inspects that table

---

## Key Concepts Covered

- **Field IDs are permanent** — renaming a column never changes its ID; this is how schema evolution works safely across millions of files
- **Optimistic concurrency** — the atomic commit uses `AssertRefSnapshotId`; if two writers race, one gets a `409 Conflict` and retries
- **No in-place mutation** — overwrites write a new file and mark the old one `DELETED` in the manifest; old files stay on S3 until `expire_snapshots()` cleans them up
- **Immutable snapshots** — readers pin to a snapshot at scan start; writers never block readers (MVCC)
- **Two-level pruning** — manifest-level partition summaries first, then per-file column min/max stats — files are eliminated without being opened
