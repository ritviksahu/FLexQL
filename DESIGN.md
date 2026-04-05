# FlexQL Design Notes

## Scope

This implementation is a C++ FlexQL server and client API that is compatible with the provided `benchmark_flexql.cpp` workflow. The backend is a custom persistent storage engine implemented in C++ and stored under the `dbdata/` directory.

## How To Run

Build the project from the repository root:

```bash
sh compile.sh
```

Start the server in one terminal:

```bash
./build/server
```

Run the provided benchmark in another terminal:

```bash
./build/benchmark
```

Run only the benchmark unit tests:

```bash
./build/benchmark --unit-test
```

Run the benchmark with a custom row count:

```bash
./build/benchmark 200000
```

Run the additional workload suite:

```bash
./build/stress_benchmark
```

Run the workload suite with custom parameters:

```bash
./build/stress_benchmark 5000 4 1000 25
```

Parameters for `stress_benchmark` are:

- seed rows
- client threads
- operations per thread
- write batch size

For a clean rerun of the provided benchmark, stop the server and remove the persistent database files:

```bash
rm -rf dbdata
```

## Storage and Durability

- Primary storage is on disk inside `dbdata/`, not RAM.
- Each table has a metadata file and a data file.
- `CREATE TABLE` and `DROP TABLE` use a small journal file so interrupted structural changes can be recovered safely.
- Inserts are committed durably to an append-only insert WAL first, then checkpointed to table data files in batches.
- Startup recovery loads table files, replays any unfinished journal/WAL work, and checkpoints it back to the data files.
- Memory is used only as an accelerator through in-memory row caches and equality indexes rebuilt from persisted data.

## Server Model

- TCP server listens on `127.0.0.1:9000`.
- Each client is handled on a dedicated thread.
- SQL is accepted as semicolon-terminated statements. The parser is intentionally narrow because the benchmark sends one complete statement at a time.
- The engine keeps persisted rows in memory as a secondary cache for faster reads, while disk remains the source of truth.
- A batched checkpoint path reduces insert fsync overhead by flushing many committed WAL-backed rows to table files together.

## Query Support

The custom SQL engine supports the required comparison operators in `WHERE` and `JOIN` clauses:

- `=`
- `>`
- `<`
- `>=`
- `<=`

Supported statements for this project:

- `CREATE TABLE`
- `DROP TABLE [IF EXISTS]`
- `INSERT INTO ... VALUES (...), (...), ...`
- `SELECT`
- `SELECT ... INNER JOIN ... ON ...`
- `SELECT COUNT(*) ...`

Batch insertion is supported natively by parsing and applying multi-row `INSERT INTO ... VALUES (...), (...);` statements in a single request.

## Wire Protocol

- Client sends raw SQL text.
- Server returns zero or more `ROW ...` lines for result rows.
- Each row is encoded as a column count followed by length-prefixed column names and values.
- Server terminates every response with `END`.
- Errors are returned as `ERROR: ...` followed by `END`.

## Trade-offs

- The storage engine is intentionally small and focused on the SQL subset required by the benchmark and project statement, rather than trying to be a full general-purpose database.
- Dedicated per-client threads keep the implementation simple, though a production system would likely move to a thread pool or event loop.
- Parsing is custom and intentionally limited to the supported SQL subset.
- Equality indexes are kept in memory to improve read and join performance, but persisted table files remain the primary durable storage.
- Recovery is based on replaying a small journal plus an append-only insert WAL, which is much simpler than implementing a full MVCC or page-cache architecture.
- Batched checkpointing improves insert throughput significantly, but it is still much simpler than a production-grade LSM tree or B-tree storage engine.

## Extra Measurement Coverage

The project now includes an additional executable, `stress_benchmark`, for broader evaluation beyond the provided benchmark:

- Read-heavy workload with concurrent join/count queries.
- Write-heavy workload with concurrent batched inserts.
- Mixed multi-client workload with simultaneous reads and writes.

This does not replace the provided benchmark. It complements it so the project can be checked against changing benchmark shapes and more realistic concurrent access patterns.
