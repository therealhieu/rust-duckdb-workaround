# Rust DuckDB-Polars Workaround
## Introduction
This repository provides a solution to convert DuckDB query result to Polars DataFrame.

For the one who is not familiar with Apache Arrow in Rust, DuckDB and Polars use diffrent Apache Arrow versions. While DuckDB uses [arrow-rs](https://github.com/apache/arrow-rs), Polars uses [arrow2](https://github.com/jorgecarleitao/arrow2). Difference between these two versions can be discovered more in [this](https://github.com/apache/arrow-rs/issues/1176). In brief: 

- `Arrow2 uses Box<dyn Array> as children to allow easy mutation; arrow-rs uses Arc<dyn Array>`

- `Arrow2 does not have TimestampArray nor DecimalArray, and instead sticks to the physical types only`

One more tricky thing worth mentioning is that Polars does not use the original [jorgecarleitao/arrow2](https://github.com/jorgecarleitao/arrow2), but a forked version [ritchie46/arrow2](https://github.com/ritchie46/arrow2)

Because there are already existing implmentations for [arrow-rs - arrow2 conversion](https://github.com/jorgecarleitao/arrow2/blob/main/src/datatypes/mod.rs), most of efforts in this repository is to resolve the dependency conflicts: `ensure DuckDB's arrow-rs and arrow2's arrow-rs have the same version -> ensrue Polars uses this arrow2 version`.

To get things done, I fork [ritchier46/arrow2](https://github.com/ritchie46/arrow2) and bump dependencies relating to arrow-rs to the same version as DuckDB's arrow-rs. I made a pull request to [ritchier46/arrow2](https://github.com/ritchie46/arrow2/pull/10) and another one to [jorgecarleitao/arrow2](https://github.com/jorgecarleitao/arrow2/pull/1482). `When these two PRs are merged, we can make a PR to DuckDB to integrate this workaround`.

## DuckDB-Polars conversion
We can retrieve DuckDB query result as a [`Vec<RecordBatch>`](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html). In eanch RecordBatch, we iterate over its columns, convert each column into [`Box<dyn arrow2::Array>`](https://docs.rs/arrow2/latest/arrow2/array/trait.Array.html) -> [polars::Series](https://docs.rs/polars/latest/polars/series/struct.Series.html), and after collecting `Vec<Series>` we get a DataFrame.
## Project structure
```
duckdb-polars/
- src/
    - lib.rs -> main logic
    - export.rs -> re-exported dependencies
- tests/
    - fixtures/ -> test data

cli/ -> CLI binary
```

## CLI
In root directory, run:
```bash
cargo build --release
```
To run the CLI, run:
```bash
./target/release/duckdb-polars-cli --help
```

```
Usage: duckdb-polars-cli --sql <SQL>

Options:
  -s, --sql <SQL>  SQL query to run
  -h, --help       Print help
  -V, --version    Print version
```

Example:
```bash
./target/release/duckdb-polars-cli --sql 'SELECT 1 AS a, 2 AS b'  
```
```bash
2023-05-13T09:38:28.784456Z  INFO run{self=Args { sql: "SELECT 1 AS a, 2 AS b" }}: duckdb_polars_cli: Running query: SELECT 1 AS a, 2 AS b
2023-05-13T09:38:28.788924Z  INFO run{self=Args { sql: "SELECT 1 AS a, 2 AS b" }}: duckdb_polars_cli: Output df: shape: (1, 2)
┌─────┬─────┐
│ a   ┆ b   │
│ --- ┆ --- │
│ i32 ┆ i32 │
╞═════╪═════╡
│ 1   ┆ 2   │
└─────┴─────┘
2023-05-13T09:38:28.791118Z  INFO run{self=Args { sql: "SELECT 1 AS a, 2 AS b" }}: duckdb_polars_cli: df schema: Schema:
name: a, data type: Int32
name: b, data type: Int32
```