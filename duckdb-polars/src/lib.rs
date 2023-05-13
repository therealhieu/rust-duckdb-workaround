pub mod export;

use std::fmt::Display;

use duckdb::{arrow::record_batch::RecordBatch, Connection, Error as DuckDBError};
use polars::export::arrow as arrow2;
use polars::export::rayon::prelude::{
    IndexedParallelIterator, IntoParallelIterator, ParallelIterator,
};
use polars::prelude::*;
use polars_core::utils::accumulate_dataframes_vertical_unchecked;

#[derive(Debug)]
pub enum DuckDBPolarsError {
    Internal { msg: String },
    Polars { msg: String, source: PolarsError },
    DuckDB { msg: String, source: DuckDBError },
}

impl Display for DuckDBPolarsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            DuckDBPolarsError::Internal { msg } => msg,
            DuckDBPolarsError::Polars { msg, .. } => msg,
            DuckDBPolarsError::DuckDB { msg, .. } => msg,
        };
        write!(f, "{}", msg)
    }
}

impl From<PolarsError> for DuckDBPolarsError {
    fn from(e: PolarsError) -> Self {
        DuckDBPolarsError::Polars {
            msg: format!("Polars error: {}", e),
            source: e,
        }
    }
}

impl From<DuckDBError> for DuckDBPolarsError {
    fn from(e: DuckDBError) -> Self {
        DuckDBPolarsError::DuckDB {
            msg: format!("DuckDB error: {}", e),
            source: e,
        }
    }
}

pub fn arrowrs_record_batches_to_polars_df(
    rbs: Vec<RecordBatch>,
) -> Result<DataFrame, DuckDBPolarsError> {
    let column_names = rbs[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name())
        .cloned()
        .collect::<Vec<_>>();
    let dfs = rbs
        .into_par_iter()
        .map(|rb| {
            let s_vec = rb
                .columns()
                .into_par_iter()
                .enumerate()
                .map(|(i, array)| {
                    let arrowrs_array_data = array.as_ref().to_data();
                    let arrow2_array = arrow2::array::from_data(&arrowrs_array_data);

                    let name = column_names
                        .get(i)
                        .ok_or_else(|| DuckDBPolarsError::Internal {
                            msg: format!("Column name not found for index {}", i),
                        })?
                        .as_ref();

                    Series::try_from((name, arrow2_array)).map_err(DuckDBPolarsError::from)
                })
                .collect::<Result<Vec<_>, DuckDBPolarsError>>()?;

            Ok(DataFrame::new_no_checks(s_vec))
        })
        .collect::<Result<Vec<_>, DuckDBPolarsError>>()?;

    Ok(accumulate_dataframes_vertical_unchecked(dfs))
}

pub fn query_to_df_polars(conn: &Connection, query: &str) -> Result<DataFrame, DuckDBPolarsError> {
    let mut statement = conn.prepare(query)?;
    let rbs = statement.query_arrow([])?.collect::<Vec<_>>();
    arrowrs_record_batches_to_polars_df(rbs)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rstest::{fixture, rstest};
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestSpec {
        query: String,
        /// Expected in jsonlines format
        expected: String,
    }

    impl TestSpec {
        pub fn from_yaml(yaml: &str) -> Self {
            serde_yaml::from_str(yaml).expect("Failed to parse yaml")
        }

        fn run(&self, connection: &Connection) {
            let mut df = query_to_df_polars(connection, &self.query).expect("Failed to run query");
            let mut buf = Vec::new();
            JsonWriter::new(&mut buf)
                .with_json_format(JsonFormat::JsonLines)
                .finish(&mut df)
                .expect("Failed to serialize df");

            let actual = String::from_utf8(buf).expect("Failed to parse utf8");
            println!("{}", actual);
            assert_eq!(actual, self.expected);
        }
    }

    impl FromStr for TestSpec {
        type Err = serde_yaml::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            serde_yaml::from_str(s)
        }
    }

    #[fixture]
    #[once]
    fn connection() -> Connection {
        Connection::open_in_memory().expect("Failed to open connection")
    }

    #[rstest]
    #[case::simple_test(TestSpec::from_yaml(
        r#"
        query: |
            SELECT 1 AS a, 2 AS b
        expected: |
            {"a":1,"b":2}
    "#
    ))]
    #[case::read_ndjson(TestSpec::from_yaml(
        r#"
        query: |
            SELECT
                *
            FROM
                read_ndjson_auto("tests/fixtures/data1.jsonl")
        expected: |
            {"int":1,"str_list":["a","b","c"],"struct":{"float":1.0,"mixed":["1","\"a\""]}}
            {"int":2,"str_list":["d","e","f"],"struct":{"float":2.0,"mixed":["2","\"b\""]}}
            {"int":3,"str_list":["g","h","i"],"struct":{"float":3.0,"mixed":["3","\"c\""]}}
    "#
    ))]
    fn test_query_to_df_polars(connection: &Connection, #[case] test_spec: TestSpec) {
        test_spec.run(connection);
    }
}
