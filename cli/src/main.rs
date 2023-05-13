use clap::Parser;

use duckdb_polars::{export::duckdb::Connection, query_to_df_polars, DuckDBPolarsError};
use tracing::{info, instrument};

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, help = "SQL query to run")]
    sql: String,
}

impl Args {
    #[instrument]
    pub fn run(&self) -> Result<(), DuckDBPolarsError> {
        let conn = Connection::open_in_memory().expect("Failed to open connection");
        info!("Running query: {}", self.sql);
        let df = query_to_df_polars(&conn, &self.sql)?;
        info!("Output df: {}", df);
        info!("df schema: {:#?}", df.schema());

        Ok(())
    }
}

fn main() -> Result<(), DuckDBPolarsError> {
    tracing_subscriber::fmt::init();
    Args::parse().run()
}
