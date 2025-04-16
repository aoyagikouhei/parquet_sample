

pub mod group;
pub mod maker;
pub mod move_csv;
pub mod read_all;
pub mod filter;
pub mod schema;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let command = std::env::var("COMMAND")?;
    match command.as_str() {
        // COMMAND=move_csv cargo run
        // duckdb
        // select * from 'output.parquet';
        "move_csv" => move_csv::execute("../input.csv", "output.parquet")?,
        // COMMAND=read_all cargo run
        "read_all" => read_all::execute("output.parquet")?,
        // COMMAND=filter cargo run
        "filter" => filter::execute("output.parquet").await?,
        // COMMAND=group cargo run
        "group" => group::execute("output.parquet").await?,
        // COMMAND=maker cargo run
        "maker" => maker::execute("result.parquet").await?,
        // COMMAND=group2 cargo run
        "group2" => group::execute("result.parquet").await?,
        _ => panic!("Unknown command: {}", command),
    }

    Ok(())
}