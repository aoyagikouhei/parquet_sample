

pub mod group;
pub mod maker;
pub mod move_csv;
pub mod read_all;
pub mod filter;
pub mod schema;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    move_csv::execute("../input.csv", "output.parquet")?;
    read_all::execute("output.parquet")?;
    filter::execute("output.parquet").await?;
    group::execute("output.parquet").await?;
    maker::execute("result.parquet").await?;
    read_all::execute("result.parquet")?;
    group::execute("result.parquet").await?;

    Ok(())
}