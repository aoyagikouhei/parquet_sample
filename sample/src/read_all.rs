use std::fs::File;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::util::pretty::print_batches;

pub fn execute(input: &str) -> anyhow::Result<()> {
    let file = File::open(input)?;
    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(8192)
        .build()?;
    let mut batches = Vec::new();
    for batch in parquet_reader {
        batches.push(batch?);
    }
    print_batches(&batches)?;
    Ok(())
}