use std::sync::Arc;
use crate::schema::make_schema;
use std::fs::File;
use arrow::csv;
use parquet::arrow::ArrowWriter;

pub fn execute(input: &str, output: &str) -> anyhow::Result<()> {
    // スキーマー定義
    let schema = Arc::new(make_schema());

    // CSVファイルを読み込み、Parquetファイルに変換
    let file = File::open(input)?;
    let mut csv = csv::ReaderBuilder::new(Arc::clone(&schema)).with_header(true)
        .build(file)?;

    let output = File::create(output)?;
    let mut writer = ArrowWriter::try_new(output, Arc::clone(&schema), None)?;
    while let Some(batch) = csv.next() {
        let batch = batch?;
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}