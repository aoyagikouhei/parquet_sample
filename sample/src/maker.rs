use std::sync::Arc;

use arrow::array::RecordBatch;
use parquet::arrow::AsyncArrowWriter;
use tokio::fs::File;
use crate::schema::make_schema;

pub async fn execute(input: &str) -> anyhow::Result<()> {
    let file = File::create(input).await.unwrap();
    let schema = Arc::new(make_schema());
    let mut writer: AsyncArrowWriter<File> = AsyncArrowWriter::try_new(file, Arc::clone(&schema), None)?;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::UInt64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(arrow::array::StringArray::from(vec!["x", "y", "z", "w", "a"])),
            Arc::new(arrow::array::StringArray::from(vec!["xxx", "yyy", "zzz", "www", "aaa"])),
            Arc::new(arrow::array::UInt64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )?;
    writer.write(&batch).await?;
    writer.flush().await?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::UInt64Array::from(vec![6, 7, 8, 9, 10])),
            Arc::new(arrow::array::StringArray::from(vec!["b", "c", "d", "e", "f"])),
            Arc::new(arrow::array::StringArray::from(vec!["bbb", "ccc", "ddd", "eee", "fff"])),
            Arc::new(arrow::array::UInt64Array::from(vec![600, 700, 800, 900, 1000])),
        ],
    )?;
    writer.write(&batch).await?;
    writer.flush().await?;

    writer.close().await?;
    Ok(())
}