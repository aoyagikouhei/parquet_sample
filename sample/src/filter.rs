use std::time::SystemTime;
use arrow::array::{BooleanArray, Scalar, UInt64Array};
use arrow::compute::kernels::cmp::eq;
use arrow::error::ArrowError;
use arrow::util::pretty::print_batches;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use futures_util::stream::TryStreamExt;

// 2つのBooleanArrayをORで結合する
fn merge_bool_array(a: Result<BooleanArray, ArrowError>, b: Result<BooleanArray, ArrowError>) -> Result<BooleanArray, ArrowError> {
    if let Ok(a) = a {
        if let Ok(b) = b {
            // resとres2の結果をORで結合。個別に分解して行う
            Ok(a.iter().zip(b.iter()).map(|(a, b)| Some(a.unwrap() | b.unwrap())).collect())
        } else {
            Ok(a)
        }
    } else {
        a
    }
}

pub async fn execute(input: &str) -> anyhow::Result<()> {
    let file = tokio::fs::File::open(input).await?;

    let mut builder = ParquetRecordBatchStreamBuilder::new(file)
        .await?
        .with_batch_size(8192);

    // 3列のみを読み込む
    let file_metadata = builder.metadata().file_metadata().clone();
    let mask = ProjectionMask::roots(file_metadata.schema_descr(), [0, 1, 2]);
    builder = builder.with_projection(mask);

    // IDが1または5の行のみを抽出
    let scalar1 = UInt64Array::from(vec![1]);
    let scalar2 = UInt64Array::from(vec![5]);
    let filter = ArrowPredicateFn::new(
        ProjectionMask::roots(file_metadata.schema_descr(), [0]),
        move |record_batch| {
            let res1 = eq(record_batch.column(0), &Scalar::new(&scalar1));
            let res2 = eq(record_batch.column(0), &Scalar::new(&scalar2));
            merge_bool_array(res1, res2)
        },
    );

    let row_filter = RowFilter::new(vec![Box::new(filter)]);
    builder = builder.with_row_filter(row_filter);

    let stream = builder.build()?;

    let start = SystemTime::now();

    let result = stream.try_collect::<Vec<_>>().await?;

    println!("took: {} ms", start.elapsed()?.as_millis());

    print_batches(&result)?;
    Ok(())
}
