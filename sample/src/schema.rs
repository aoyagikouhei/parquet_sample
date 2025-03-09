use arrow::datatypes::{DataType, Field, Schema};

pub fn make_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("content", DataType::Utf8, false),
        Field::new("page", DataType::UInt64, false),
    ])
}