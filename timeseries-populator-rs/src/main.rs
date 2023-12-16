use std::sync::Arc;

use chrono::{DateTime, Duration, FixedOffset, Timelike};
use deltalake::{
    arrow::{
        array::{Float32Array, Int16Array, Int32Array, TimestampMicrosecondArray},
        datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
        record_batch::RecordBatch,
    },
    parquet::{
        basic::{Compression, ZstdLevel},
        file::properties::WriterProperties,
        schema::types::ColumnPath,
    },
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaOps, SchemaField,
};
use rand::{thread_rng, Rng};

#[tokio::main]
async fn main() {
    let table_uri = "/home/ben/ffi-timeseries-rs/timeseries";
    let ops = DeltaOps::try_from_uri(table_uri).await.unwrap();

    let mut table = ops
        .create()
        .with_columns(get_timeseries_fields())
        .with_partition_columns(vec!["year"])
        .with_table_name("timeseries_values")
        .with_comment("Per-sensor values across time")
        .await
        .unwrap();

    let arrow_schema: Arc<ArrowSchema> = Arc::new(get_timeseries_arrow_schema());

    let sensor_ids = generate_sensor_ids();
    let years: Vec<i16> = (2000..2023).collect();

    let writer_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(6).unwrap()))
        .set_dictionary_enabled(true)
        // Don't dictionary encode the value field, as they'll be random floats which won't repeat often/at all
        .set_column_dictionary_enabled(ColumnPath::new(vec![String::from("value")]), false)
        .build();

    let mut writer = RecordBatchWriter::for_table(&table)
        .unwrap()
        .with_writer_properties(writer_properties);

    for year in years.into_iter() {
        let batch = generate_yearly_batch(&sensor_ids, year, arrow_schema.clone());
        writer.write(batch).await.unwrap();
        writer.flush_and_commit(&mut table).await.unwrap();
    }
}

fn generate_sensor_ids() -> Vec<i32> {
    (0..NUMBER_OF_SENSORS)
        .map(|_| thread_rng().gen_range(0..1000))
        .collect()
}

/// Generate a year's worth of data for the given sensor IDs. For each sensor, random values are
/// produced every 5 minutes from 08:00 - 17:00.
fn generate_yearly_batch(sensor_ids: &[i32], year: i16, schema: Arc<ArrowSchema>) -> RecordBatch {
    let mut rng = thread_rng();
    let yearly_timestamps = generate_yearly_timestamps(year);
    let number_of_rows = yearly_timestamps.len() * NUMBER_OF_SENSORS;

    let mut timestamp_column: Vec<i64> = Vec::with_capacity(number_of_rows);
    let mut year_column: Vec<i16> = Vec::with_capacity(number_of_rows);
    let mut id_column: Vec<i32> = Vec::with_capacity(number_of_rows);
    let mut value_column: Vec<f32> = Vec::with_capacity(number_of_rows);

    // Generate a column for sensor IDs. Each ID appears once every 5 minutes for 9 hours per day
    for timestamp in yearly_timestamps.iter() {
        for sensor in sensor_ids.iter() {
            timestamp_column.push(timestamp.timestamp_micros());
            year_column.push(year);
            id_column.push(*sensor);
            // Random value (eg: degrees Farenheit)
            value_column.push(rng.gen_range(0.0..100.0));
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(timestamp_column)),
            Arc::new(Int16Array::from(year_column)),
            Arc::new(Int32Array::from(id_column)),
            Arc::new(Float32Array::from(value_column)),
        ],
    )
    .unwrap()
}

/// Generate 5 minute timestamps every day for 1 year. Timestamps start on 1st January (inclusive).
/// Timestamps are generated for 08:00-17:00 (inclusive) for every day of the week.
fn generate_yearly_timestamps(year: i16) -> Vec<DateTime<FixedOffset>> {
    // 365 days * 9 hours per day * 12 timestamps per hour (1 timestamp every 5 minutes)
    let mut datetimes: Vec<DateTime<FixedOffset>> = Vec::with_capacity(365 * 9 * 12);
    let mut current_datetime =
        DateTime::parse_from_rfc3339(format!("{}-01-01T08:00:00+00:00", year).as_str()).unwrap();
    let end_datetime =
        DateTime::parse_from_rfc3339(format!("{}-12-31T17:01:00+00:00", year).as_str()).unwrap();

    while current_datetime < end_datetime {
        datetimes.push(current_datetime);

        current_datetime += Duration::minutes(5);

        // If the current DateTime is > 17:00, then move to 08:00 the next day
        if current_datetime.hour() == 17 && current_datetime.minute() > 0 {
            current_datetime += Duration::days(1);
            current_datetime = current_datetime
                .with_hour(8)
                .unwrap()
                .with_minute(0)
                .unwrap();
        }
    }

    datetimes
}

fn get_timeseries_fields() -> Vec<SchemaField> {
    vec![
        SchemaField::new(
            String::from("timestamp"),
            deltalake::SchemaDataType::primitive(String::from("timestamp")),
            false,
            Default::default(),
        ),
        SchemaField::new(
            String::from("year"),
            deltalake::SchemaDataType::primitive(String::from("short")),
            false,
            Default::default(),
        ),
        SchemaField::new(
            String::from("id"),
            deltalake::SchemaDataType::primitive(String::from("integer")),
            false,
            Default::default(),
        ),
        SchemaField::new(
            String::from("value"),
            deltalake::SchemaDataType::primitive(String::from("float")),
            false,
            Default::default(),
        ),
    ]
}

fn get_timeseries_arrow_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new(
            "timestamp",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("year", ArrowDataType::Int16, false),
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("value", ArrowDataType::Float32, false),
    ])
}

const NUMBER_OF_SENSORS: usize = 300;
