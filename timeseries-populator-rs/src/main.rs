use std::sync::Arc;

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime};
use deltalake::{
    arrow::{
        array::{Float32Array, Int32Array, TimestampMicrosecondArray},
        datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
        record_batch::RecordBatch,
    },
    parquet::{
        basic::{Compression, ZstdLevel},
        file::properties::WriterProperties,
        schema::types::ColumnPath,
    },
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaOps
};
use deltalake::arrow::compute::concat_batches;
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::kernel::{Action, DataType, StructField};
use deltalake::operations::transaction::CommitBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use rand::{thread_rng, Rng};


struct BatchedTimeSeries {
    schema: SchemaRef,
    // Inclusive
    end_year: i32,
    sensor_ids: Vec<i32>,
    current_timestamp: NaiveDateTime,
    buffered_batches: Vec<RecordBatch>,
    buffered_batches_size_bytes: usize,
    target_batch_size_bytes: usize,
    timestamp_column: Vec<i64>,
    id_column: Vec<i32>,
    value_column: Vec<f32>
}

impl BatchedTimeSeries {
    pub fn new(schema: SchemaRef, target_batch_size_bytes: usize, start_year :i32, end_year: i32, sensor_ids: Vec<i32>) -> Self {
        BatchedTimeSeries {
            schema,
            target_batch_size_bytes,
            sensor_ids,
            end_year,
            current_timestamp: NaiveDate::from_ymd_opt(start_year, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            buffered_batches: Vec::new(),
            buffered_batches_size_bytes: 0,
            timestamp_column: Vec::with_capacity(NUMBER_OF_SENSORS),
            id_column: Vec::with_capacity(NUMBER_OF_SENSORS),
            value_column: Vec::with_capacity(NUMBER_OF_SENSORS)
        }
    }
}

impl Iterator for BatchedTimeSeries {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_timestamp.year() > self.end_year {
            // We've iterated over the given range
            return None;
        }

        let mut rng = thread_rng();

        while self.current_timestamp.year() <= self.end_year {
            // Write values for all sensors
            for id in self.sensor_ids.iter() {
                self.timestamp_column.push(self.current_timestamp.and_utc().timestamp());
                self.id_column.push(*id);
                // Random value (eg: degrees Fahrenheit)
                self.value_column.push(rng.gen_range(0.0..100.0));
            }

            let record_batch = RecordBatch::try_new(self.schema.clone(), vec![
                Arc::new(TimestampMicrosecondArray::from_iter_values(self.timestamp_column.drain(..))),
                Arc::new(Int32Array::from_iter_values(self.id_column.drain(..))),
                Arc::new(Float32Array::from_iter_values(self.value_column.drain(..)))
            ]).unwrap();

            // Move current time along
            self.current_timestamp += Duration::minutes(5);

            let new_buffer_size = self.buffered_batches_size_bytes + record_batch.get_array_memory_size();
            self.buffered_batches.push(record_batch);
            self.buffered_batches_size_bytes = new_buffer_size;

            if new_buffer_size >= self.target_batch_size_bytes {
                self.buffered_batches_size_bytes = 0;
                let batches = self.buffered_batches.drain(..);
                return Some(concat_batches(&self.schema, batches.as_slice()).unwrap())
            }
        }

        // We've iterated over the given range
        None
    }
}

#[tokio::main]
async fn main() {
    let table_uri = "/home/ben/ffi-timeseries-rs/timeseries";
    let ops = DeltaOps::try_from_uri(table_uri).await.unwrap();

    let table = ops
        .create()
        .with_columns(get_time_series_fields())
        .with_table_name("time_series_values")
        .with_comment("Per-sensor values across time")
        .await
        .unwrap();

    let writer_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(9).unwrap()))
        .set_dictionary_enabled(true)
        // Don't dictionary encode the value field, as they'll be random floats which won't repeat often/at all
        .set_column_dictionary_enabled(ColumnPath::new(vec![String::from("value")]), false)
        .build();

    // Use RecordBatchWriter instead of DeltaOps so that we can control when we commit
    // changes (versus making 1 commit for every written batch).
    let mut writer = RecordBatchWriter::for_table(&table)
        .unwrap()
        .with_writer_properties(writer_properties);

    let arrow_schema: Arc<ArrowSchema> = Arc::new(get_time_series_arrow_schema());
    let sensor_ids = generate_sensor_ids();

    let batched_time_series = BatchedTimeSeries::new(
        arrow_schema.clone(),
        256 * MEGABYTE,
        2013,
        2023,
        sensor_ids
    );

    let state = table.snapshot().unwrap();
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: None,
        predicate: None
    };

    let mut actions: Vec<Action> = Vec::new();

    for time_series in batched_time_series {
        writer.write(time_series).await.unwrap();
        let mut time_series_actions: Vec<Action> = writer.flush().await.unwrap().into_iter().map(|a| a.into()).collect();
        actions.append(&mut time_series_actions);
    }

    // Write a commit to the table with the appended data
    CommitBuilder::default()
        .with_actions(actions)
        .build(Some(state), table.log_store(), operation)
        .unwrap()
        .await
        .unwrap();
}

fn generate_sensor_ids() -> Vec<i32> {
    (0..NUMBER_OF_SENSORS)
        .map(|_| thread_rng().gen_range(0..10000))
        .collect()
}

fn get_time_series_fields() -> Vec<StructField> {
    vec![
        StructField::new(
            String::from("timestamp"),
            DataType::TIMESTAMPNTZ,
            Default::default(),
        ),
        StructField::new(
            String::from("id"),
            DataType::INTEGER,
            false
        ),
        StructField::new(
            String::from("value"),
            DataType::FLOAT,
            false,
        ),
    ]
}

fn get_time_series_arrow_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new(
            "timestamp",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("value", ArrowDataType::Float32, false),
    ])
}

const NUMBER_OF_SENSORS: usize = 3000;
const MEGABYTE: usize = 1000000;