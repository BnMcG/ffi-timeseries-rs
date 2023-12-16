use std::sync::Arc;

use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use deltalake::{
    arrow::{
        array::{Array, StructArray},
        error::ArrowError,
        record_batch::RecordBatch,
    },
    datafusion::prelude::{SessionConfig, SessionContext},
};

pub fn query_timeseries(
    query: &str,
    timeseries_table_uri: &str,
) -> (FFI_ArrowArray, FFI_ArrowSchema) {
    // Tokio block_on because UniFFI doesn't generate async functions for C#
    // (Although I'm not sure if it would implicitly convert a Rust async fn into a blocking C# method)
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let batches = runtime.block_on(async move {
        let table = deltalake::open_table(timeseries_table_uri).await.unwrap();

        // Set MAX batch size as we only want to return a single batch to make the FFI interface easier to deal with
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(usize::MAX));
        ctx.register_table("timeseries", Arc::new(table)).unwrap();
        ctx.sql(query).await.unwrap().collect().await.unwrap()
    });

    let batch = batches.into_iter().next().unwrap();
    let array = record_batch_to_struct_array(&batch).unwrap().into_data();

    to_ffi(&array).unwrap()
}

fn record_batch_to_struct_array(batch: &RecordBatch) -> Result<StructArray, ArrowError> {
    // Collect fields and corresponding arrays from the record batch
    let arrays: Vec<Arc<dyn Array>> = (0..batch.num_columns())
        .map(|i| batch.column(i).clone())
        .collect();

    // Create a struct array from the fields
    StructArray::try_new(batch.schema().fields.clone(), arrays, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        // Should not panic if timeseries-populator-rs has run
        query_timeseries(
            "SELECT * FROM timeseries LIMIT 1",
            "/home/ben/ffi-timeseries-rs/timeseries",
        );
    }
}
