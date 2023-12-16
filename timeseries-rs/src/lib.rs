use std::{
    ffi::{c_char, CStr},
    sync::Arc,
    time::Instant,
};

use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use deltalake::{
    arrow::{
        array::{Array, StructArray},
        error::ArrowError,
        record_batch::RecordBatch,
    },
    datafusion::prelude::{SessionConfig, SessionContext},
};

#[repr(C)]
pub struct FfiReturnValue {
    array: FFI_ArrowArray,
    schema: FFI_ArrowSchema,
}

/// # Safety
/// The caller must ensure that the pointers for query_c, timeseries_table_uri_c are valid.
#[no_mangle]
pub unsafe extern "C" fn query_timeseries(
    query_c: *const c_char,
    timeseries_table_uri_c: *const c_char,
) -> FfiReturnValue {
    let query = unsafe { CStr::from_ptr(query_c).to_str().unwrap() };
    let timeseries_table_uri = unsafe { CStr::from_ptr(timeseries_table_uri_c).to_str().unwrap() };

    let started_at = Instant::now();
    // Tokio block_on because I've not looked into FFI-ing async functions...
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let batches = runtime.block_on(async move {
        let table = deltalake::open_table(timeseries_table_uri).await.unwrap();

        let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(4_000_000));
        ctx.register_table("timeseries", Arc::new(table)).unwrap();
        ctx.sql(query).await.unwrap().collect().await.unwrap()
    });

    let batch = batches.into_iter().next().unwrap();
    let array = record_batch_to_struct_array(&batch).unwrap().into_data();

    let (ffi_array, ffi_schema) = to_ffi(&array).unwrap();
    let execution_duration = started_at.elapsed();
    println!(
        "[rust] query executed in {}ms",
        execution_duration.as_millis()
    );

    FfiReturnValue {
        array: ffi_array,
        schema: ffi_schema,
    }
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
    use std::ffi::CString;

    use super::*;

    #[test]
    fn it_works() {
        // Should not panic if timeseries-populator-rs has run
        let query = CString::new("SELECT * FROM timeseries LIMIT 1").unwrap();
        let uri = CString::new("/home/ben/ffi-timeseries-rs/timeseries").unwrap();

        unsafe {
            query_timeseries(query.as_ptr(), uri.as_ptr());
        }
    }
}
