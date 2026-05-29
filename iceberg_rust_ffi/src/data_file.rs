/// DataFiles FFI — opaque handle, inspection helpers, and table listing.
use std::collections::HashMap;
use std::ffi::c_void;

use iceberg::spec::{DataContentType, DataFile, Datum, ManifestContentType};
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, NotifyGuard, ResponseGuard, RT,
};

use crate::error_codes::{classified_error, IcebergErrorCode};
use crate::response::IcebergBoxedResponse;

/// Opaque handle for a collection of data files.
///
/// Produced by writers and by `iceberg_table_list_data_files`. Consumed
/// (files moved out) by `iceberg_fast_append_action_add_data_files`,
/// `iceberg_overwrite_action_add_data_files`, and
/// `iceberg_overwrite_action_delete_data_files`. Free with
/// `iceberg_data_files_free` after use.
pub struct IcebergDataFiles {
    pub data_files: Vec<DataFile>,
}

unsafe impl Send for IcebergDataFiles {}
unsafe impl Sync for IcebergDataFiles {}

/// Type alias for async operations that return a DataFiles handle.
pub type IcebergDataFilesResponse = IcebergBoxedResponse<IcebergDataFiles>;

/// Free a data files handle.
#[no_mangle]
pub extern "C" fn iceberg_data_files_free(data_files: *mut IcebergDataFiles) {
    if !data_files.is_null() {
        unsafe {
            let _ = Box::from_raw(data_files);
        }
    }
}

/// Return the number of data files in the handle (0 if null).
#[no_mangle]
pub extern "C" fn iceberg_data_files_len(data_files: *const IcebergDataFiles) -> usize {
    if data_files.is_null() {
        return 0;
    }
    unsafe { (*data_files).data_files.len() }
}

/// Return a JSON array of objects describing every data file in the handle.
///
/// Each object contains all metadata fields from the Iceberg DataFile spec:
/// content, file_path, file_format, record_count, file_size_in_bytes,
/// column_sizes, value_counts, null_value_counts, nan_value_counts,
/// lower_bounds, upper_bounds, split_offsets, sort_order_id, equality_ids,
/// first_row_id, referenced_data_file, content_offset, content_size_in_bytes.
///
/// Returns null on error or if `data_files` is null.
/// The caller must free the returned string with `iceberg_destroy_cstring`.
#[no_mangle]
pub extern "C" fn iceberg_data_files_to_json(
    data_files: *const IcebergDataFiles,
) -> *mut std::ffi::c_char {
    #[derive(serde::Serialize)]
    struct DataFileJson<'a> {
        content: &'static str,
        file_path: &'a str,
        file_format: String,
        record_count: u64,
        file_size_in_bytes: u64,
        column_sizes: &'a HashMap<i32, u64>,
        value_counts: &'a HashMap<i32, u64>,
        null_value_counts: &'a HashMap<i32, u64>,
        nan_value_counts: &'a HashMap<i32, u64>,
        lower_bounds: HashMap<i32, &'a Datum>,
        upper_bounds: HashMap<i32, &'a Datum>,
        split_offsets: Option<&'a [i64]>,
        sort_order_id: Option<i32>,
        equality_ids: Option<Vec<i32>>,
        first_row_id: Option<i64>,
        referenced_data_file: Option<String>,
        content_offset: Option<i64>,
        content_size_in_bytes: Option<i64>,
    }

    if data_files.is_null() {
        return std::ptr::null_mut();
    }
    let df_ref = unsafe { &*data_files };

    let entries: Vec<DataFileJson> = df_ref
        .data_files
        .iter()
        .map(|f| DataFileJson {
            content: match f.content_type() {
                DataContentType::Data => "data",
                DataContentType::PositionDeletes => "position_deletes",
                DataContentType::EqualityDeletes => "equality_deletes",
            },
            file_path: f.file_path(),
            file_format: f.file_format().to_string(),
            record_count: f.record_count(),
            file_size_in_bytes: f.file_size_in_bytes(),
            column_sizes: f.column_sizes(),
            value_counts: f.value_counts(),
            null_value_counts: f.null_value_counts(),
            nan_value_counts: f.nan_value_counts(),
            lower_bounds: f.lower_bounds().iter().map(|(k, v)| (*k, v)).collect(),
            upper_bounds: f.upper_bounds().iter().map(|(k, v)| (*k, v)).collect(),
            split_offsets: f.split_offsets(),
            sort_order_id: f.sort_order_id(),
            equality_ids: f.equality_ids(),
            first_row_id: f.first_row_id(),
            referenced_data_file: f.referenced_data_file(),
            content_offset: f.content_offset(),
            content_size_in_bytes: f.content_size_in_bytes(),
        })
        .collect();

    match serde_json::to_string(&entries) {
        Ok(json) => match std::ffi::CString::new(json) {
            Ok(c_str) => c_str.into_raw(),
            Err(_) => std::ptr::null_mut(),
        },
        Err(_) => std::ptr::null_mut(),
    }
}

export_runtime_op!(
    iceberg_table_list_data_files,
    crate::IcebergDataFilesResponse,
    || {
        if table.is_null() {
            return Err(classified_error(
                IcebergErrorCode::STATE_RESOURCE_FREED,
                "Resource has been freed",
                "Null table pointer provided",
            ));
        }
        let table_ref = unsafe { &*table };
        Ok(table_ref)
    },
    table_ref,
    async {
        let table = &table_ref.table;
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok::<IcebergDataFiles, anyhow::Error>(IcebergDataFiles {
                data_files: vec![],
            });
        };

        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to load manifest list: {e}"))?;

        let mut data_files = Vec::new();
        for manifest_entry in manifest_list.entries() {
            if manifest_entry.content != ManifestContentType::Data {
                continue;
            }
            if !manifest_entry.has_added_files() && !manifest_entry.has_existing_files() {
                continue;
            }
            let manifest = manifest_entry
                .load_manifest(table.file_io())
                .await
                .map_err(|e| anyhow::anyhow!("Failed to load manifest: {e}"))?;
            for entry in manifest.entries() {
                if entry.is_alive() {
                    data_files.push(entry.data_file().clone());
                }
            }
        }

        Ok::<IcebergDataFiles, anyhow::Error>(IcebergDataFiles { data_files })
    },
    table: *mut crate::table::IcebergTable
);
