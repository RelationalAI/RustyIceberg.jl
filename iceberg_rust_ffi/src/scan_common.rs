/// Common macros for scan builder methods shared between regular and incremental scans

/// FFI mirror of RustyIceberg.jl's `IcebergPerfConfig`. This struct intentionally
/// has NO `Default` impl and NO default values — every field is supplied by Julia,
/// which is the single source of truth for tuning defaults. It is passed by value
/// into `iceberg_new_scan` / `iceberg_new_incremental_scan`; its layout must match
/// the Julia struct field-for-field (all `u64`, declared order). The
/// `iceberg_perf_config_abi_is_stable` test (size/align/offsets) and the
/// `iceberg_perf_config_roundtrip` FFI fn (driven by a Julia unit test) guard this.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IcebergPerfConfigFFI {
    pub batch_size: u64,
    pub manifest_file_concurrency_limit: u64,
    pub manifest_entry_concurrency_limit: u64,
    pub file_prefetch_depth: u64,
    pub serialization_concurrency_limit: u64,
}

/// Test-only: echo each field of an `IcebergPerfConfigFFI` back into `out[0..5]`
/// so a Julia unit test can assert the by-value struct layout agrees across the
/// FFI boundary (field order, padding, calling convention). `out` must point to
/// at least 5 `u64`s.
#[no_mangle]
pub extern "C" fn iceberg_perf_config_roundtrip(p: IcebergPerfConfigFFI, out: *mut u64) {
    if out.is_null() {
        return;
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out, 5) };
    out[0] = p.batch_size;
    out[1] = p.manifest_file_concurrency_limit;
    out[2] = p.manifest_entry_concurrency_limit;
    out[3] = p.file_prefetch_depth;
    out[4] = p.serialization_concurrency_limit;
}

#[cfg(test)]
mod perf_config_abi_tests {
    use super::IcebergPerfConfigFFI;
    use std::mem::{align_of, size_of};

    #[test]
    fn iceberg_perf_config_abi_is_stable() {
        assert_eq!(size_of::<IcebergPerfConfigFFI>(), 5 * 8); // 5 × u64
        assert_eq!(align_of::<IcebergPerfConfigFFI>(), 8);
        let p = IcebergPerfConfigFFI {
            batch_size: 0,
            manifest_file_concurrency_limit: 0,
            manifest_entry_concurrency_limit: 0,
            file_prefetch_depth: 0,
            serialization_concurrency_limit: 0,
        };
        let base = &p as *const _ as usize;
        assert_eq!((&p.batch_size as *const _ as usize) - base, 0);
        assert_eq!((&p.manifest_file_concurrency_limit as *const _ as usize) - base, 8);
        assert_eq!((&p.manifest_entry_concurrency_limit as *const _ as usize) - base, 16);
        assert_eq!((&p.file_prefetch_depth as *const _ as usize) - base, 24);
        assert_eq!((&p.serialization_concurrency_limit as *const _ as usize) - base, 32);
    }
}

/// Macro to generate select_columns function for any scan type
macro_rules! impl_select_columns {
    ($fn_name:ident, $scan_type:ident) => {
        #[no_mangle]
        pub extern "C" fn $fn_name(
            scan: &mut *mut $scan_type,
            column_names: *const *const c_char,
            num_columns: usize,
        ) -> CResult {
            if scan.is_null() || (*scan).is_null() || column_names.is_null() {
                return CResult::Error;
            }

            let mut columns = Vec::new();

            for i in 0..num_columns {
                let col_ptr = unsafe { *column_names.add(i) };
                if col_ptr.is_null() {
                    return CResult::Error;
                }

                let col_str = unsafe {
                    match CStr::from_ptr(col_ptr).to_str() {
                        Ok(s) => s,
                        Err(_) => return CResult::Error,
                    }
                };
                columns.push(col_str.to_string());
            }

            let scan_ref = unsafe { Box::from_raw(*scan) };

            if scan_ref.builder.is_none() {
                return CResult::Error;
            }
            *scan = Box::into_raw(Box::new($scan_type {
                builder: scan_ref.builder.map(|b| b.select(columns)),
                scan: scan_ref.scan,
                serialization_concurrency: scan_ref.serialization_concurrency,
                file_io: scan_ref.file_io,
                batch_size: scan_ref.batch_size,
                file_prefetch_depth: scan_ref.file_prefetch_depth,
            }));

            CResult::Ok
        }
    };
}

/// Macro to generate scan builder methods with zero or more parameters
///
/// # Examples
///
/// With single parameter:
/// ```ignore
/// impl_scan_builder_method!(
///     iceberg_scan_with_snapshot_id,
///     IcebergScan,
///     snapshot_id,
///     snapshot_id: i64
/// );
/// ```
///
/// Without parameters:
/// ```ignore
/// impl_scan_builder_method!(
///     iceberg_scan_with_file_column,
///     IcebergScan,
///     with_file_column
/// );
/// ```
macro_rules! impl_scan_builder_method {
    ($fn_name:ident, $scan_type:ident, $builder_method:ident $(, $param:ident: $param_type:ty)*) => {
        #[no_mangle]
        pub extern "C" fn $fn_name(scan: &mut *mut $scan_type $(, $param: $param_type)*) -> CResult {
            if scan.is_null() || (*scan).is_null() {
                return CResult::Error;
            }
            let scan_ref = unsafe { Box::from_raw(*scan) };

            if scan_ref.builder.is_none() {
                return CResult::Error;
            }

            *scan = Box::into_raw(Box::new($scan_type {
                builder: scan_ref.builder.map(|b| b.$builder_method($($param),*)),
                scan: scan_ref.scan,
                serialization_concurrency: scan_ref.serialization_concurrency,
                file_io: scan_ref.file_io,
                batch_size: scan_ref.batch_size,
                file_prefetch_depth: scan_ref.file_prefetch_depth,
            }));

            CResult::Ok
        }
    };
}

/// Macro to generate build function for any scan type
macro_rules! impl_scan_build {
    ($fn_name:ident, $scan_type:ident) => {
        #[no_mangle]
        pub extern "C" fn $fn_name(
            scan: &mut *mut $scan_type,
            error_out: *mut *mut std::os::raw::c_char,
        ) -> CResult {
            if scan.is_null() || (*scan).is_null() {
                return CResult::Error;
            }
            // Take ownership and immediately null out *scan.  Any early return
            // from this point on leaves *scan == null, which is safe to pass to
            // the free function (impl_scan_free! guards against null inner ptrs).
            let scan_ref = unsafe { Box::from_raw(*scan) };
            *scan = std::ptr::null_mut();

            if scan_ref.builder.is_none() {
                return CResult::Error;
            }
            let builder = scan_ref.builder.unwrap();

            match builder.build() {
                Ok(built_scan) => {
                    *scan = Box::into_raw(Box::new($scan_type {
                        builder: None,
                        scan: Some(built_scan),
                        serialization_concurrency: scan_ref.serialization_concurrency,
                        file_io: scan_ref.file_io,
                        batch_size: scan_ref.batch_size,
                        file_prefetch_depth: scan_ref.file_prefetch_depth,
                    }));
                    CResult::Ok
                }
                Err(e) => {
                    // Forward the classified error string to Julia via error_out.
                    // The caller is responsible for freeing it with iceberg_destroy_cstring.
                    if !error_out.is_null() {
                        let classified = crate::error_codes::classify_iceberg(e);
                        let msg = format!("{classified}");
                        unsafe {
                            *error_out = std::ffi::CString::new(msg).unwrap_or_default().into_raw();
                        }
                    }
                    CResult::Error
                }
            }
        }
    };
}

/// Macro to generate scan_free function for any scan type.
///
/// Guards against a null inner pointer (`*scan == null`) which can arise when
/// `impl_scan_build!` returns an error — in that case the macro nulls `*scan`
/// to prevent double-free but the caller still calls free for symmetry.
macro_rules! impl_scan_free {
    ($fn_name:ident, $scan_type:ident) => {
        #[no_mangle]
        pub extern "C" fn $fn_name(scan: &mut *mut $scan_type) {
            // Check the *inner* pointer (*scan), not the outer reference (scan).
            // The outer reference from Julia is never null; the inner pointer is
            // null when build! failed and already freed the scan.
            if !(*scan).is_null() {
                unsafe {
                    let _ = Box::from_raw(*scan);
                    *scan = std::ptr::null_mut();
                }
            }
        }
    };
}

// Re-export macros for use in other modules
pub(crate) use impl_scan_build;
pub(crate) use impl_scan_builder_method;
pub(crate) use impl_scan_free;
pub(crate) use impl_select_columns;
