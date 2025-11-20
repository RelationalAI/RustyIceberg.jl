/// Common macros for scan builder methods shared between regular and incremental scans
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
///     iceberg_scan_with_data_file_concurrency_limit,
///     IcebergScan,
///     with_data_file_concurrency_limit,
///     n: usize
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
            }));

            CResult::Ok
        }
    };
}

/// Macro to generate with_batch_size function for any scan type
macro_rules! impl_with_batch_size {
    ($fn_name:ident, $scan_type:ident) => {
        #[no_mangle]
        pub extern "C" fn $fn_name(scan: &mut *mut $scan_type, n: usize) -> CResult {
            if scan.is_null() || (*scan).is_null() {
                return CResult::Error;
            }
            let scan_ref = unsafe { Box::from_raw(*scan) };

            if scan_ref.builder.is_none() {
                return CResult::Error;
            }

            assert!(scan_ref.scan.is_none());

            *scan = Box::into_raw(Box::new($scan_type {
                builder: scan_ref.builder.map(|b| b.with_batch_size(Some(n))),
                scan: None,
            }));

            CResult::Ok
        }
    };
}

/// Macro to generate build function for any scan type
macro_rules! impl_scan_build {
    ($fn_name:ident, $scan_type:ident) => {
        #[no_mangle]
        pub extern "C" fn $fn_name(scan: &mut *mut $scan_type) -> CResult {
            if scan.is_null() || (*scan).is_null() {
                return CResult::Error;
            }
            let scan_ref = unsafe { Box::from_raw(*scan) };
            if scan_ref.builder.is_none() {
                return CResult::Error;
            }
            let builder = scan_ref.builder.unwrap();

            match builder.build() {
                Ok(built_scan) => {
                    *scan = Box::into_raw(Box::new($scan_type {
                        builder: None,
                        scan: Some(built_scan),
                    }));
                    CResult::Ok
                }
                Err(_) => CResult::Error,
            }
        }
    };
}

/// Macro to generate scan_free function for any scan type
macro_rules! impl_scan_free {
    ($fn_name:ident, $scan_type:ident) => {
        #[no_mangle]
        pub extern "C" fn $fn_name(scan: &mut *mut $scan_type) {
            if !scan.is_null() {
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
pub(crate) use impl_with_batch_size;
