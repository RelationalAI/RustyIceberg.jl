/// Common macros for scan builder methods shared between regular and incremental scans

/// Macro to generate select_columns function for any scan type.
/// Uses in-place mutation so it works regardless of how many fields the scan struct has.
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

            let scan_ref = unsafe { &mut **scan };
            let builder = scan_ref.builder.take();
            if builder.is_none() {
                return CResult::Error;
            }
            scan_ref.builder = builder.map(|b| b.select(columns));
            CResult::Ok
        }
    };
}

/// Macro to generate scan builder methods with zero or more parameters.
/// Uses in-place mutation so it works with any struct that has `builder` and `scan` fields.
macro_rules! impl_scan_builder_method {
    ($fn_name:ident, $scan_type:ident, $builder_method:ident $(, $param:ident: $param_type:ty)*) => {
        #[no_mangle]
        pub extern "C" fn $fn_name(scan: &mut *mut $scan_type $(, $param: $param_type)*) -> CResult {
            if scan.is_null() || (*scan).is_null() {
                return CResult::Error;
            }
            let scan_ref = unsafe { &mut **scan };
            let builder = scan_ref.builder.take();
            if builder.is_none() {
                return CResult::Error;
            }
            scan_ref.builder = builder.map(|b| b.$builder_method($($param),*));
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
            let scan_ref = unsafe { &mut **scan };
            let builder = scan_ref.builder.take();
            if builder.is_none() {
                return CResult::Error;
            }
            match builder.unwrap().build() {
                Ok(built_scan) => {
                    scan_ref.scan = Some(built_scan);
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

/// Macro to generate with_serialization_concurrency_limit function for any scan type
macro_rules! impl_with_serialization_concurrency_limit {
    ($fn_name:ident, $scan_type:ident) => {
        #[no_mangle]
        pub extern "C" fn $fn_name(scan: &mut *mut $scan_type, n: usize) -> CResult {
            if scan.is_null() || (*scan).is_null() {
                return CResult::Error;
            }
            let scan_ref = unsafe { &mut **scan };
            scan_ref.serialization_concurrency = n;
            CResult::Ok
        }
    };
}

// Re-export macros for use in other modules
pub(crate) use impl_scan_build;
pub(crate) use impl_scan_builder_method;
pub(crate) use impl_scan_free;
pub(crate) use impl_select_columns;
pub(crate) use impl_with_serialization_concurrency_limit;
