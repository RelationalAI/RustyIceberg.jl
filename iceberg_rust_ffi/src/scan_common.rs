/// Common macros for scan builder methods shared between regular and incremental scans

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

// Re-export macro for use in other modules
pub(crate) use impl_scan_free;
