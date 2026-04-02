/// Generic response types for FFI operations
use crate::{CResult, Context, RawResponse};
use std::ffi::c_char;
use std::ptr;

/// Generic response type for scalar property operations
/// Can be used for bool, i64, and other simple types
#[repr(C)]
pub struct IcebergPropertyResponse<T> {
    pub result: CResult,
    pub value: T,
    pub error_message: *mut c_char,
    pub context: *const Context,
}

unsafe impl<T: Send> Send for IcebergPropertyResponse<T> {}

impl<T: Default> RawResponse for IcebergPropertyResponse<T> {
    type Payload = T;
    fn result_mut(&mut self) -> &mut CResult {
        &mut self.result
    }
    fn context_mut(&mut self) -> &mut *const Context {
        &mut self.context
    }
    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.error_message
    }
    fn set_payload(&mut self, payload: Option<Self::Payload>) {
        if let Some(val) = payload {
            self.value = val;
        }
    }
}

/// Generic response type for boxed/pointer payloads
/// The value field is a raw pointer, and the payload gets boxed before storing
#[repr(C)]
pub struct IcebergBoxedResponse<T> {
    pub result: CResult,
    pub value: *mut T,
    pub error_message: *mut c_char,
    pub context: *const Context,
}

unsafe impl<T: Send> Send for IcebergBoxedResponse<T> {}

impl<T> RawResponse for IcebergBoxedResponse<T> {
    type Payload = T;
    fn result_mut(&mut self) -> &mut CResult {
        &mut self.result
    }
    fn context_mut(&mut self) -> &mut *const Context {
        &mut self.context
    }
    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.error_message
    }
    fn set_payload(&mut self, payload: Option<Self::Payload>) {
        match payload {
            Some(val) => {
                self.value = Box::into_raw(Box::new(val));
            }
            None => self.value = ptr::null_mut(),
        }
    }
}

/// Response type for string list operations
#[repr(C)]
pub struct IcebergStringListResponse {
    pub result: CResult,
    pub items: *mut *mut c_char,
    pub count: usize,
    pub error_message: *mut c_char,
    pub context: *const Context,
}

unsafe impl Send for IcebergStringListResponse {}

impl RawResponse for IcebergStringListResponse {
    type Payload = Vec<String>;

    fn result_mut(&mut self) -> &mut CResult {
        &mut self.result
    }

    fn context_mut(&mut self) -> &mut *const Context {
        &mut self.context
    }

    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.error_message
    }

    fn set_payload(&mut self, payload: Option<Self::Payload>) {
        match payload {
            Some(items) => {
                let strings: Vec<*mut c_char> = items
                    .into_iter()
                    .map(|s| {
                        let c_string = std::ffi::CString::new(s).unwrap_or_default();
                        c_string.into_raw()
                    })
                    .collect();

                self.count = strings.len();
                let boxed_strings = Box::new(strings);
                self.items = Box::into_raw(boxed_strings) as *mut *mut c_char;
            }
            None => {
                self.items = ptr::null_mut();
                self.count = 0;
            }
        }
    }
}

/// Response type for file row count operations (parallel arrays of paths and counts)
#[repr(C)]
pub struct IcebergFileRowCountResponse {
    pub result: CResult,
    pub paths: *mut *mut c_char,
    pub counts: *mut i64,
    pub count: usize,
    pub error_message: *mut c_char,
    pub context: *const Context,
}

unsafe impl Send for IcebergFileRowCountResponse {}

impl RawResponse for IcebergFileRowCountResponse {
    type Payload = Vec<(String, i64)>;

    fn result_mut(&mut self) -> &mut CResult {
        &mut self.result
    }

    fn context_mut(&mut self) -> &mut *const Context {
        &mut self.context
    }

    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.error_message
    }

    fn set_payload(&mut self, payload: Option<Self::Payload>) {
        match payload {
            Some(items) => {
                let mut paths: Vec<*mut c_char> = Vec::with_capacity(items.len());
                let mut counts: Vec<i64> = Vec::with_capacity(items.len());
                for (path, count) in items {
                    let c_string = std::ffi::CString::new(path).unwrap_or_default();
                    paths.push(c_string.into_raw());
                    counts.push(count);
                }
                self.count = paths.len();
                // Use boxed slices so the pointer points directly to the array data,
                // not to a Vec struct. This allows Julia to index the arrays directly.
                let mut paths_slice = paths.into_boxed_slice();
                let mut counts_slice = counts.into_boxed_slice();
                self.paths = paths_slice.as_mut_ptr();
                self.counts = counts_slice.as_mut_ptr();
                std::mem::forget(paths_slice);
                std::mem::forget(counts_slice);
            }
            None => {
                self.paths = ptr::null_mut();
                self.counts = ptr::null_mut();
                self.count = 0;
            }
        }
    }
}

/// Response type for nested string list operations (for namespace lists)
#[repr(C)]
pub struct IcebergNestedStringListResponse {
    pub result: CResult,
    pub outer_items: *mut *mut *mut c_char,
    pub outer_count: usize,
    pub inner_counts: *mut usize,
    pub error_message: *mut c_char,
    pub context: *const Context,
}

unsafe impl Send for IcebergNestedStringListResponse {}

impl RawResponse for IcebergNestedStringListResponse {
    type Payload = Vec<Vec<String>>;

    fn result_mut(&mut self) -> &mut CResult {
        &mut self.result
    }

    fn context_mut(&mut self) -> &mut *const Context {
        &mut self.context
    }

    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.error_message
    }

    fn set_payload(&mut self, payload: Option<Self::Payload>) {
        match payload {
            Some(namespace_lists) => {
                let mut outer_items_vec: Vec<*mut *mut c_char> = Vec::new();
                let mut inner_counts: Vec<usize> = Vec::new();

                for namespace in namespace_lists {
                    let strings: Vec<*mut c_char> = namespace
                        .into_iter()
                        .map(|s| {
                            let c_string = std::ffi::CString::new(s).unwrap_or_default();
                            c_string.into_raw()
                        })
                        .collect();

                    let count = strings.len();
                    inner_counts.push(count);

                    // Box the string vector and cast the box pointer
                    // This creates a *mut *mut c_char that Julia can use
                    let boxed_strings = Box::new(strings);
                    let strings_ptr = Box::into_raw(boxed_strings) as *mut *mut c_char;
                    outer_items_vec.push(strings_ptr);
                }

                self.outer_count = outer_items_vec.len();
                let boxed_outer = Box::new(outer_items_vec);
                // Same pattern as StringListResponse
                self.outer_items = Box::into_raw(boxed_outer) as *mut *mut *mut c_char;

                let boxed_counts = Box::new(inner_counts);
                self.inner_counts = Box::into_raw(boxed_counts) as *mut usize;
            }
            None => {
                self.outer_items = ptr::null_mut();
                self.inner_counts = ptr::null_mut();
                self.outer_count = 0;
            }
        }
    }
}
