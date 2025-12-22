/// Catalog support for iceberg_rust_ffi
use crate::IcebergTable;
use anyhow::Result;
use async_trait::async_trait;
use iceberg::{Catalog, CatalogBuilder, Error, ErrorKind, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{RestCatalogBuilder, TokenAuthenticator};
use std::collections::HashMap;
use std::ffi::{c_char, c_void, CString};
use std::sync::Arc;

// FFI exports
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, NotifyGuard, ResponseGuard, RT,
};

// Utility imports
use crate::util::{parse_c_string, parse_properties, parse_string_array};
use crate::PropertyEntry;

/// Callback function type for token authentication from FFI
///
/// The callback receives:
/// - user_data: opaque pointer to user context (e.g., Julia closure)
/// - token_ptr: output pointer where the token string should be written
///
/// Returns:
/// - 0 for success (token_ptr must point to a CString allocated with CString::into_raw)
/// - non-zero for error
pub type TokenAuthenticatorCallback =
    extern "C" fn(user_data: *mut c_void, token_ptr: *mut *mut c_char) -> i32;

/// Rust implementation of TokenAuthenticator that calls a C callback with user_data
#[derive(Debug, Clone)]
struct FFITokenAuthenticator {
    callback: TokenAuthenticatorCallback,
    user_data: *mut c_void,
}

// SAFETY: We trust that the Julia callback is thread-safe.
// The user_data pointer is opaque and its thread-safety is the caller's responsibility.
unsafe impl Send for FFITokenAuthenticator {}
unsafe impl Sync for FFITokenAuthenticator {}

#[async_trait]
impl TokenAuthenticator for FFITokenAuthenticator {
    async fn get_token(&self) -> iceberg::Result<String> {
        let mut token_ptr: *mut c_char = std::ptr::null_mut();

        let result = (self.callback)(self.user_data, &mut token_ptr);

        if result != 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Token authenticator callback failed",
            ));
        }

        if token_ptr.is_null() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Token authenticator returned null pointer",
            ));
        }

        // SAFETY: The callback is responsible for ensuring token_ptr is a valid
        // null-terminated C string that was allocated with CString::into_raw
        let token_cstring = unsafe { CString::from_raw(token_ptr) };

        token_cstring.into_string().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid UTF-8 in token: {}", e),
            )
        })
    }
}

/// Opaque catalog handle for FFI
/// Holds a raw pointer to a RestCatalog allocated on the heap.
/// The catalog is owned by this struct and cleaned up when dropped.
#[repr(C)]
pub struct IcebergCatalog {
    catalog: *mut iceberg_catalog_rest::RestCatalog,
}

impl Drop for IcebergCatalog {
    fn drop(&mut self) {
        unsafe {
            if !self.catalog.is_null() {
                let _ = Box::from_raw(self.catalog);
            }
        }
    }
}

// SAFETY: The catalog pointer is owned exclusively by this struct.
// Send and Sync are safe because:
// 1. The RestCatalog is accessed only through this struct
// 2. We enforce exclusive mutable access for operations that mutate (set_token_authenticator)
// 3. The pointer is never shared or aliased from FFI
unsafe impl Send for IcebergCatalog {}
unsafe impl Sync for IcebergCatalog {}

impl IcebergCatalog {
    /// Create a new REST catalog
    pub async fn create_rest(uri: String, props: HashMap<String, String>) -> Result<Self> {
        let mut catalog_props = props;
        catalog_props.insert("uri".to_string(), uri);

        let catalog = RestCatalogBuilder::default()
            .load("rest", catalog_props)
            .await?;

        Ok(IcebergCatalog {
            catalog: Box::into_raw(Box::new(catalog)),
        })
    }

    /// Set a custom token authenticator
    /// Must be called before the first catalog operation
    pub fn set_token_authenticator(
        &mut self,
        callback: TokenAuthenticatorCallback,
        user_data: *mut c_void,
    ) -> Result<()> {
        if self.catalog.is_null() {
            return Err(anyhow::anyhow!("Catalog is null"));
        }

        let authenticator = Arc::new(FFITokenAuthenticator {
            callback,
            user_data,
        });

        // SAFETY: We own the catalog through the raw pointer. This is safe because:
        // 1. with_token_authenticator is synchronous
        // 2. It only updates internal state and returns self
        // 3. We have exclusive mutable access through &mut self
        // 4. We use ptr::read/write to move the value without Clone
        unsafe {
            let catalog = std::ptr::read(self.catalog);
            let updated = catalog.with_token_authenticator(authenticator);
            std::ptr::write(self.catalog, updated);
        }

        Ok(())
    }

    /// Load a table by namespace and name
    pub async fn load_table(
        &self,
        namespace_parts: Vec<String>,
        table_name: String,
    ) -> Result<IcebergTable> {
        let namespace = NamespaceIdent::from_vec(namespace_parts)?;
        let table_ident = TableIdent::new(namespace, table_name);
        // SAFETY: catalog is valid as long as self is valid
        let table = unsafe { (*self.catalog).load_table(&table_ident).await? };

        Ok(IcebergTable { table })
    }

    /// List tables in a namespace
    pub async fn list_tables(&self, namespace_parts: Vec<String>) -> Result<Vec<String>> {
        let namespace = NamespaceIdent::from_vec(namespace_parts)?;
        // SAFETY: catalog is valid as long as self is valid
        let tables = unsafe { (*self.catalog).list_tables(&namespace).await? };

        Ok(tables.into_iter().map(|t| t.name().to_string()).collect())
    }

    /// List namespaces
    pub async fn list_namespaces(
        &self,
        parent_parts: Option<Vec<String>>,
    ) -> Result<Vec<Vec<String>>> {
        let parent = if let Some(parts) = parent_parts {
            Some(NamespaceIdent::from_vec(parts)?)
        } else {
            None
        };

        // SAFETY: catalog is valid as long as self is valid
        let namespaces = unsafe {
            (*self.catalog)
                .list_namespaces(parent.as_ref())
                .await?
        };

        Ok(namespaces
            .into_iter()
            .map(|ns| ns.inner().to_vec())
            .collect())
    }

    /// Check if a table exists
    pub async fn table_exists(
        &self,
        namespace_parts: Vec<String>,
        table_name: String,
    ) -> Result<bool> {
        let namespace = NamespaceIdent::from_vec(namespace_parts)?;
        let table_ident = TableIdent::new(namespace, table_name);
        // SAFETY: catalog is valid as long as self is valid
        unsafe { (*self.catalog).table_exists(&table_ident).await }
            .map_err(|e| anyhow::anyhow!(e))
    }
}

/// Response type for catalog operations that return a catalog
#[repr(C)]
pub struct IcebergCatalogResponse {
    pub result: crate::CResult,
    pub catalog: *mut IcebergCatalog,
    pub error_message: *mut c_char,
    pub context: *const crate::Context,
}

unsafe impl Send for IcebergCatalogResponse {}

impl crate::RawResponse for IcebergCatalogResponse {
    type Payload = IcebergCatalog;

    fn result_mut(&mut self) -> &mut crate::CResult {
        &mut self.result
    }

    fn context_mut(&mut self) -> &mut *const crate::Context {
        &mut self.context
    }

    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.error_message
    }

    fn set_payload(&mut self, payload: Option<Self::Payload>) {
        match payload {
            Some(catalog) => {
                let catalog_ptr = Box::into_raw(Box::new(catalog));
                self.catalog = catalog_ptr;
            }
            None => self.catalog = std::ptr::null_mut(),
        }
    }
}

/// Response type for string list operations
#[repr(C)]
pub struct IcebergStringListResponse {
    pub result: crate::CResult,
    pub items: *mut *mut c_char,
    pub count: usize,
    pub error_message: *mut c_char,
    pub context: *const crate::Context,
}

unsafe impl Send for IcebergStringListResponse {}

impl crate::RawResponse for IcebergStringListResponse {
    type Payload = Vec<String>;

    fn result_mut(&mut self) -> &mut crate::CResult {
        &mut self.result
    }

    fn context_mut(&mut self) -> &mut *const crate::Context {
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
                self.items = std::ptr::null_mut();
                self.count = 0;
            }
        }
    }
}

/// Response type for boolean operations
#[repr(C)]
pub struct IcebergBoolResponse {
    pub result: crate::CResult,
    pub value: bool,
    pub error_message: *mut c_char,
    pub context: *const crate::Context,
}

unsafe impl Send for IcebergBoolResponse {}

impl crate::RawResponse for IcebergBoolResponse {
    type Payload = bool;

    fn result_mut(&mut self) -> &mut crate::CResult {
        &mut self.result
    }

    fn context_mut(&mut self) -> &mut *const crate::Context {
        &mut self.context
    }

    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.error_message
    }

    fn set_payload(&mut self, payload: Option<Self::Payload>) {
        match payload {
            Some(value) => self.value = value,
            None => self.value = false,
        }
    }
}

/// Response type for nested string list operations (for namespace lists)
#[repr(C)]
pub struct IcebergNestedStringListResponse {
    pub result: crate::CResult,
    pub outer_items: *mut *mut *mut c_char,
    pub outer_count: usize,
    pub inner_counts: *mut usize,
    pub error_message: *mut c_char,
    pub context: *const crate::Context,
}

unsafe impl Send for IcebergNestedStringListResponse {}

impl crate::RawResponse for IcebergNestedStringListResponse {
    type Payload = Vec<Vec<String>>;

    fn result_mut(&mut self) -> &mut crate::CResult {
        &mut self.result
    }

    fn context_mut(&mut self) -> &mut *const crate::Context {
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
                self.outer_items = std::ptr::null_mut();
                self.inner_counts = std::ptr::null_mut();
                self.outer_count = 0;
            }
        }
    }
}

/// Free a catalog
#[no_mangle]
pub extern "C" fn iceberg_catalog_free(catalog: *mut IcebergCatalog) {
    if !catalog.is_null() {
        unsafe {
            let _ = Box::from_raw(catalog);
        }
    }
}

// FFI Export functions for catalog operations
// These functions are exported to be called from Julia via the FFI

// Create a REST catalog
export_runtime_op!(
    iceberg_rest_catalog_create,
    IcebergCatalogResponse,
    || {
        let uri_str = parse_c_string(uri, "uri")?;
        let props = parse_properties(properties, properties_len)?;
        Ok((uri_str, props))
    },
    result_tuple,
    async {
        let (uri, props) = result_tuple;
        IcebergCatalog::create_rest(uri, props).await
    },
    uri: *const c_char,
    properties: *const PropertyEntry,
    properties_len: usize
);

// Load a table from the catalog
export_runtime_op!(
    iceberg_catalog_load_table,
    crate::IcebergTableResponse,
    || {
        if catalog.is_null() {
            return Err(anyhow::anyhow!("Null catalog pointer provided"));
        }

        let namespace_parts = parse_string_array(namespace_parts_ptr, namespace_parts_len)?;
        let table_name = parse_c_string(table_name, "table_name")?;
        let catalog_ref = unsafe { &*catalog };

        Ok((catalog_ref, namespace_parts, table_name))
    },
    result_tuple,
    async {
        let (catalog_ref, namespace_parts, table_name) = result_tuple;
        catalog_ref.load_table(namespace_parts, table_name).await
    },
    catalog: *mut IcebergCatalog,
    namespace_parts_ptr: *const *const c_char,
    namespace_parts_len: usize,
    table_name: *const c_char
);

// List tables in a namespace
export_runtime_op!(
    iceberg_catalog_list_tables,
    IcebergStringListResponse,
    || {
        if catalog.is_null() {
            return Err(anyhow::anyhow!("Null catalog pointer provided"));
        }

        let namespace_parts = parse_string_array(namespace_parts_ptr, namespace_parts_len)?;
        let catalog_ref = unsafe { &*catalog };

        Ok((catalog_ref, namespace_parts))
    },
    result_tuple,
    async {
        let (catalog_ref, namespace_parts) = result_tuple;
        catalog_ref.list_tables(namespace_parts).await
    },
    catalog: *mut IcebergCatalog,
    namespace_parts_ptr: *const *const c_char,
    namespace_parts_len: usize
);

// List namespaces
export_runtime_op!(
    iceberg_catalog_list_namespaces,
    IcebergNestedStringListResponse,
    || {
        if catalog.is_null() {
            return Err(anyhow::anyhow!("Null catalog pointer provided"));
        }

        let parent_parts = if namespace_parts_len > 0 {
            Some(parse_string_array(namespace_parts_ptr, namespace_parts_len)?)
        } else {
            None
        };
        let catalog_ref = unsafe { &*catalog };

        Ok((catalog_ref, parent_parts))
    },
    result_tuple,
    async {
        let (catalog_ref, parent_parts) = result_tuple;
        catalog_ref.list_namespaces(parent_parts).await
    },
    catalog: *mut IcebergCatalog,
    namespace_parts_ptr: *const *const c_char,
    namespace_parts_len: usize
);

// Check if a table exists
export_runtime_op!(
    iceberg_catalog_table_exists,
    IcebergBoolResponse,
    || {
        if catalog.is_null() {
            return Err(anyhow::anyhow!("Null catalog pointer provided"));
        }

        let namespace_parts = parse_string_array(namespace_parts_ptr, namespace_parts_len)?;
        let table_name = parse_c_string(table_name, "table_name")?;
        let catalog_ref = unsafe { &*catalog };

        Ok((catalog_ref, namespace_parts, table_name))
    },
    result_tuple,
    async {
        let (catalog_ref, namespace_parts, table_name) = result_tuple;
        catalog_ref.table_exists(namespace_parts, table_name).await
    },
    catalog: *mut IcebergCatalog,
    namespace_parts_ptr: *const *const c_char,
    namespace_parts_len: usize,
    table_name: *const c_char
);

/// Set a custom token authenticator for the catalog
#[no_mangle]
pub extern "C" fn iceberg_catalog_set_token_authenticator(
    catalog: *mut IcebergCatalog,
    callback: TokenAuthenticatorCallback,
    user_data: *mut c_void,
) -> crate::CResult {
    // Check for null catalog pointer
    if catalog.is_null() {
        return crate::CResult::Error;
    }

    // SAFETY: catalog was checked to be non-null above.
    // The caller must ensure the catalog pointer remains valid for the duration of this call.
    let catalog_ref = unsafe { &mut *catalog };

    // Call the synchronous set_token_authenticator method
    match catalog_ref.set_token_authenticator(callback, user_data) {
        Ok(()) => crate::CResult::Ok,
        Err(e) => {
            eprintln!("Error setting token authenticator: {}", e);
            crate::CResult::Error
        }
    }
}
