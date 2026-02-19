use crate::response::{
    IcebergBoxedResponse, IcebergNestedStringListResponse, IcebergPropertyResponse,
    IcebergStringListResponse,
};
/// Catalog support for iceberg_rust_ffi
use crate::IcebergTable;
use anyhow::Result;
use async_trait::async_trait;
use iceberg::io::{StorageCredential, StorageCredentialsLoader};
use iceberg::{Catalog, CatalogBuilder, Error, ErrorKind, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{CustomAuthenticator, RestCatalog, RestCatalogBuilder};
use std::collections::HashMap;
use std::ffi::{c_char, c_void};
use std::sync::{Arc, Mutex, OnceLock, Weak};

// FFI exports
use object_store_ffi::{
    export_runtime_op, with_cancellation, CResult, NotifyGuard, ResponseGuard, RT,
};

// Utility imports
use crate::util::{parse_c_string, parse_properties, parse_string_array};
use crate::PropertyEntry;

/// Callback function type for custom token authentication from FFI with token caching support
///
/// The callback receives:
/// - auth_fn: opaque pointer to user context (e.g., Julia Ref to authenticator)
/// - token_data_ptr: output pointer where token string bytes pointer should be written
/// - token_len_ptr: output pointer where token string length should be written
/// - reuse_token_ptr: output pointer for reuse flag (1 = reuse previous token, 0 = new token)
///
/// The callback is responsible for:
/// 1. Extracting the authenticator from auth_fn pointer
/// 2. Invoking the Julia authenticator function which returns Union{String, Nothing}:
///    - String: new token to cache and use
///    - nothing: signal to reuse the previously cached token
/// 3. Setting reuse_token_ptr to indicate the result:
///    - Write 1 if Julia returned nothing (reuse the previous token)
///    - Write 0 if Julia returned a String (new token data provided)
/// 4. If reuse flag is 0: write token_data_ptr (pointer to bytes) and token_len_ptr (length)
///    Note: Julia is responsible for keeping the data valid for the duration of the call.
///    Rust will copy the data immediately.
///
/// Returns:
/// - 0 for success
/// - non-zero for error
pub type CustomAuthenticatorCallback = unsafe extern "C" fn(
    auth_fn: *mut c_void,
    token_data_ptr: *mut *mut c_char,
    token_len_ptr: *mut usize,
    reuse_token_ptr: *mut i32,
) -> i32;

/// Rust implementation of CustomAuthenticator that calls a C callback with auth_fn pointer
/// Supports token caching to avoid unnecessary copying when Julia returns the same token
#[derive(Clone, Debug)]
struct FFITokenAuthenticator {
    callback: CustomAuthenticatorCallback,
    auth_fn: *mut c_void,
    // Cached token to avoid copying when Julia signals to reuse
    cached_token: Arc<Mutex<Option<String>>>,
}

// SAFETY: We trust that the Julia callback is thread-safe.
// The auth_fn pointer is opaque and its thread-safety is the caller's responsibility.
unsafe impl Send for FFITokenAuthenticator {}
unsafe impl Sync for FFITokenAuthenticator {}

#[async_trait]
impl CustomAuthenticator for FFITokenAuthenticator {
    async fn get_token(&self) -> iceberg::Result<String> {
        let mut token_data: *mut c_char = std::ptr::null_mut();
        let mut token_len: usize = 0;
        let mut reuse_token_box = Box::new(0i32);
        let reuse_token_ptr = reuse_token_box.as_mut() as *mut i32;

        let result = unsafe {
            (self.callback)(
                self.auth_fn,
                &mut token_data,
                &mut token_len,
                reuse_token_ptr,
            )
        };

        if result != 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Token authenticator callback failed",
            ));
        }

        // If Julia signals to reuse the previous token, return it
        if *reuse_token_box != 0 {
            let cached = self.cached_token.lock().unwrap();
            if let Some(token) = cached.as_ref() {
                return Ok(token.clone());
            } else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Token authenticator requested to reuse previous token, but no cached token exists",
                ));
            }
        }

        // Otherwise, handle the new token from Julia
        if token_data.is_null() || token_len == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Token authenticator returned null pointer or zero length",
            ));
        }

        // SAFETY: The callback is responsible for ensuring token_data points to valid
        // UTF-8 bytes with length token_len. Rust will copy the data immediately.
        let token = unsafe {
            let slice = std::slice::from_raw_parts(token_data as *const u8, token_len);
            String::from_utf8(slice.to_vec()).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid UTF-8 in token: {}", e),
                )
            })?
        };

        // Cache the new token for potential reuse
        *self.cached_token.lock().unwrap() = Some(token.clone());

        Ok(token)
    }
}

/// Credential loader that calls the REST catalog's load_table_credentials endpoint
/// to obtain storage credentials for table data access.
///
/// Uses a `Weak` reference to break the circular dependency:
/// `Arc<RestCatalog>` → (owns) `Arc<RestCredentialsLoader>` → (weak) `RestCatalog`
#[derive(Debug)]
struct RestCredentialsLoader {
    catalog: OnceLock<Weak<RestCatalog>>,
}

#[async_trait]
impl StorageCredentialsLoader for RestCredentialsLoader {
    async fn load_credentials(
        &self,
        table_ident: &TableIdent,
        location: &str,
    ) -> iceberg::Result<StorageCredential> {
        let catalog = self
            .catalog
            .get()
            .and_then(|w| w.upgrade())
            .ok_or_else(|| {
                Error::new(ErrorKind::Unexpected, "Catalog reference is not available")
            })?;
        let response = catalog.load_table_credentials(table_ident).await?;
        response
            .storage_credentials
            .into_iter()
            .filter(|c| location.starts_with(&c.prefix))
            .max_by_key(|c| c.prefix.len())
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "No matching credential for location"))
    }
}

/// Opaque catalog handle for FFI
/// Stores a RestCatalog instance wrapped in an Arc for safe memory management.
/// Also stores the authenticator to allow setting it before catalog creation.
pub struct IcebergCatalog {
    catalog: Option<Arc<RestCatalog>>,
    /// Stores a pending authenticator to be applied before first use
    authenticator: Option<Arc<FFITokenAuthenticator>>,
    /// Whether to attach a storage credentials loader after catalog creation
    use_credentials_loader: bool,
}

// SAFETY: Send and Sync are safe because:
// 1. The RestCatalog is accessed only through this struct via exclusive ownership (Box)
// 2. We enforce exclusive mutable access for operations that mutate (set_token_authenticator)
// 3. The struct represents unshared ownership across FFI boundary
// 4. The struct is manually freed via iceberg_catalog_free(), not via Drop
unsafe impl Send for IcebergCatalog {}
unsafe impl Sync for IcebergCatalog {}

impl Default for IcebergCatalog {
    fn default() -> Self {
        IcebergCatalog {
            catalog: None,
            authenticator: None,
            use_credentials_loader: false,
        }
    }
}

impl IcebergCatalog {
    /// Create and initialize a REST catalog with optional authenticator
    pub async fn create_rest(
        mut self,
        uri: String,
        props: HashMap<String, String>,
    ) -> Result<Self> {
        let mut catalog_props = props;
        catalog_props.insert("uri".to_string(), uri);

        // Apply authenticator to builder if set
        let mut builder = RestCatalogBuilder::default();
        if let Some(ref authenticator) = self.authenticator {
            builder = builder.with_token_authenticator(authenticator.clone());
        }

        let mut catalog = builder.load("rest", catalog_props).await?;

        if self.use_credentials_loader {
            // Create loader with empty catalog reference
            let loader = Arc::new(RestCredentialsLoader {
                catalog: OnceLock::new(),
            });
            let loader_ref = Arc::clone(&loader);

            // Attach loader to catalog while we still have &mut access
            catalog.set_storage_credentials_loader(loader);

            // Wrap catalog in Arc
            let catalog_arc = Arc::new(catalog);

            // Fill the loader's weak reference to the catalog
            let _ = loader_ref.catalog.set(Arc::downgrade(&catalog_arc));

            self.catalog = Some(catalog_arc);
        } else {
            self.catalog = Some(Arc::new(catalog));
        }

        Ok(self)
    }

    /// Set a custom token authenticator before catalog creation
    pub fn set_token_authenticator(
        &mut self,
        callback: CustomAuthenticatorCallback,
        auth_fn: *mut c_void,
    ) -> Result<()> {
        let authenticator = Arc::new(FFITokenAuthenticator {
            callback,
            auth_fn,
            cached_token: Arc::new(Mutex::new(None)),
        });

        // Store the authenticator to be used when building the catalog
        self.authenticator = Some(authenticator);

        Ok(())
    }

    /// Get a reference to the underlying RestCatalog.
    ///
    /// Returns a reference to the catalog.
    /// Panics if the catalog has not been initialized via create_rest.
    fn as_ref(&self) -> &RestCatalog {
        self.catalog
            .as_ref()
            .expect("catalog should be initialized")
    }

    /// Get a reference to the underlying RestCatalog for transaction commit.
    ///
    /// Returns Some(&RestCatalog) if initialized, None otherwise.
    pub fn get_catalog(&self) -> Option<&RestCatalog> {
        self.catalog.as_deref()
    }

    /// Load a table by namespace and name
    pub async fn load_table(
        &self,
        namespace_parts: Vec<String>,
        table_name: String,
    ) -> Result<IcebergTable> {
        let namespace = NamespaceIdent::from_vec(namespace_parts)?;
        let table_ident = TableIdent::new(namespace, table_name);
        let table = self.as_ref().load_table(&table_ident).await?;

        Ok(IcebergTable { table })
    }

    /// Load a table by namespace and name with vended credentials
    pub async fn load_table_with_credentials(
        &self,
        namespace_parts: Vec<String>,
        table_name: String,
    ) -> Result<IcebergTable> {
        let namespace = NamespaceIdent::from_vec(namespace_parts)?;
        let table_ident = TableIdent::new(namespace, table_name);
        let table = self
            .as_ref()
            .load_table_with_credentials(&table_ident)
            .await?;

        Ok(IcebergTable { table })
    }

    /// List tables in a namespace
    pub async fn list_tables(&self, namespace_parts: Vec<String>) -> Result<Vec<String>> {
        let namespace = NamespaceIdent::from_vec(namespace_parts)?;
        let tables = self.as_ref().list_tables(&namespace).await?;

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

        let namespaces = self.as_ref().list_namespaces(parent.as_ref()).await?;

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
        self.as_ref()
            .table_exists(&table_ident)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    /// Internal method to create a table with optional credential vending
    /// Create a new table in the catalog with optional credential vending
    pub async fn create_table(
        &self,
        namespace_parts: Vec<String>,
        table_name: String,
        schema: iceberg::spec::Schema,
        partition_spec: Option<iceberg::spec::PartitionSpec>,
        sort_order: Option<iceberg::spec::SortOrder>,
        properties: HashMap<String, String>,
        load_credentials: bool,
    ) -> Result<IcebergTable> {
        // Convert namespace parts to NamespaceIdent
        let namespace_ident = iceberg::NamespaceIdent::from_vec(namespace_parts)?;

        // Convert PartitionSpec to UnboundPartitionSpec if present
        let unbound_partition_spec = partition_spec.map(|ps| ps.into());

        // Build the table creation request
        let table_creation = iceberg::TableCreation::builder()
            .name(table_name)
            .schema(schema)
            .partition_spec_opt(unbound_partition_spec)
            .sort_order_opt(sort_order)
            .properties(properties)
            .build();

        // Call the appropriate catalog method based on load_credentials flag
        let table = if load_credentials {
            self.as_ref()
                .create_table_with_credentials(&namespace_ident, table_creation)
                .await?
        } else {
            self.as_ref()
                .create_table(&namespace_ident, table_creation)
                .await?
        };

        Ok(IcebergTable { table })
    }

    /// Create a new namespace in the catalog
    pub async fn create_namespace(
        &self,
        namespace_parts: Vec<String>,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        // Convert namespace parts to NamespaceIdent
        let namespace_ident = iceberg::NamespaceIdent::from_vec(namespace_parts)?;

        // Create namespace with properties
        let _namespace = self
            .as_ref()
            .create_namespace(&namespace_ident, properties)
            .await?;

        Ok(())
    }

    /// Drop a table from the catalog
    pub async fn drop_table(&self, namespace_parts: Vec<String>, table_name: String) -> Result<()> {
        // Convert namespace parts and table name to TableIdent
        let table_ident = iceberg::TableIdent::new(
            iceberg::NamespaceIdent::from_vec(namespace_parts)?,
            table_name,
        );

        // Drop the table
        self.as_ref().drop_table(&table_ident).await?;

        Ok(())
    }

    /// Drop a namespace from the catalog
    pub async fn drop_namespace(&self, namespace_parts: Vec<String>) -> Result<()> {
        // Convert namespace parts to NamespaceIdent
        let namespace_ident = iceberg::NamespaceIdent::from_vec(namespace_parts)?;

        // Drop the namespace
        self.as_ref().drop_namespace(&namespace_ident).await?;

        Ok(())
    }
}

// Type aliases for response types
pub type IcebergCatalogResponse = IcebergBoxedResponse<IcebergCatalog>;
pub type IcebergBoolResponse = IcebergPropertyResponse<bool>;

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

// Create an empty catalog (default constructor exposed as sync FFI function)
#[no_mangle]
pub extern "C" fn iceberg_catalog_init() -> *mut IcebergCatalog {
    Box::into_raw(Box::new(IcebergCatalog::default()))
}

// Create a REST catalog from an existing catalog pointer (which may have an authenticator already set)
// This function takes ownership of the catalog pointer and returns it in the response
export_runtime_op!(
    iceberg_rest_catalog_create,
    IcebergCatalogResponse,
    || {
        if catalog.is_null() {
            return Err(anyhow::anyhow!("Null catalog pointer provided"));
        }
        // SAFETY: catalog was checked to be non-null above and came from FFI
        let catalog = unsafe { Box::from_raw(catalog) };
        let uri_str = parse_c_string(uri, "uri")?;
        let props = parse_properties(properties, properties_len)?;
        Ok::<(Box<IcebergCatalog>, String, HashMap<String, String>), anyhow::Error>((catalog, uri_str, props))
    },
    result_tuple,
    async {
        let (catalog, uri, props) = result_tuple;
        // create_rest takes ownership and returns the catalog
        catalog.create_rest(uri, props).await
    },
    catalog: *mut IcebergCatalog,
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

// Load a table from the catalog with vended credentials
export_runtime_op!(
    iceberg_catalog_load_table_with_credentials,
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
        catalog_ref.load_table_with_credentials(namespace_parts, table_name).await
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
    callback: CustomAuthenticatorCallback,
    auth_fn: *mut c_void,
) -> CResult {
    // Check for null catalog pointer
    if catalog.is_null() {
        return CResult::Error;
    }

    // SAFETY: catalog was checked to be non-null above.
    // The caller must ensure the catalog pointer remains valid for the duration of this call.
    let result = unsafe { &mut *catalog }.set_token_authenticator(callback, auth_fn);

    match result {
        Ok(()) => CResult::Ok,
        Err(e) => {
            eprintln!("Error setting token authenticator: {}", e);
            CResult::Error
        }
    }
}

/// Enable the storage credentials loader for the catalog.
/// Must be called before iceberg_rest_catalog_create.
/// When enabled, the catalog will use a loader that calls load_table_credentials
/// to obtain storage credentials for table data access.
#[no_mangle]
pub extern "C" fn iceberg_catalog_set_storage_credentials_loader(
    catalog: *mut IcebergCatalog,
) -> CResult {
    if catalog.is_null() {
        return CResult::Error;
    }

    let catalog_ref = unsafe { &mut *catalog };
    catalog_ref.use_credentials_loader = true;

    CResult::Ok
}

// Create a new table in the catalog with optional credential vending
export_runtime_op!(
    iceberg_catalog_create_table,
    crate::IcebergTableResponse,
    || {
        // Input validation
        if catalog.is_null() {
            return Err(anyhow::anyhow!("Null catalog pointer provided"));
        }

        // Parse arguments
        let namespace_parts = parse_string_array(namespace_parts_ptr, namespace_parts_len)?;
        let table_name = parse_c_string(table_name, "table_name")?;
        let schema_json = parse_c_string(schema_json, "schema_json")?;
        let partition_spec_json = parse_c_string(partition_spec_json, "partition_spec_json")?;
        let sort_order_json = parse_c_string(sort_order_json, "sort_order_json")?;
        let props = parse_properties(properties, properties_len)?;

        // Get catalog reference
        let catalog_ref = unsafe { &*catalog };

        // Deserialize JSON to Iceberg types
        let schema: iceberg::spec::Schema = serde_json::from_str(&schema_json)
            .map_err(|e| anyhow::anyhow!("Failed to parse schema JSON: {}", e))?;

        let partition_spec = if !partition_spec_json.is_empty() && partition_spec_json != "{}" {
            Some(serde_json::from_str(&partition_spec_json)
                .map_err(|e| anyhow::anyhow!("Failed to parse partition spec JSON: {}", e))?)
        } else {
            None
        };

        let sort_order = if !sort_order_json.is_empty() && sort_order_json != "{}" {
            Some(serde_json::from_str(&sort_order_json)
                .map_err(|e| anyhow::anyhow!("Failed to parse sort order JSON: {}", e))?)
        } else {
            None
        };

        Ok((catalog_ref, namespace_parts, table_name, schema, partition_spec, sort_order, props, load_credentials != 0))
    },
    result_tuple,
    async {
        let (catalog_ref, namespace_parts, table_name, schema, partition_spec, sort_order, props, load_credentials) = result_tuple;
        catalog_ref.create_table(namespace_parts, table_name, schema, partition_spec, sort_order, props, load_credentials).await
    },
    catalog: *mut IcebergCatalog,
    namespace_parts_ptr: *const *const c_char,
    namespace_parts_len: usize,
    table_name: *const c_char,
    schema_json: *const c_char,
    partition_spec_json: *const c_char,
    sort_order_json: *const c_char,
    properties: *const PropertyEntry,
    properties_len: usize,
    load_credentials: u8
);

// Create a new namespace in the catalog
export_runtime_op!(
    iceberg_catalog_create_namespace,
    crate::IcebergBoolResponse,
    || {
        // Input validation
        if catalog.is_null() {
            return Err(anyhow::anyhow!("Null catalog pointer provided"));
        }

        // Parse arguments
        let namespace_parts = parse_string_array(namespace_parts_ptr, namespace_parts_len)?;
        let props = parse_properties(properties, properties_len)?;

        // Get catalog reference
        let catalog_ref = unsafe { &*catalog };

        Ok((catalog_ref, namespace_parts, props))
    },
    result_tuple,
    async {
        let (catalog_ref, namespace_parts, props) = result_tuple;
        catalog_ref.create_namespace(namespace_parts, props).await?;
        Ok::<bool, anyhow::Error>(true) // Return true to indicate success
    },
    catalog: *mut IcebergCatalog,
    namespace_parts_ptr: *const *const c_char,
    namespace_parts_len: usize,
    properties: *const PropertyEntry,
    properties_len: usize
);

// Drop a table from the catalog
export_runtime_op!(
    iceberg_catalog_drop_table,
    crate::IcebergBoolResponse,
    || {
        // Input validation
        if catalog.is_null() {
            return Err(anyhow::anyhow!("Null catalog pointer provided"));
        }

        // Parse arguments
        let namespace_parts = parse_string_array(namespace_parts_ptr, namespace_parts_len)?;
        let table_name = parse_c_string(table_name, "table_name")?;

        // Get catalog reference
        let catalog_ref = unsafe { &*catalog };

        Ok((catalog_ref, namespace_parts, table_name))
    },
    result_tuple,
    async {
        let (catalog_ref, namespace_parts, table_name) = result_tuple;
        catalog_ref.drop_table(namespace_parts, table_name).await?;
        Ok::<bool, anyhow::Error>(true) // Return true to indicate success
    },
    catalog: *mut IcebergCatalog,
    namespace_parts_ptr: *const *const c_char,
    namespace_parts_len: usize,
    table_name: *const c_char
);

// Drop a namespace from the catalog
export_runtime_op!(
    iceberg_catalog_drop_namespace,
    crate::IcebergBoolResponse,
    || {
        // Input validation
        if catalog.is_null() {
            return Err(anyhow::anyhow!("Null catalog pointer provided"));
        }

        // Parse arguments
        let namespace_parts = parse_string_array(namespace_parts_ptr, namespace_parts_len)?;

        // Get catalog reference
        let catalog_ref = unsafe { &*catalog };

        Ok((catalog_ref, namespace_parts))
    },
    result_tuple,
    async {
        let (catalog_ref, namespace_parts) = result_tuple;
        catalog_ref.drop_namespace(namespace_parts).await?;
        Ok::<bool, anyhow::Error>(true) // Return true to indicate success
    },
    catalog: *mut IcebergCatalog,
    namespace_parts_ptr: *const *const c_char,
    namespace_parts_len: usize
);

export_runtime_op!(
    iceberg_catalog_invalidate_token,
    IcebergBoolResponse,
    || {
        if catalog.is_null() {
            return Err(anyhow::anyhow!("Null catalog pointer provided"));
        }
        // SAFETY: catalog was checked to be non-null above and came from FFI
        let catalog_ref = unsafe { &*catalog };
        Ok(catalog_ref)
    },
    catalog_ref,
    async {
        if let Some(cat) = &catalog_ref.catalog {
            cat.invalidate_token().await?;
        }
        Ok::<bool, anyhow::Error>(true)
    },
    catalog: *mut IcebergCatalog
);
