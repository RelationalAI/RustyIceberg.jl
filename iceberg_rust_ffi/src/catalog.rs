/// Catalog support for iceberg_rust_ffi
use crate::IcebergTable;
use anyhow::Result;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use std::collections::HashMap;
use std::ffi::c_char;

/// Opaque catalog handle for FFI
#[repr(C)]
pub struct IcebergCatalog {
    catalog: Box<dyn Catalog>,
}

impl IcebergCatalog {
    /// Create a new REST catalog
    pub async fn create_rest(uri: String, props: HashMap<String, String>) -> Result<Self> {
        let mut catalog_props = props;
        catalog_props.insert("uri".to_string(), uri);

        let catalog = RestCatalogBuilder::default()
            .load("rest", catalog_props)
            .await?;

        Ok(IcebergCatalog {
            catalog: Box::new(catalog),
        })
    }

    /// Load a table by namespace and name
    pub async fn load_table(
        &self,
        namespace_parts: Vec<String>,
        table_name: String,
    ) -> Result<IcebergTable> {
        let namespace = NamespaceIdent::from_vec(namespace_parts)?;
        let table_ident = TableIdent::new(namespace, table_name);
        let table = self.catalog.load_table(&table_ident).await?;

        Ok(IcebergTable { table })
    }

    /// List tables in a namespace
    pub async fn list_tables(&self, namespace_parts: Vec<String>) -> Result<Vec<String>> {
        let namespace = NamespaceIdent::from_vec(namespace_parts)?;
        let tables = self.catalog.list_tables(&namespace).await?;

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

        let namespaces = self.catalog.list_namespaces(parent.as_ref()).await?;

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
        self.catalog
            .table_exists(&table_ident)
            .await
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
