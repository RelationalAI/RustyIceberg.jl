/// Utility functions for FFI argument parsing
use crate::PropertyEntry;
use anyhow::Result;
use std::collections::HashMap;
use std::ffi::{c_char, CStr};

/// Parse FFI property entries into a HashMap
pub fn parse_properties(
    properties: *const PropertyEntry,
    properties_len: usize,
) -> Result<HashMap<String, String>> {
    let mut props = HashMap::new();
    if !properties.is_null() && properties_len > 0 {
        let properties_slice = unsafe { std::slice::from_raw_parts(properties, properties_len) };

        for prop in properties_slice {
            let key = unsafe {
                CStr::from_ptr(prop.key)
                    .to_str()
                    .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in property key: {}", e))?
            };
            let value = unsafe {
                CStr::from_ptr(prop.value)
                    .to_str()
                    .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in property value: {}", e))?
            };
            props.insert(key.to_string(), value.to_string());
        }
    }
    Ok(props)
}

/// Parse FFI string array (pointer to array of c_char pointers)
pub fn parse_string_array(ptr: *const *const c_char, len: usize) -> Result<Vec<String>> {
    let mut strings = Vec::new();
    if !ptr.is_null() && len > 0 {
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
        for str_ptr in slice {
            let s = unsafe {
                CStr::from_ptr(*str_ptr)
                    .to_str()
                    .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in string: {}", e))?
            };
            strings.push(s.to_string());
        }
    }
    Ok(strings)
}

/// Parse a single c_char pointer to String
pub fn parse_c_string(ptr: *const c_char, field_name: &str) -> Result<String> {
    unsafe {
        CStr::from_ptr(ptr)
            .to_str()
            .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in {}: {}", field_name, e))
            .map(|s| s.to_string())
    }
}
