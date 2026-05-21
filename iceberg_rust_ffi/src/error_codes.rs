/// Stable error codes for the Iceberg FFI.
///
/// Errors are serialised into the `error_message` C-string using a three-part
/// tab-separated format so no new FFI fields are needed:
///
///   "{code}\t{user_msg}\t{detail}"
///
/// Julia splits on the first two tabs, tries to parse the leading integer as
/// an `IcebergError` enum variant, and constructs `IcebergException` from the
/// three pieces. Any error message that does not match this pattern is treated
/// as `INTERNAL`.

// ── Not Found (1xx) ──────────────────────────────────────────────────────────
pub const NOT_FOUND_METADATA: u32 = 101;
pub const NOT_FOUND_DATA_FILE: u32 = 102;
pub const NOT_FOUND_TABLE: u32 = 103;
pub const NOT_FOUND_NAMESPACE: u32 = 104;
#[allow(dead_code)]
pub const NOT_FOUND_SNAPSHOT: u32 = 105;

// ── Permission / Auth (2xx) ───────────────────────────────────────────────────
pub const AUTH_FAILED: u32 = 201;
pub const AUTH_TOKEN_EXPIRED: u32 = 202;
#[allow(dead_code)]
pub const AUTH_INSUFFICIENT: u32 = 203;

// ── Data / Corruption (3xx) ───────────────────────────────────────────────────
pub const DATA_METADATA_INVALID: u32 = 301;
pub const DATA_FILE_CORRUPT: u32 = 302;
pub const DATA_SCHEMA_MISMATCH: u32 = 303;
#[allow(dead_code)]
pub const DATA_TYPE_MISMATCH: u32 = 304;

// ── Catalog (4xx) ─────────────────────────────────────────────────────────────
pub const CATALOG_TABLE_EXISTS: u32 = 401;
pub const CATALOG_NAMESPACE_EXISTS: u32 = 402;
pub const CATALOG_NAMESPACE_NOT_EMPTY: u32 = 403;
pub const CATALOG_REST_ONLY: u32 = 404;
pub const CATALOG_COMMIT_CONFLICT: u32 = 405;

// ── Resource state (5xx) ──────────────────────────────────────────────────────
pub const STATE_RESOURCE_FREED: u32 = 501;
pub const STATE_TRANSACTION_CONSUMED: u32 = 502;
pub const STATE_WRITER_CLOSED: u32 = 503;

// ── I/O (6xx) ─────────────────────────────────────────────────────────────────
pub const IO_NETWORK: u32 = 601;
pub const IO_S3: u32 = 602;
#[allow(dead_code)]
pub const IO_LOCAL: u32 = 603;

// ── Internal (9xx) ────────────────────────────────────────────────────────────
pub const INTERNAL: u32 = 900;

// ─────────────────────────────────────────────────────────────────────────────

/// An `anyhow`-compatible error that carries a stable code and a short
/// user-visible message alongside the raw technical detail.
#[derive(Debug)]
pub struct ClassifiedError {
    pub code: u32,
    pub user_msg: String,
    pub detail: String,
}

impl std::fmt::Display for ClassifiedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}\t{}\t{}", self.code, self.user_msg, self.detail)
    }
}

impl std::error::Error for ClassifiedError {}

/// Build a pre-classified `anyhow::Error` directly.
///
/// `#[track_caller]` embeds the call site (file:line) into `detail` at
/// zero runtime cost so that `IcebergException.detail` in Julia always
/// identifies which FFI guard fired.
#[track_caller]
pub fn classified_error(
    code: u32,
    user_msg: impl Into<String>,
    detail: impl Into<String>,
) -> anyhow::Error {
    let loc = std::panic::Location::caller();
    anyhow::Error::new(ClassifiedError {
        code,
        user_msg: user_msg.into(),
        detail: format!("{} [{}:{}]", detail.into(), loc.file(), loc.line()),
    })
}

/// Classify an `iceberg::Error` directly.
///
/// Intentionally does NOT delegate to `classify()` so that `#[track_caller]`
/// records the call site of *this* function rather than a line inside
/// `classify()`. All callers must use a closure (`map_err(|e| classify_iceberg(e))`)
/// rather than a bare function pointer so that the closure body's location is
/// captured, not a location inside `map_err`.
#[track_caller]
pub fn classify_iceberg(e: iceberg::Error) -> anyhow::Error {
    let loc = std::panic::Location::caller();
    let detail = format!("{:#}", e);
    let (code, user_msg) = classify_by_kind(e.kind(), &detail);
    anyhow::Error::new(ClassifiedError {
        code,
        user_msg,
        detail: format!("{detail} [{}:{}]", loc.file(), loc.line()),
    })
}

/// Classify an `anyhow::Error`, returning a new `anyhow::Error` whose
/// `Display` output is in the `"{code}\t{user_msg}\t{detail}"` format.
///
/// Errors that are already `ClassifiedError` are passed through unchanged
/// (their original call-site location is preserved).  New errors get
/// `[file:line]` appended to `detail`. Callers must use a closure
/// (`map_err(|e| classify(e))`) so that the closure body's location is
/// captured by `#[track_caller]`.
#[track_caller]
pub fn classify(e: anyhow::Error) -> anyhow::Error {
    if e.is::<ClassifiedError>() {
        return e;
    }
    let loc = std::panic::Location::caller();
    if let Some(ie) = e.downcast_ref::<iceberg::Error>() {
        let detail = format!("{:#}", ie);
        let (code, user_msg) = classify_by_kind(ie.kind(), &detail);
        return anyhow::Error::new(ClassifiedError {
            code,
            user_msg,
            detail: format!("{detail} [{}:{}]", loc.file(), loc.line()),
        });
    }
    let detail = format!("{:#}", e);
    let (code, user_msg) = classify_message(&detail);
    anyhow::Error::new(ClassifiedError {
        code,
        user_msg,
        detail: format!("{detail} [{}:{}]", loc.file(), loc.line()),
    })
}

// ── Internal helpers ─────────────────────────────────────────────────────────

fn classify_by_kind(kind: iceberg::ErrorKind, detail: &str) -> (u32, String) {
    use iceberg::ErrorKind::*;
    match kind {
        TableNotFound => (NOT_FOUND_TABLE, "Table not found".into()),
        NamespaceNotFound => (NOT_FOUND_NAMESPACE, "Namespace not found".into()),
        TableAlreadyExists => (CATALOG_TABLE_EXISTS, "Table already exists".into()),
        NamespaceAlreadyExists => (CATALOG_NAMESPACE_EXISTS, "Namespace already exists".into()),
        CatalogCommitConflicts => (
            CATALOG_COMMIT_CONFLICT,
            "Commit conflict: table was modified concurrently".into(),
        ),
        DataInvalid => classify_data_invalid(detail),
        _ => classify_message(detail),
    }
}

fn classify_data_invalid(detail: &str) -> (u32, String) {
    let lower = detail.to_lowercase();
    if lower.contains("parquet") {
        (DATA_FILE_CORRUPT, "Corrupt or unreadable data file".into())
    } else if lower.contains("column") || lower.contains("field") {
        (
            DATA_SCHEMA_MISMATCH,
            "Column not found in table schema".into(),
        )
    } else {
        (DATA_METADATA_INVALID, "Invalid table metadata".into())
    }
}

fn classify_message(detail: &str) -> (u32, String) {
    let lower = detail.to_lowercase();

    // ── Auth ──────────────────────────────────────────────────────────────────
    if lower.contains("permissiondenied")
        || lower.contains("permission denied")
        || lower.contains("invalidaccesskeyid")
        || lower.contains("accessdenied")
        || lower.contains("authorizationqueryparameterserror")
        || lower.contains("invalid access key")
    {
        return (AUTH_FAILED, "Authentication failed: access denied".into());
    }
    if lower.contains("token") && (lower.contains("expired") || lower.contains("refresh")) {
        return (AUTH_TOKEN_EXPIRED, "Token refresh failed".into());
    }

    // ── Not found ─────────────────────────────────────────────────────────────
    if (lower.contains("nosuchkey")
        || lower.contains("not found")
        || lower.contains("no such file"))
        && (lower.contains(".parquet") || lower.contains("parquet"))
    {
        return (NOT_FOUND_DATA_FILE, "Data file not found".into());
    }
    if lower.contains("nosuchkey")
        || (lower.contains("not found") && lower.contains("metadata"))
        || (lower.contains("no such file") && lower.contains("metadata"))
    {
        return (NOT_FOUND_METADATA, "Metadata file not found".into());
    }
    if lower.contains("nosuchkey") || (lower.contains("no such file")) {
        return (NOT_FOUND_METADATA, "Metadata file not found".into());
    }

    // ── Resource state ────────────────────────────────────────────────────────
    if lower.contains("writer already closed") {
        return (STATE_WRITER_CLOSED, "Writer has already been closed".into());
    }
    if (lower.contains("writer") && lower.contains("freed"))
        || lower.contains("writer is not initialized")
    {
        return (STATE_RESOURCE_FREED, "Resource has been freed".into());
    }
    if lower.contains("transaction already consumed") {
        return (
            STATE_TRANSACTION_CONSUMED,
            "Transaction has already been committed or rolled back".into(),
        );
    }
    if lower.contains("null") && lower.contains("pointer") {
        return (STATE_RESOURCE_FREED, "Resource has been freed".into());
    }
    if lower.contains("not initialized") || lower.contains("catalog not initialized") {
        return (STATE_RESOURCE_FREED, "Resource has been freed".into());
    }

    // ── Catalog ───────────────────────────────────────────────────────────────
    if lower.contains("rest catalog") || lower.contains("rest catalogs") {
        return (
            CATALOG_REST_ONLY,
            "This operation requires a REST catalog".into(),
        );
    }
    if lower.contains("namespace not empty") || lower.contains("non-empty namespace") {
        return (
            CATALOG_NAMESPACE_NOT_EMPTY,
            "Cannot drop non-empty namespace".into(),
        );
    }

    // ── Data corruption ───────────────────────────────────────────────────────
    if lower.contains("parquet")
        && (lower.contains("invalid")
            || lower.contains("corrupt")
            || lower.contains("parse")
            || lower.contains("error"))
    {
        return (DATA_FILE_CORRUPT, "Corrupt or unreadable data file".into());
    }
    if lower.contains("invalid")
        && (lower.contains("metadata") || lower.contains("json") || lower.contains("schema"))
    {
        return (DATA_METADATA_INVALID, "Invalid table metadata".into());
    }
    if lower.contains("column") && lower.contains("not found") {
        return (
            DATA_SCHEMA_MISMATCH,
            "Column not found in table schema".into(),
        );
    }

    // ── I/O ───────────────────────────────────────────────────────────────────
    if lower.contains("connection refused")
        || lower.contains("connection reset")
        || lower.contains("timed out")
        || lower.contains("network error")
    {
        return (IO_NETWORK, "Network error".into());
    }
    if lower.contains("s3error") || (lower.contains("s3") && lower.contains("error")) {
        return (IO_S3, "S3 error".into());
    }

    (
        INTERNAL,
        "Internal error (please report this as a bug)".into(),
    )
}
