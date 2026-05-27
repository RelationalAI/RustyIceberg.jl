// Stable error codes for the Iceberg FFI.
//
// Errors are serialised into the `error_message` C-string using a three-part
// tab-separated format so no new FFI fields are needed:
//
//   "{code}\t{user_msg}\t{detail}"
//
// Julia splits on the first two tabs, tries to parse the leading integer as
// an `IcebergError` enum variant, and constructs `IcebergException` from the
// three pieces. Any error message that does not match this pattern is treated
// as `INTERNAL`.
//
// Julia counterpart: the `IcebergError` @enum in `src/RustyIceberg.jl`.
// Both sides must be kept in sync — if you add or rename a variant here,
// update the Julia enum too. The "IcebergError enum sync" testset in
// `test/error_tests.jl` calls `iceberg_all_error_codes()` to verify both
// directions at test time.

use strum::{EnumIter, IntoEnumIterator, IntoStaticStr};

/// Canonical set of error codes.
///
/// Variant names are intentionally SCREAMING_SNAKE_CASE to match the Julia
/// `IcebergError @enum` names exactly, so that `IntoStaticStr` returns the
/// right key without extra `#[strum(serialize = …)]` attributes.
///
/// The `pub const` aliases below expose the same values for use throughout
/// the classification logic — existing call-sites are unchanged.
#[derive(EnumIter, IntoStaticStr, Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u32)]
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
pub enum IcebergErrorCode {
    // ── Not Found (1xx) ──────────────────────────────────────────────────────
    NOT_FOUND_METADATA = 101,
    NOT_FOUND_DATA_FILE = 102,
    NOT_FOUND_TABLE = 103,
    NOT_FOUND_NAMESPACE = 104,
    NOT_FOUND_SNAPSHOT = 105,
    // ── Permission / Auth (2xx) ───────────────────────────────────────────────
    AUTH_FAILED = 201,
    AUTH_TOKEN_EXPIRED = 202,
    AUTH_INSUFFICIENT = 203,
    // ── Data / Corruption (3xx) ───────────────────────────────────────────────
    DATA_METADATA_INVALID = 301,
    DATA_FILE_CORRUPT = 302,
    DATA_SCHEMA_MISMATCH = 303,
    DATA_TYPE_MISMATCH = 304,
    // ── Catalog (4xx) ─────────────────────────────────────────────────────────
    CATALOG_TABLE_EXISTS = 401,
    CATALOG_NAMESPACE_EXISTS = 402,
    CATALOG_NAMESPACE_NOT_EMPTY = 403,
    CATALOG_REST_ONLY = 404,
    CATALOG_COMMIT_CONFLICT = 405,
    // ── Resource state (5xx) ──────────────────────────────────────────────────
    STATE_RESOURCE_FREED = 501,
    STATE_TRANSACTION_CONSUMED = 502,
    STATE_WRITER_CLOSED = 503,
    // ── I/O (6xx) ─────────────────────────────────────────────────────────────
    IO_NETWORK = 601,
    IO_S3 = 602,
    IO_LOCAL = 603,
    // ── Internal (9xx) ────────────────────────────────────────────────────────
    INTERNAL = 900,
}

// ─────────────────────────────────────────────────────────────────────────────

/// Return a newline-separated list of `"CODE\tNAME"` pairs for every defined
/// error code.  Used by the Julia "IcebergError enum sync" testset to verify
/// that the Rust and Julia enum definitions are in sync.
///
/// The returned pointer must be freed with `iceberg_destroy_cstring`.
#[no_mangle]
pub extern "C" fn iceberg_all_error_codes() -> *mut std::os::raw::c_char {
    let s = IcebergErrorCode::iter()
        .map(|v| format!("{}\t{}", v as u32, <&'static str>::from(v)))
        .collect::<Vec<_>>()
        .join("\n");
    std::ffi::CString::new(s).unwrap().into_raw()
}

// ─────────────────────────────────────────────────────────────────────────────

/// An `anyhow`-compatible error that carries a stable code and a short
/// user-visible message alongside the raw technical detail.
#[derive(Debug)]
pub struct ClassifiedError {
    pub code: IcebergErrorCode,
    pub user_msg: String,
    pub detail: String,
}

impl std::fmt::Display for ClassifiedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}\t{}\t{}",
            self.code as u32, self.user_msg, self.detail
        )
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
    code: IcebergErrorCode,
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
    let classify_str = format!("{e}");
    let (code, user_msg) = classify_by_kind(e.kind(), &classify_str);
    anyhow::Error::new(ClassifiedError {
        code,
        user_msg,
        detail: format!("{e:?} [{}:{}]", loc.file(), loc.line()),
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

fn classify_by_kind(kind: iceberg::ErrorKind, detail: &str) -> (IcebergErrorCode, String) {
    use iceberg::ErrorKind::*;
    match kind {
        TableNotFound => (IcebergErrorCode::NOT_FOUND_TABLE, "Table not found".into()),
        NamespaceNotFound => (
            IcebergErrorCode::NOT_FOUND_NAMESPACE,
            "Namespace not found".into(),
        ),
        TableAlreadyExists => (
            IcebergErrorCode::CATALOG_TABLE_EXISTS,
            "Table already exists".into(),
        ),
        NamespaceAlreadyExists => (
            IcebergErrorCode::CATALOG_NAMESPACE_EXISTS,
            "Namespace already exists".into(),
        ),
        CatalogCommitConflicts => (
            IcebergErrorCode::CATALOG_COMMIT_CONFLICT,
            "Commit conflict: table was modified concurrently".into(),
        ),
        DataInvalid => classify_data_invalid(detail),
        _ => classify_message(detail),
    }
}

fn classify_data_invalid(detail: &str) -> (IcebergErrorCode, String) {
    let lower = detail.to_lowercase();
    if lower.contains("snapshot") && lower.contains("not found") {
        return (
            IcebergErrorCode::NOT_FOUND_SNAPSHOT,
            "Snapshot not found".into(),
        );
    }
    // JSON parse errors contain positional text like "at line N column M" — check
    // for json parsing first so "column M" doesn't trigger DATA_SCHEMA_MISMATCH.
    if lower.contains("parse json")
        || lower.contains("json string")
        || lower.contains("eof while parsing")
    {
        return (
            IcebergErrorCode::DATA_METADATA_INVALID,
            "Invalid table metadata".into(),
        );
    }
    if lower.contains("parquet") {
        (
            IcebergErrorCode::DATA_FILE_CORRUPT,
            "Corrupt or unreadable data file".into(),
        )
    } else if lower.contains("column") || lower.contains("field") {
        (
            IcebergErrorCode::DATA_SCHEMA_MISMATCH,
            "Column not found in table schema".into(),
        )
    } else {
        (
            IcebergErrorCode::DATA_METADATA_INVALID,
            "Invalid table metadata".into(),
        )
    }
}

fn classify_message(detail: &str) -> (IcebergErrorCode, String) {
    let lower = detail.to_lowercase();

    // ── Auth ──────────────────────────────────────────────────────────────────
    if lower.contains("permissiondenied")
        || lower.contains("permission denied")
        || lower.contains("invalidaccesskeyid")
        || lower.contains("accessdenied")
        || lower.contains("authorizationqueryparameterserror")
        || lower.contains("invalid access key")
    {
        return (
            IcebergErrorCode::AUTH_FAILED,
            "Authentication failed: access denied".into(),
        );
    }
    if lower.contains("loading credential")
        || lower.contains("loadcredential")
        || (lower.contains("credential") && lower.contains("sign"))
        || lower.contains("ec2 metadata")
    {
        return (
            IcebergErrorCode::AUTH_FAILED,
            "Authentication failed: could not load credentials".into(),
        );
    }
    if lower.contains("token") && (lower.contains("expired") || lower.contains("refresh")) {
        return (
            IcebergErrorCode::AUTH_TOKEN_EXPIRED,
            "Token refresh failed".into(),
        );
    }

    // ── Not found ─────────────────────────────────────────────────────────────
    if (lower.contains("nosuchkey")
        || lower.contains("not found")
        || lower.contains("no such file"))
        && (lower.contains(".parquet") || lower.contains("parquet"))
    {
        return (
            IcebergErrorCode::NOT_FOUND_DATA_FILE,
            "Data file not found".into(),
        );
    }
    if lower.contains("nosuchkey")
        || (lower.contains("not found") && lower.contains("metadata"))
        || (lower.contains("no such file") && lower.contains("metadata"))
    {
        return (
            IcebergErrorCode::NOT_FOUND_METADATA,
            "Metadata file not found".into(),
        );
    }
    if lower.contains("nosuchkey") || (lower.contains("no such file")) {
        return (
            IcebergErrorCode::NOT_FOUND_METADATA,
            "Metadata file not found".into(),
        );
    }

    // ── Resource state ────────────────────────────────────────────────────────
    if lower.contains("writer already closed") {
        return (
            IcebergErrorCode::STATE_WRITER_CLOSED,
            "Writer has already been closed".into(),
        );
    }
    if (lower.contains("writer") && lower.contains("freed"))
        || lower.contains("writer is not initialized")
    {
        return (
            IcebergErrorCode::STATE_RESOURCE_FREED,
            "Resource has been freed".into(),
        );
    }
    if lower.contains("transaction already consumed") {
        return (
            IcebergErrorCode::STATE_TRANSACTION_CONSUMED,
            "Transaction has already been committed or rolled back".into(),
        );
    }
    if lower.contains("null") && lower.contains("pointer") {
        return (
            IcebergErrorCode::STATE_RESOURCE_FREED,
            "Resource has been freed".into(),
        );
    }
    if lower.contains("not initialized") || lower.contains("catalog not initialized") {
        return (
            IcebergErrorCode::STATE_RESOURCE_FREED,
            "Resource has been freed".into(),
        );
    }

    // ── Catalog ───────────────────────────────────────────────────────────────
    if lower.contains("rest catalog") || lower.contains("rest catalogs") {
        return (
            IcebergErrorCode::CATALOG_REST_ONLY,
            "This operation requires a REST catalog".into(),
        );
    }
    if lower.contains("namespace not empty") || lower.contains("non-empty namespace") {
        return (
            IcebergErrorCode::CATALOG_NAMESPACE_NOT_EMPTY,
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
        return (
            IcebergErrorCode::DATA_FILE_CORRUPT,
            "Corrupt or unreadable data file".into(),
        );
    }
    if lower.contains("invalid")
        && (lower.contains("metadata") || lower.contains("json") || lower.contains("schema"))
    {
        return (
            IcebergErrorCode::DATA_METADATA_INVALID,
            "Invalid table metadata".into(),
        );
    }
    // "column not found" (scan) OR "Field id N not found" (writer/iceberg-rs)
    if (lower.contains("column") || lower.contains("field")) && lower.contains("not found") {
        return (
            IcebergErrorCode::DATA_SCHEMA_MISMATCH,
            "Column not found in table schema".into(),
        );
    }

    // ── I/O ───────────────────────────────────────────────────────────────────
    if lower.contains("connection refused")
        || lower.contains("connection reset")
        || lower.contains("timed out")
        || lower.contains("network error")
    {
        return (IcebergErrorCode::IO_NETWORK, "Network error".into());
    }
    if lower.contains("s3error") || (lower.contains("s3") && lower.contains("error")) {
        return (IcebergErrorCode::IO_S3, "S3 error".into());
    }

    (
        IcebergErrorCode::INTERNAL,
        "Internal error (please report this as a bug)".into(),
    )
}
