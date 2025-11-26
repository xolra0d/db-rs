mod command_runner;
mod execution;
mod logical_plan;
mod plan_optimization;
mod sql_parser;

pub use command_runner::CommandRunner;

use crate::error::{Error, Result};
use crate::storage::ColumnDef;

use sqlparser::ast::Ident;

/// Validate the name of fields, databases, columns.
/// Returns true if `name` consists of english alphabet, numbers and underscore
/// Otherwise returns false
pub fn validate_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

pub fn parse_ident(ident: &Ident, columns: &[ColumnDef]) -> Result<ColumnDef> {
    if let Some(column_def) = columns.iter().find(|col| col.name == ident.value) {
        Ok(column_def.clone())
    } else {
        Err(Error::ColumnNotFound(format!(
            "Column specified ({}) was not found",
            ident.value
        )))
    }
}

#[cfg(test)]
pub mod tests {
    use crate::sql::validate_name;

    #[test]
    fn test_invalid_names() {
        assert!(!validate_name("*"));
        assert!(!validate_name("csji="));
        assert!(!validate_name("csji122yrd01/"));
        assert!(!validate_name(""));
    }

    #[test]
    fn test_valid_names() {
        assert!(validate_name("coffee_shop"));
        assert!(validate_name("amsterdam"));
        assert!(validate_name("John_Data"));
    }
}
