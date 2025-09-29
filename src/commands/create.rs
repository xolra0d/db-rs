use crate::commands::DatabaseCommand;
use crate::engine::{Engine, FieldType};
use crate::protocol::{Command, CommandError, CommandResult};

use std::str::from_utf8;
use std::path::Path;

/// Command for creating databases and tables.
/// ### Examples
/// - To create database: "create database <database_name>"
/// - To create table: "create table <database_name> <table_name> <field_name:String> <field_type:String> <field_name:String> <field_type:String>..."
pub struct CreateCommand;

/// Validation constants
const MIN_NAME_LENGTH: usize = 1;
const MAX_NAME_LENGTH: usize = 64;
const ALLOWED_NAME_CHARS: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";

/// Validates database or table name
fn validate_name(name: &str) -> CommandResult<()> {
    if name.len() < MIN_NAME_LENGTH {
        return Err(CommandError::ExecutionError(
            format!("Name must be at least {} characters long", MIN_NAME_LENGTH).into(),
        ));
    }
    
    if name.len() > MAX_NAME_LENGTH {
        return Err(CommandError::ExecutionError(
            format!("Name must be at most {} characters long", MAX_NAME_LENGTH).into(),
        ));
    }
    
    if !name.chars().all(|c| ALLOWED_NAME_CHARS.contains(c)) {
        return Err(CommandError::ExecutionError(
            "Name can only contain alphanumeric characters and underscores".into(),
        ));
    }
    
    if name.starts_with('_') {
        return Err(CommandError::ExecutionError(
            "Name cannot start with underscore".into(),
        ));
    }
    
    Ok(())
}

impl DatabaseCommand for CreateCommand {
    fn name() -> &'static str {
        "create"
    }

    fn execute(args: &[Command], engine: &Engine) -> CommandResult<Command> {
        if args.len() < 2 {
            return Err(CommandError::ExecutionError(
                "Usage: create database <name> OR create table <db_name> <table_name> [field_name:field_type...]".into(),
            ));
        }

        let Command::String(data) = &args[0] else {
            return Err(CommandError::ExecutionError(
                "First argument must be a string (database or table)".into(),
            ));
        };

        match &data[..] {
            b"database" => {
                if args.len() != 2 {
                    return Err(CommandError::ExecutionError(
                        "Usage: create database <name>".into(),
                    ));
                }
                create_db(&args[1], engine)
            }
            b"table" => {
                if args.len() < 3 {
                    return Err(CommandError::ExecutionError(
                        "Usage: create table <db_name> <table_name> [field_name:field_type...]".into(),
                    ));
                }
                create_table(&args[1..], engine)
            }
            arg => Err(CommandError::ExecutionError(
                format!(
                    "Expected 'database' or 'table', got: '{}'",
                    from_utf8(arg).unwrap_or("invalid utf8")
                )
                .into(),
            )),
        }
    }

    fn description() -> &'static str {
        "Create a new database or table. Usage: create database <name> OR create table <db_name> <table_name> [field_name:field_type...]"
    }
}

fn create_db(db_name: &Command, engine: &Engine) -> CommandResult<Command> {
    let Command::String(data) = db_name else {
        return Err(CommandError::ExecutionError(
            "Database name must be a string".into(),
        ));
    };

    let db_name_str = Engine::bytes_to_str(data)?;
    validate_name(db_name_str)?;

    let db_path = engine.get_db_dir().join(db_name_str);

    // Check if database already exists before attempting to create
    if db_path.exists() {
        return Err(CommandError::ExecutionError(
            format!("Database '{}' already exists", db_name_str).into(),
        ));
    }

    match std::fs::create_dir(&db_path) {
        Ok(_) => Ok(Command::String("OK".into())),
        Err(error) => match error.kind() {
            std::io::ErrorKind::AlreadyExists => Err(CommandError::ExecutionError(
                format!("Database '{}' already exists", db_name_str).into(),
            )),
            std::io::ErrorKind::PermissionDenied => Err(CommandError::ExecutionError(
                format!("Permission denied to create database '{}'", db_name_str).into(),
            )),
            std::io::ErrorKind::InvalidInput => Err(CommandError::ExecutionError(
                format!("Invalid database name '{}'", db_name_str).into(),
            )),
            _ => Err(CommandError::ExecutionError(
                format!("Failed to create database '{}': {}", db_name_str, error).into(),
            )),
        },
    }
}

fn create_table(args: &[Command], engine: &Engine) -> CommandResult<Command> {
    // Parse database name
    let Command::String(db_name) = &args[0] else {
        return Err(CommandError::ExecutionError(
            "Database name must be a string".into(),
        ));
    };
    let db_name_str = Engine::bytes_to_str(db_name)?;
    validate_name(db_name_str)?;

    // Parse table name
    let Command::String(table_name) = &args[1] else {
        return Err(CommandError::ExecutionError(
            "Table name must be a string".into(),
        ));
    };
    let table_name_str = Engine::bytes_to_str(table_name)?;
    validate_name(table_name_str)?;

    // Check if database exists
    let db_path = engine.get_db_dir().join(db_name_str);
    if !db_path.exists() {
        return Err(CommandError::ExecutionError(
            format!("Database '{}' does not exist", db_name_str).into(),
        ));
    }

    let table_full_path = db_path.join(table_name_str);
    
    // Check if table already exists
    if table_full_path.exists() {
        return Err(CommandError::ExecutionError(
            format!("Table '{}' already exists in database '{}'", table_name_str, db_name_str).into(),
        ));
    }

    // Parse fields more efficiently
    let fields = parse_fields(&args[2..])?;
    
    if fields.is_empty() {
        return Err(CommandError::ExecutionError(
            "Table must have at least one field".into(),
        ));
    }

    // Create table directory
    std::fs::create_dir(&table_full_path)?;

    // Create field files in batch
    create_field_files(&table_full_path, &fields)?;

    Ok(Command::String("OK".into()))
}

/// Parse field definitions from command arguments
fn parse_fields(args: &[Command]) -> CommandResult<Vec<(String, FieldType)>> {
    if args.len() % 2 != 0 {
        return Err(CommandError::ExecutionError(
            "Field definitions must come in pairs (name, type)".into(),
        ));
    }

    let mut fields = Vec::with_capacity(args.len() / 2);
    
    for chunk in args.chunks_exact(2) {
        let Command::String(field_name) = &chunk[0] else {
            return Err(CommandError::ExecutionError(
                "Field name must be a string".into(),
            ));
        };
        let field_name_str = Engine::bytes_to_str(field_name)?;
        validate_name(field_name_str)?;

        let Command::String(field_type) = &chunk[1] else {
            return Err(CommandError::ExecutionError(
                "Field type must be a string".into(),
            ));
        };
        let field_type = Engine::parse_field_type(field_type)?;
        
        // Check for duplicate field names
        if fields.iter().any(|(name, _)| name == field_name_str) {
            return Err(CommandError::ExecutionError(
                format!("Duplicate field name: '{}'", field_name_str).into(),
            ));
        }
        
        fields.push((field_name_str.to_string(), field_type));
    }

    Ok(fields)
}

/// Create field files efficiently
fn create_field_files(table_path: &Path, fields: &[(String, FieldType)]) -> CommandResult<()> {
    for (field_name, field_type) in fields {
        let field_type_str = field_type.to_str();
        let field_file_path = table_path.join(format!("{}.{}", field_name, field_type_str));
        
        std::fs::write(&field_file_path, field_type_str)
            .map_err(|e| CommandError::ExecutionError(
                format!("Failed to create field file '{}': {}", field_file_path.display(), e).into(),
            ))?;
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::bytes::Bytes;

    #[test]
    fn test_create_database_success() {
        let temp_dir = std::env::temp_dir().join("test_db_create_database_success");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let engine = Engine::new(temp_dir.clone());

        let args = vec![
            Command::String(Bytes::from("database")),
            Command::String(Bytes::from("testdb")),
        ];

        let result = CreateCommand::execute(&args, &engine).unwrap();

        if let Command::String(response) = result {
            let response_str = String::from_utf8(response.to_vec()).unwrap();
            assert_eq!(response_str, "OK");
        } else {
            println!("GOT ERROR: {:?}", result);
            panic!("Expected success string result");
        }

        assert!(temp_dir.join("testdb").exists());
        assert!(temp_dir.join("testdb").is_dir());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_create_table_success() {
        let temp_dir = std::env::temp_dir().join("test_db_create_table_success");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let engine = Engine::new(temp_dir.clone());

        // First create database
        let args = vec![
            Command::String(Bytes::from("database")),
            Command::String(Bytes::from("testdb")),
        ];

        let result = CreateCommand::execute(&args, &engine).unwrap();
        assert!(matches!(result, Command::String(_)));

        let test_db_path = temp_dir.join("testdb");
        assert!(test_db_path.exists());
        assert!(test_db_path.is_dir());

        // Then create table
        let args = vec![
            Command::String(Bytes::from("table")),
            Command::String(Bytes::from("testdb")),
            Command::String(Bytes::from("test_table")),
            Command::String(Bytes::from("name")),
            Command::String(Bytes::from("String")),
            Command::String(Bytes::from("job")),
            Command::String(Bytes::from("String")),
        ];

        let result = CreateCommand::execute(&args, &engine).unwrap();
        assert!(matches!(result, Command::String(_)));

        let table_path = test_db_path.join("test_table");
        assert!(table_path.exists());
        assert!(table_path.is_dir());
        
        // Check field files were created
        assert!(table_path.join("name.String").exists());
        assert!(table_path.join("job.String").exists());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_create_database_validation() {
        let temp_dir = std::env::temp_dir().join("test_db_create_validation");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let engine = Engine::new(temp_dir.clone());

        // Test empty name
        let args = vec![
            Command::String(Bytes::from("database")),
            Command::String(Bytes::from("")),
        ];
        assert!(CreateCommand::execute(&args, &engine).is_err());

        // Test invalid characters
        let args = vec![
            Command::String(Bytes::from("database")),
            Command::String(Bytes::from("test-db")),
        ];
        assert!(CreateCommand::execute(&args, &engine).is_err());

        // Test name starting with underscore
        let args = vec![
            Command::String(Bytes::from("database")),
            Command::String(Bytes::from("_testdb")),
        ];
        assert!(CreateCommand::execute(&args, &engine).is_err());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_create_table_duplicate_fields() {
        let temp_dir = std::env::temp_dir().join("test_db_duplicate_fields");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let engine = Engine::new(temp_dir.clone());

        // Create database first
        let args = vec![
            Command::String(Bytes::from("database")),
            Command::String(Bytes::from("testdb")),
        ];
        CreateCommand::execute(&args, &engine).unwrap();

        // Try to create table with duplicate field names
        let args = vec![
            Command::String(Bytes::from("table")),
            Command::String(Bytes::from("testdb")),
            Command::String(Bytes::from("test_table")),
            Command::String(Bytes::from("name")),
            Command::String(Bytes::from("String")),
            Command::String(Bytes::from("name")), // Duplicate!
            Command::String(Bytes::from("String")),
        ];
        assert!(CreateCommand::execute(&args, &engine).is_err());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
