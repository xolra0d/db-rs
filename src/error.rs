use derive_more::Display;
use serde::Serialize;

pub type Result<T> = std::result::Result<T, Error>;

/// Universal error.
#[derive(Serialize, Debug, Display, PartialEq, Eq)]
pub enum Error {
    // mod storage
    #[display("System time went backword. Try again later.")]
    SystemTimeWentBackword,
    #[display("Database not found.")]
    DatabaseNotFound,
    #[display("Table not found.")]
    TableNotFound,
    #[display("Invalid table.")]
    InvalidTable,
    #[display("Invalid database name.")]
    InvalidDatabaseName,
    #[display("Invalid column name: {_0}")]
    InvalidColumnName(String),
    #[display("Database already exists.")]
    DatabaseAlreadyExists,
    #[display("Table already exists.")]
    TableAlreadyExists,

    // mod sql
    #[display("Couldn't parse SQL.")]
    SqlToAstConversion,
    #[display("No statement provided")]
    EmptyStatement,
    #[display("Unsupported command: {_0}.")]
    UnsupportedCommand(String),
    #[display("Unsupported column name: {_0}.")]
    UnsupportedColumnType(String),
    #[display("Invalid engine name.")]
    InvalidEngineName,
    #[display("Unsupported table option: {_0}")]
    UnsupportedTableOption(String),
    #[display("Invalid ORDER BY.")]
    InvalidOrderBy,
    #[display("Invalid table name.")]
    InvalidTableName,
    #[display("No columns specified.")]
    NoColumnsSpecified,
    #[display("Invalid columns specified.")]
    InvalidColumnsSpecified,
    #[display("Invalid source of values.")]
    InvalidSource,
    #[display("Unsupported column constraint: {_0}")]
    UnsupportedColumnConstraint(String),
    #[display("Could not insert data: {_0}.")]
    CouldNotInsertData(String),
    #[display("Could not remove bad part: {_0}.")]
    CouldNotRemoveBadPart(String),
    #[display("No values provided")]
    EmptySource,
    #[display("Table entry already exists")]
    TableEntryAlreadyExists,
    #[display("Permission denied")]
    PermissionDenied,

    // mod engines
    #[display("No ORDER BY columns found")]
    OrderByColumnsNotFound,

    // mod main
    SendResponse, // does not need display
}
