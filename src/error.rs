use derive_more::Display;
use serde::Serialize;

#[derive(Serialize, Debug, Display)]
pub enum Error {
    // mod storage
    #[display("Table is empty.")]
    EmptyTable,
    #[display("Different length of given columns.")]
    ColumnLengthDiff,
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
    #[display("Invalid ORDER BY.")]
    InvalidOrderBy,
    #[display("Invalid table name.")]
    InvalidTableName,
    #[display("No columns specified.")]
    NoColumnsSpecified,
    #[display("Invalid source of values.")]
    InvalidSource,

    // mod main
    SendResponse, // does not need display
}

pub type Result<T> = std::result::Result<T, Error>;
