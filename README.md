!!UNDER DEVELOPMENT!!
---
# TouchHouse - Blazingly Fast Column-Oriented Database
___
## Features
- **Message Pack over TCP**: Efficient binary serialization format
- **Concurrent Connections**: Handle multiple client connections simultaneously
___
## Modules
- `src/main.rs` - Server entry point and connection handling
- `src/engines/` - Database engines implementations
- `src/sql/` - Sql parsing and execution
- `src/storage/` - Storage specific implementations
- `src/config.rs` - Configuration management with environment variables
___
## Installation & Usage

### Prerequisites
- [Rust](https://rustup.rs/)

### Quick Start
1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd touchhouse
   ```

2. **Run the server**:
   ```bash
   cargo run --release
   ```
   The server will start on `127.0.0.1:7070` by default.

### Example Usage

### Client Connection

```bash
python3 client.py HOST PORT
```

### Example Database Operations
```bash
# Create a database
CREATE DATABASE mydb;

# Create a table
CREATE TABLE my_db.users (id UUID, name String, age UInt8) ENGINE=MergeTree ORDER BY (name, age)
```
___
## Tech Stack
- **[`tokio`](https://tokio.rs/)** - Async runtime for Rust
- **[`futures`](https://docs.rs/futures/)** - Async primitives and utilities
- **[`log`](https://docs.rs/log/)** - Logging facade
- **[`env_logger`](https://docs.rs/env_logger/)** - Logging implementation
- **[`tokio_util`](https://docs.rs/tokio-util/)** - Tokio utilities and codecs
- **[`serde`](https://docs.rs/serde/)** - Serializing and deserializing framework
- **[`rmp_serde`](https://docs.rs/rmp-serde/)** - Rust MessagePack library
- **[`bincode`](https://docs.rs/bincode/)** - Fast serializing/deserializing protocol
- **[`sqlparser`](sqhttps://docs.rs/sqlparser/)** - Apache Datafusion SQL to AST parser

## Data types
- **String**
- **Uuid**
- **Bool**
- **Int8**
- **Int16**
- **Int32**
- **Int64**
- **UInt8**
- **UInt16**
- **UInt32**
- **UInt64**


## Docs
For more in-depth description read `cargo doc --open`.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
