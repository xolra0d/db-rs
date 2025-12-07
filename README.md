# TouchHouse - Blazingly Fast Column-Oriented Database
___
## Modules
- `src/main.rs` - Server entry point and connection handling
- `src/engines/` - Database engines implementations
- `src/sql/` - Sql parsing and execution
- `src/storage/` - Storage specific implementations
- `src/config.rs` - Configuration management with environment variables
___
## Installation & Usage

### Quick Start
1. **Grap binary**:
   ```bash
   curl -L https://github.com/xolra0d/touchhouse/releases/latest/download/touchhouse -o touchhouse
   ```

2. **Run the server**:
   ```bash
   chmod +x touchouse
   ./touchouse
   ```
   The server will create default configuration and start on `127.0.0.1:7070`.

### Example Usage

### Client Connection

```bash
python3 client.py HOST PORT
```

### Example Database Operations
```bash
CREATE DATABASE mydb;
CREATE TABLE my_db.users (id UUID, name String, age UInt8) ENGINE=MergeTree ORDER BY (name, age)
INSERT INTO my_db.users (id, name, age) VALUES ('123e4567-e89b-12d3-a456-426614174000', 'Alice', 30)
SELECT * FROM my_db.users WHERE name = 'Alice' LIMIT 1
```
___
## Tech Stack
- **[`tokio`](https://tokio.rs/)** - Async runtime for Rust.
- **[`futures`](https://docs.rs/futures/)** - Async primitives and utilities.
- **[`log`](https://docs.rs/log/)** - Logging facade.
- **[`env_logger`](https://docs.rs/env_logger/)** - Logging implementation.
- **[`tokio_util`](https://docs.rs/tokio-util/)** - Tokio utilities and codecs.
- **[`serde`](https://docs.rs/serde/)** - Serializing and deserializing framework.
- **[`rmp_serde`](https://docs.rs/rmp-serde/)** - Rust MessagePack library.
- **[`sqlparser`](https://docs.rs/sqlparser/)** - Apache Datafusion SQL to AST parser.
- **[`toml`](https://docs.rs/toml/)** - Toml configuration.
- **[`uuid`](https://docs.rs/uuid/)** - Uuid support.
- **[`derive_more`](https://docs.rs/derive_more/)** - Display support for enums.
- **[`lz4`](https://docs.rs/lz4/)** - LZ4 compression/decompression.
- **[`dashmap`](https://docs.rs/dashmap/)** - Global runtime configuration.
- **[`rkyv`](https://docs.rs/rkyv/)** - Zero copy deserialization.
- **[`rayon`](https://docs.rs/rayon/)** - Program parallelization.
- **[`memmap2`](https://docs.rs/memmap2/)** - File memory mapping for faster access.

## Docs
Read more in `docs/`.

For more in-depth description `cargo doc --open`.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
