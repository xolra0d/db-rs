# TouchHouse - Blazingly Fast Column-Oriented Database (Rust)
___
**Lightweight Rust-based database server**, which provides a simple protocol for database operations over TCP.
___
## Features
- **Database Management**: Create and manage multiple databases with simple commands
- **Message Pack over TCP**: Efficient binary serialization format
- **Concurrent Connections**: Handle multiple client connections simultaneously
___
## Available Commands
- **`help`** - Shows all available commands
- **`help <command_name>`** - Shows description for specific command
___
## Modules
- `src/main.rs` - Server entry point and connection handling
- `src/engine.rs` - Core database engine and command execution
- `src/commands/` - command system (echo, help...)
- `src/protocol/` - Custom binary protocol implementation
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

### Configuration
Set environment variables to customize the server:

```bash
# Server configuration
export HOST=127.0.0.1        # Default: 127.0.0.1
export PORT=7070             # Default: 7070

# Database location  
export DB_DIR=db_files       # Default: db_files

# Logging configuration
export LOG_LEVEL=1           # 1=Info, 2=Warn, 3=Error (Default: 1)
```

### Example Usage
```bash
HOST=0.0.0.0 PORT=8080 DB_DIR=/var/lib/touchhouse cargo run
```

### Client Connection

```bash
python3 client.py
```

### Example Database Operations
```bash
# List available commands
help

# Get help for specific command
help create

# Create a database
create database mydb

# Create a table with fields
create table mydb users name String email String
```
___
## Tech Stack
- **[`tokio`](https://tokio.rs/)** - Async runtime for Rust
- **[`futures`](https://docs.rs/futures/)** - Async primitives and utilities
- **[`log`](https://docs.rs/log/)** - Logging facade
- **[`env_logger`](https://docs.rs/env_logger/)** - Logging implementation
- **[`tokio-util`](https://docs.rs/tokio-util/)** - Tokio utilities and codecs

## Allowed data types
- **Array**: `Command::Array(sequence)`
- **String**: `Command::String(your_string)`


## Docs
For more in-depth description read `cargo doc --open`.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---
**Fun Fact**: Named after the famous Scottish TouchHouse, which during some time had an owner full in debts (which will happen to you if you decide to run it in production without proper backup strategies)
