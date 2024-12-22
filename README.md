# DuckDB Flyway (migration manager)

A simple migration manager for DuckDB databases, inspired by Flyway.

## Features

- Simple and lightweight Python-based migrations
- Automatic migration discovery from directory
- Transaction safety - each migration runs in its own transaction
- Migration version validation ensures correct ordering
- Customizable logging via standard Python logging

## Installation

```sh
pip install duckdb-flyway
```

## Usage

1. Create a migrations directory in your project. Migration files must:
   - Start with 'm'
   - End with '.py'
   - Export a 'migration' object
   - Have unique, sortable IDs (typically timestamps)

```
migrations/
  m20240320000001_create_users.py
  m20240320000002_add_email.py
```

2. Each migration file should export a `migration` object:

```python
from duckdb_flyway import Migration
from duckdb import DuckDBPyConnection

def run(con: DuckDBPyConnection) -> None:
    """Create the users table.

    Args:
        con: DuckDB connection to use for the migration.
            Transaction management is handled automatically.
    """
    con.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT now()
        );
    """)

# ID must be unique and sortable (typically a timestamp)
migration = Migration("20240320000001", run)
```

3. Run migrations in your app:

```python
import duckdb
from duckdb_flyway import DuckDBFlyway, MigrationError

try:
    # Connect to your database
    con = duckdb.connect("path/to/db.duckdb")

    # Create migrations service - migrations_dir is required
    flyway = DuckDBFlyway(con, migrations_dir="path/to/migrations")

    # Load and run all pending migrations
    migrations = flyway.find_migrations()
    flyway.run_migrations(migrations)

except MigrationError as e:
    print(f"Migration failed: {e}")
    # Handle migration failure
```

## How it Works

- Migrations are discovered from Python files in the migrations directory
- Each migration runs in its own transaction for safety
- Migrations are tracked in a `schema_migrations` table
- New migrations must have higher IDs than previously applied ones
- Failed migrations are rolled back automatically

## Development

1. Clone the repository and install dependencies:

```sh
git clone https://github.com/aluxian/duckdb-flyway.git
cd duckdb-flyway
uv venv
source .venv/bin/activate
uv sync
```

2. Run linting checks:

```sh
uv run ruff check .
```

3. Run tests:

```sh
uv run pytest
```

4. Start Aider:

```sh
uvx --python 3.12 --from 'aider-chat[playwright]' --with 'aider-chat[help]' aider
```
