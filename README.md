# DuckDB Flyway (migration manager)

A simple migration manager for DuckDB databases, inspired by Flyway.

## Why?

- Simple and lightweight
- Automatic migration discovery
- Transaction safety
- Migration version validation
- Customizable logging

## Installation

```sh
pip install duckdb-flyway
```

## Usage

1. Create a migrations directory in your project:

```python
migrations/
  m20240320000001_create_users.py
  m20240320000002_add_email.py
```

2. Each migration file should export a `migration` object:

```python
from duckdb_flyway import Migration

def run(con: DuckDBPyConnection) -> None:
    """Create the users table.
    
    Args:
        con: DuckDB connection to use for the migration
    """
    con.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT now()
        );
    """)

migration = Migration("20240320000001", run)
```

3. Run migrations in your app:

```python
import duckdb
from duckdb_flyway import DuckDBFlyway

# Connect to your database
con = duckdb.connect("path/to/db.duckdb")

# Create migrations service with migrations directory
flyway = DuckDBFlyway(con, migrations_dir="path/to/migrations")

# Run all pending migrations
flyway.run_migrations(flyway.find_migrations())
```

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
