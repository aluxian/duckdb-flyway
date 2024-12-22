import os
import importlib.util
from typing import List
from duckdb import DuckDBPyConnection
from loguru import logger
from migrations.types import Migration


def get_migrations(migrations_dir: str) -> List[Migration]:
    """Automatically discover and load all migration files"""
    migrations = []

    # List all Python files in migrations directory
    for filename in sorted(os.listdir(migrations_dir)):
        if (
            filename.startswith("m")
            and filename.endswith(".py")
            and not filename.startswith("__")
        ):
            filepath = os.path.join(migrations_dir, filename)

            # Load the module
            spec = importlib.util.spec_from_file_location(filename[:-3], filepath)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                # Get the migration object
                if hasattr(module, "migration"):
                    migrations.append(module.migration)
                else:
                    logger.warning(
                        f"Migration file {filename} has no 'migration' export"
                    )

    return migrations


class MigrationError(Exception):
    """Raised when a migration fails"""

    pass


class MigrationsService:
    def __init__(self, con: DuckDBPyConnection):
        self.con = con
        self.migrations_dir = os.path.join(
            os.path.dirname(__file__), "..", "migrations"
        )

    def get_all_migrations(self) -> List[Migration]:
        """Return all migrations in order"""
        return get_migrations(self.migrations_dir)

    def init_migrations_table(self):
        """Create the migrations tracking table if it doesn't exist"""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS _migrations (
                id TEXT PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT now()
            );
        """)

    def get_applied_migrations(self) -> List[str]:
        """Get list of already applied migration IDs"""
        return [
            row[0]
            for row in self.con.execute(
                "SELECT id FROM _migrations ORDER BY id"
            ).fetchall()
        ]

    def validate_migration_order(self, migrations: List[Migration], applied: set[str]):
        """Ensure new migrations have higher IDs than applied ones"""
        if not applied:
            return

        max_applied = max(applied)
        new_migrations = [m for m in migrations if m.id not in applied]

        if any(m.id < max_applied for m in new_migrations):
            raise MigrationError(
                f"Found new migration(s) with ID lower than latest applied migration {max_applied}"
            )

    def apply_migration(self, migration: Migration):
        """Apply a single migration and record it"""
        logger.info(f"Applying migration {migration.id}")

        try:
            # Explicitly start transaction
            self.con.begin()

            # Run the migration function
            migration.run(self.con)

            # Record migration as applied
            self.con.execute("INSERT INTO _migrations (id) VALUES (?)", [migration.id])

            # Commit the transaction
            self.con.commit()
            logger.info(f"Successfully applied migration {migration.id}")

        except Exception as e:
            # Now rollback will work because we explicitly started the transaction
            self.con.rollback()
            logger.error(f"Failed to apply migration {migration.id}: {str(e)}")
            raise MigrationError(f"Migration {migration.id} failed: {str(e)}") from e

    def run_migrations(self, migrations: List[Migration]):
        """Run all pending migrations in order"""
        try:
            self.init_migrations_table()

            # Get already applied migrations
            applied = set(self.get_applied_migrations())

            # Validate migration order
            self.validate_migration_order(migrations, applied)

            # Apply each migration in its own transaction
            for migration in sorted(migrations, key=lambda m: m.id):
                if migration.id not in applied:
                    self.apply_migration(migration)

        except Exception as e:
            logger.error("Migration failed, application startup aborted")
            raise MigrationError("Failed to run migrations") from e
