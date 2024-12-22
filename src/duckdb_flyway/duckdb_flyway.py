import os
import importlib.util
from typing import Union, Callable, TypeAlias
from os import PathLike
from duckdb import DuckDBPyConnection
from loguru import logger as default_logger
from dataclasses import dataclass
from logging import Logger

MigrationFunction: TypeAlias = Callable[[DuckDBPyConnection], None]


@dataclass
class Migration:
    """A database migration that can be applied to a DuckDB database.

    Attributes:
        id: Unique identifier for the migration, typically a timestamp like '20240320000001'.
            Must be a string that can be ordered lexicographically.
        run: Function that performs the migration. Takes a DuckDB connection as argument
            and must handle its own transaction management.
    """

    id: str
    run: MigrationFunction


class MigrationError(Exception):
    """Raised when a migration fails to apply"""

    pass


class DuckDBFlyway:
    def __init__(
        self,
        con: DuckDBPyConnection,
        migrations_dir: Union[str, PathLike, None] = None,
        logger: Logger = default_logger,
    ):
        """Initialize DuckDBFlyway.

        Args:
            con: DuckDB connection
            migrations_dir: Path to migrations directory
            logger: Logger instance to use
        """
        self.con = con
        self.logger = logger

        if migrations_dir is None:
            raise ValueError("migrations_dir parameter is required - must specify path to migrations directory")
        self.migrations_dir = migrations_dir

    def find_migrations(self) -> list[Migration]:
        """Load all migration files from the migrations directory.

        Discovers Python files starting with 'm' and ending in '.py'.
        Each file must export a 'migration' object of type Migration.
        Files are loaded in alphabetical order by filename.

        Returns:
            List of migration objects sorted by ID
        """
        migrations = []

        # List all Python files in migrations directory
        for filename in sorted(os.listdir(self.migrations_dir)):
            if (
                filename.startswith("m")
                and filename.endswith(".py")
                and not filename.startswith("__")
            ):
                filepath = os.path.join(self.migrations_dir, filename)

                # Load the module
                spec = importlib.util.spec_from_file_location(filename[:-3], filepath)
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)

                    # Get the migration object
                    if hasattr(module, "migration"):
                        migrations.append(module.migration)
                    else:
                        self.logger.warning(
                            f"Migration file {filename} skipped: missing required 'migration' export"
                        )

        return migrations

    def init_schema_migrations(self) -> None:
        """Create the schema migrations tracking table if it doesn't exist"""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                id TEXT PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT now()
            );
        """)

    def get_applied_migrations(self) -> list[str]:
        """Get list of already applied migration IDs.
        
        Returns:
            List of migration IDs that have been applied, sorted by ID
        """
        return [
            row[0]
            for row in self.con.execute(
                "SELECT id FROM schema_migrations ORDER BY id"
            ).fetchall()
        ]

    def validate_migration_order(self, migrations: list[Migration], applied: set[str]) -> None:
        """Ensure new migrations have higher IDs than applied ones.
        
        Args:
            migrations: List of all migrations to validate
            applied: Set of already applied migration IDs
            
        Raises:
            MigrationError: If a new migration has an ID lower than an applied one
        """
        if not applied:
            return

        max_applied = max(applied)
        new_migrations = [m for m in migrations if m.id not in applied]

        if any(m.id < max_applied for m in new_migrations):
            raise MigrationError(
                f"Invalid migration order: found new migration(s) with ID lower than "
                f"latest applied migration {max_applied}. All new migrations must have "
                f"higher IDs than existing ones."
            )

    def _apply_migration(self, migration: Migration) -> None:
        """Apply a single migration and record it"""
        self.logger.info(f"Applying migration {migration.id}")

        try:
            # Explicitly start transaction
            self.con.begin()

            # Run the migration function
            migration.run(self.con)

            # Record migration as applied
            self.con.execute(
                "INSERT INTO schema_migrations (id) VALUES (?)", [migration.id]
            )

            # Commit the transaction
            self.con.commit()
            self.logger.info(f"Successfully applied migration {migration.id}")

        except Exception as e:
            # Now rollback will work because we explicitly started the transaction
            self.con.rollback()
            self.logger.error(f"Failed to apply migration {migration.id}: {str(e)}")
            raise MigrationError(f"Migration {migration.id} failed: {str(e)}") from e

    def run_migrations(self, migrations: list[Migration]) -> None:
        """Run all pending migrations in order"""
        try:
            self.init_schema_migrations()

            # Get already applied migrations
            applied = set(self.get_applied_migrations())

            # Validate migration order
            self.validate_migration_order(migrations, applied)

            # Apply each migration in its own transaction
            for migration in sorted(migrations, key=lambda m: m.id):
                if migration.id not in applied:
                    self._apply_migration(migration)

        except Exception as e:
            self.logger.error(f"Migration failed: {str(e)}")
            raise MigrationError(f"Failed to run migrations: {str(e)}") from e

    def find_and_run_migrations(self) -> None:
        """Find and run all pending migrations.
        
        Convenience method that combines find_migrations() and run_migrations().
        
        Raises:
            MigrationError: If any migration fails to apply
        """
        migrations = self.find_migrations()
        self.run_migrations(migrations)
