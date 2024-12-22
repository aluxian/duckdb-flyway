import duckdb
import pytest
from duckdb import DuckDBPyConnection

from duckdb_flyway import Migration
from .duckdb_flyway import DuckDBFlyway, MigrationError


def create_test_migration(id: str, sql: str = None) -> Migration:
    """Create a test migration with optional SQL.

    Args:
        id: Migration ID
        sql: Optional SQL to execute in the migration

    Returns:
        Migration object ready for testing
    """

    def run(con):
        if sql:
            con.execute(sql)

    return Migration(id, run)


def table_exists(con: DuckDBPyConnection, table_name: str) -> bool:
    """Check if a table exists in the database.

    Args:
        con: Database connection
        table_name: Name of table to check

    Returns:
        True if table exists, False otherwise
    """
    result = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?", [table_name]
    ).fetchone()
    return bool(result)


def get_table_names(con: DuckDBPyConnection) -> list[str]:
    """Get list of all table names in database.

    Args:
        con: Database connection

    Returns:
        List of table names
    """
    return [
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables ORDER BY table_name"
        ).fetchall()
    ]


@pytest.fixture(scope="function")
def test_db_connection() -> DuckDBPyConnection:
    """Create an in-memory database connection for testing.

    Returns:
        DuckDBPyConnection: Fresh in-memory database connection
    """
    con = duckdb.connect(":memory:")
    yield con
    con.close()


@pytest.fixture(scope="function")
def flyway(test_db_connection, tmp_path) -> DuckDBFlyway:
    """Create DuckDBFlyway instance with fresh connection and temp migrations dir.

    Args:
        test_db_connection: Database connection fixture
        tmp_path: Temporary directory path fixture

    Returns:
        DuckDBFlyway: Configured instance
    """
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    return DuckDBFlyway(test_db_connection, migrations_dir=migrations_dir)


def test_init_schema_migrations(flyway: DuckDBFlyway) -> None:
    """Test schema migrations table creation.

    Verifies that:
    - The schema_migrations table is created
    - The table has the correct columns and types
    """
    flyway.init_schema_migrations()

    # Verify table exists and has correct schema
    result = flyway.con.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'schema_migrations'
        ORDER BY column_name;
    """).fetchall()

    assert len(result) == 2
    assert result[0][0] == "applied_at"
    assert result[1][0] == "id"


def test_get_applied_migrations_empty(flyway: DuckDBFlyway) -> None:
    """Test getting applied migrations when none exist."""
    flyway.init_schema_migrations()
    assert flyway.get_applied_migrations() == []


def test_get_applied_migrations(flyway: DuckDBFlyway) -> None:
    """Test getting applied migrations.

    Verifies that migrations are returned in correct order and contain expected IDs.
    """
    flyway.init_schema_migrations()
    flyway.con.execute(
        "INSERT INTO schema_migrations (id) VALUES (?), (?)",
        ["20240320000000", "20240320000001"],
    )

    applied = flyway.get_applied_migrations()
    assert applied == ["20240320000000", "20240320000001"]


def test_validate_migration_order_valid(flyway: DuckDBFlyway) -> None:
    """Test validation passes for correctly ordered migrations"""
    applied = {"20240320000000", "20240320000001"}
    migrations = [
        Migration("20240320000000", lambda _: None),
        Migration("20240320000001", lambda _: None),
        Migration("20240320000002", lambda _: None),
    ]

    # Should not raise exception
    flyway.validate_migration_order(migrations, applied)


def test_validate_migration_order_invalid(flyway: DuckDBFlyway) -> None:
    """Test validation fails for out-of-order migrations"""
    applied = {"20240320000002"}
    migrations = [
        Migration("20240320000001", lambda _: None),
        Migration("20240320000002", lambda _: None),
    ]

    with pytest.raises(MigrationError) as exc_info:
        flyway.validate_migration_order(migrations, applied)
    assert "Invalid migration order: found new migration(s) with ID lower than " in str(
        exc_info.value
    )


def test_apply_migration_creates_table_and_records_success(flyway, mocker) -> None:
    """Test successful migration application.

    Verifies that:
    - The migration function is called with correct connection
    - The migration is recorded in schema_migrations table
    - The changes from the migration are committed
    """
    flyway.init_schema_migrations()

    # Mock migration that creates a test table
    migration_func = mocker.Mock(autospec=True)
    migration_func.side_effect = lambda con: con.execute(
        "CREATE TABLE test (id INTEGER);"
    )

    migration = Migration("20240320000001", migration_func)
    flyway._apply_migration(migration)

    # Verify migration was recorded
    result = flyway.con.execute("SELECT id FROM schema_migrations").fetchone()
    assert result[0] == "20240320000001"

    # Verify migration effect (table exists)
    result = flyway.con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name = 'test'
    """).fetchone()
    assert result[0] == "test"

    # Verify migration function was called exactly once with correct args
    migration_func.assert_called_once()
    assert isinstance(migration_func.call_args[0][0], DuckDBPyConnection)


def test_apply_migration_failure(flyway) -> None:
    """Test failed migration rolls back changes"""
    flyway.init_schema_migrations()

    # Create a migration that will fail by creating the same table twice
    def run_migration(con: DuckDBPyConnection):
        con.execute("CREATE TABLE test_table (id INTEGER);")
        con.execute("CREATE TABLE test_table (id INTEGER);")  # This will fail

    migration = Migration("20240320000001", run_migration)

    with pytest.raises(MigrationError) as exc_info:
        flyway._apply_migration(migration)
    assert "Migration 20240320000001 failed" in str(exc_info.value)

    # Verify migration was not recorded
    result = flyway.con.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()
    assert result[0] == 0

    # Verify the table was not created (rolled back)
    result = flyway.con.execute("""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_name = 'test_table'
    """).fetchone()
    assert result[0] == 0


def test_run_migrations_success(flyway) -> None:
    """Test running multiple migrations successfully"""
    migrations = [
        create_test_migration("20240320000001", "CREATE TABLE test1 (id INTEGER)"),
        create_test_migration("20240320000002", "CREATE TABLE test2 (id INTEGER)"),
    ]

    flyway.run_migrations(migrations)

    assert flyway.get_applied_migrations() == ["20240320000001", "20240320000002"]
    assert table_exists(flyway.con, "test1")
    assert table_exists(flyway.con, "test2")


def test_custom_logger(flyway, mocker) -> None:
    """Test that custom logger is used"""
    custom_logger = mocker.Mock()
    flyway = DuckDBFlyway(
        flyway.con, migrations_dir=flyway.migrations_dir, logger=custom_logger
    )

    migrations = [
        Migration(
            "20240320000001",
            lambda con: con.execute("CREATE TABLE test1 (id INTEGER);"),
        )
    ]

    flyway.run_migrations(migrations)

    # Verify custom logger was called
    custom_logger.info.assert_any_call("Applying migration 20240320000001")
    custom_logger.info.assert_any_call("Successfully applied migration 20240320000001")


def test_run_migrations_failure(flyway) -> None:
    """Test that migrations are applied one by one and stop at failure"""
    # Create test migrations with one that fails
    migrations = [
        Migration(
            "20240320000001",
            lambda con: con.execute("CREATE TABLE test1 (id INTEGER);"),
        ),
        Migration("20240320000002", lambda _: raise_exception()),
        Migration(
            "20240320000003",
            lambda con: con.execute("CREATE TABLE test3 (id INTEGER);"),
        ),
    ]

    with pytest.raises(MigrationError):
        flyway.run_migrations(migrations)

    # Verify only first migration was recorded
    result = flyway.con.execute("SELECT id FROM schema_migrations").fetchall()
    assert len(result) == 1
    assert result[0][0] == "20240320000001"

    # Verify only first table was created
    tables = flyway.con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name IN ('test1', 'test3')
        ORDER BY table_name
    """).fetchall()
    assert len(tables) == 1
    assert tables[0][0] == "test1"


def raise_exception() -> None:
    """Helper function to raise an exception for testing error scenarios.

    Raises:
        Exception: Always raises with message "Migration failed"
    """
    raise Exception("Migration failed")
