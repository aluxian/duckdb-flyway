import pytest
from duckdb import DuckDBPyConnection
import duckdb
from migrations.types import Migration
from .migrations_service import MigrationsService, MigrationError


@pytest.fixture(scope="function")
def test_db_connection():
    """Create an in-memory database connection for testing"""
    con = duckdb.connect(":memory:")
    yield con
    con.close()


@pytest.fixture(scope="function")
def service(test_db_connection):
    """Create MigrationsService instance with fresh connection"""
    return MigrationsService(test_db_connection)


def test_init_migrations_table(service):
    """Test migrations table creation"""
    service.init_migrations_table()

    # Verify table exists and has correct schema
    result = service.con.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '_migrations'
        ORDER BY column_name;
    """).fetchall()

    assert len(result) == 2
    assert result[0][0] == "applied_at"
    assert result[1][0] == "id"


def test_get_applied_migrations_empty(service):
    """Test getting applied migrations when none exist"""
    service.init_migrations_table()
    assert service.get_applied_migrations() == []


def test_get_applied_migrations(service):
    """Test getting applied migrations"""
    service.init_migrations_table()
    service.con.execute(
        "INSERT INTO _migrations (id) VALUES (?), (?)",
        ["20240320000000", "20240320000001"],
    )

    applied = service.get_applied_migrations()
    assert applied == ["20240320000000", "20240320000001"]


def test_validate_migration_order_valid(service):
    """Test validation passes for correctly ordered migrations"""
    applied = {"20240320000000", "20240320000001"}
    migrations = [
        Migration("20240320000000", lambda _: None),
        Migration("20240320000001", lambda _: None),
        Migration("20240320000002", lambda _: None),
    ]

    # Should not raise exception
    service.validate_migration_order(migrations, applied)


def test_validate_migration_order_invalid(service):
    """Test validation fails for out-of-order migrations"""
    applied = {"20240320000002"}
    migrations = [
        Migration("20240320000001", lambda _: None),
        Migration("20240320000002", lambda _: None),
    ]

    with pytest.raises(MigrationError) as exc_info:
        service.validate_migration_order(migrations, applied)
    assert "Found new migration(s) with ID lower than latest applied migration" in str(
        exc_info.value
    )


def test_apply_migration_creates_table_and_records_success(service, mocker):
    """Test successful migration application creates table and records migration"""
    service.init_migrations_table()

    # Mock migration that creates a test table
    migration_func = mocker.Mock(autospec=True)
    migration_func.side_effect = lambda con: con.execute(
        "CREATE TABLE test (id INTEGER);"
    )

    migration = Migration("20240320000001", migration_func)
    service.apply_migration(migration)

    # Verify migration was recorded
    result = service.con.execute("SELECT id FROM _migrations").fetchone()
    assert result[0] == "20240320000001"

    # Verify migration effect (table exists)
    result = service.con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name = 'test'
    """).fetchone()
    assert result[0] == "test"

    # Verify migration function was called exactly once with correct args
    migration_func.assert_called_once()
    assert isinstance(migration_func.call_args[0][0], DuckDBPyConnection)


def test_apply_migration_failure(service):
    """Test failed migration rolls back changes"""
    service.init_migrations_table()

    # Create a migration that will fail by creating the same table twice
    def run_migration(con: DuckDBPyConnection):
        con.execute("CREATE TABLE test_table (id INTEGER);")
        con.execute("CREATE TABLE test_table (id INTEGER);")  # This will fail

    migration = Migration("20240320000001", run_migration)

    with pytest.raises(MigrationError) as exc_info:
        service.apply_migration(migration)
    assert "Migration 20240320000001 failed" in str(exc_info.value)

    # Verify migration was not recorded
    result = service.con.execute("SELECT COUNT(*) FROM _migrations").fetchone()
    assert result[0] == 0

    # Verify the table was not created (rolled back)
    result = service.con.execute("""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_name = 'test_table'
    """).fetchone()
    assert result[0] == 0


def test_run_migrations_success(service):
    """Test running multiple migrations successfully"""
    # Create test migrations
    migrations = [
        Migration(
            "20240320000001",
            lambda con: con.execute("CREATE TABLE test1 (id INTEGER);"),
        ),
        Migration(
            "20240320000002",
            lambda con: con.execute("CREATE TABLE test2 (id INTEGER);"),
        ),
    ]

    service.run_migrations(migrations)

    # Verify both migrations were recorded
    result = service.con.execute("SELECT id FROM _migrations ORDER BY id").fetchall()
    assert [r[0] for r in result] == ["20240320000001", "20240320000002"]

    # Verify both tables exist
    tables = service.con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name IN ('test1', 'test2')
        ORDER BY table_name
    """).fetchall()
    assert [t[0] for t in tables] == ["test1", "test2"]


def test_run_migrations_failure(service):
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
        service.run_migrations(migrations)

    # Verify only first migration was recorded
    result = service.con.execute("SELECT id FROM _migrations").fetchall()
    assert len(result) == 1
    assert result[0][0] == "20240320000001"

    # Verify only first table was created
    tables = service.con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name IN ('test1', 'test3')
        ORDER BY table_name
    """).fetchall()
    assert len(tables) == 1
    assert tables[0][0] == "test1"


def raise_exception():
    """Helper function to raise an exception"""
    raise Exception("Migration failed")
