"""Guard: the test suite must never reach the MySQL database from ``.env``."""
import nws.config
import nws.db


def test_nws_db_engine_is_sqlite_during_tests():
    assert nws.db._get_engine().url.get_backend_name() == "sqlite"


def test_mysql_url_is_not_mysql_during_tests():
    assert not str(nws.config.MYSQL_URL).startswith("mysql")
    assert not str(nws.db.MYSQL_URL).startswith("mysql")


def test_reset_engine_rebuilds_from_guarded_config():
    nws.db.reset_engine()
    assert nws.db._get_engine().url.get_backend_name() == "sqlite"
