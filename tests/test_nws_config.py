import importlib
import os

import nws.config as nws_config


def test_to_sync_mysql_url_replaces_aiomysql_driver():
    assert (
        nws_config._to_sync_mysql_url("mysql+aiomysql://dbhost/testdb")
        == "mysql+pymysql://dbhost/testdb"
    )


def test_to_sync_mysql_url_replaces_aiomysql_case_insensitively():
    assert (
        nws_config._to_sync_mysql_url("MYSQL+AIOMYSQL://dbhost/testdb")
        == "MYSQL+pymysql://dbhost/testdb"
    )


def test_mysql_url_falls_back_to_mysql_database_url_with_sync_driver():
    orig_mysql_url = os.environ.get("MYSQL_URL")
    orig_mysql_database_url = os.environ.get("MYSQL_DATABASE_URL")

    try:
        os.environ.pop("MYSQL_URL", None)
        os.environ["MYSQL_DATABASE_URL"] = "mysql+aiomysql://dbhost/testdb"
        reloaded = importlib.reload(nws_config)
        assert reloaded.MYSQL_URL == "mysql+pymysql://dbhost/testdb"
    finally:
        if orig_mysql_url is None:
            os.environ.pop("MYSQL_URL", None)
        else:
            os.environ["MYSQL_URL"] = orig_mysql_url
        if orig_mysql_database_url is None:
            os.environ.pop("MYSQL_DATABASE_URL", None)
        else:
            os.environ["MYSQL_DATABASE_URL"] = orig_mysql_database_url
        importlib.reload(nws_config)
