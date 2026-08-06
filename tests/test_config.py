"""Tests for app/config.py - verifies .env loading works."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

os.environ['KALSHI_API_KEY'] = 'test_key'
os.environ['KALSHI_PRIVATE_KEY_PATH'] = './test_key.pem'
os.environ['MYSQL_DATABASE_URL'] = '******localhost:3306/test'
os.environ['TRADING_MODE'] = 'PAPER'
os.environ['BUY_TRIGGER_PRICE_LOW'] = '0.82'
os.environ['BUY_TRIGGER_PRICE_HIGH'] = '0.83'
os.environ['HEDGE_TRIGGER_PRICE'] = '0.48'
os.environ['STOP_LOSS_PRICE_ASK'] = '0.35'
os.environ['STOP_LOSS_PRICE_BID'] = '0.30'
os.environ['INITIAL_CONTRACT_COUNT'] = '1'
os.environ['MINIMUM_SPREAD'] = '0.04'
os.environ['MONITOR_START_PRICE'] = '0.80'
os.environ['SPREAD_MONITOR_PRICE'] = '0.90'
os.environ['DRY_RUN'] = 'true'


class TestAppConfig:

    def test_from_env_loads_correctly(self):
        import pytest
        pytest.importorskip("pydantic_settings")
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.kalshi_api_key == 'test_key'
        assert cfg.trading_mode == 'PAPER'
        assert cfg.buy_trigger_price_low == 82
        assert cfg.buy_trigger_price_high == 83
        assert cfg.hedge_trigger_price == 48
        assert cfg.stop_loss_price_ask == 35
        assert cfg.stop_loss_price_bid == 30
        assert cfg.initial_contract_count == 1
        assert cfg.minimum_spread == 4
        assert cfg.monitor_start_price == 80
        assert cfg.spread_monitor_price == 90
        assert cfg.dry_run is True
        assert cfg.enable_fast_sl_exit is False

    def test_from_env_ignores_legacy_buy_trigger_price(self):
        import pytest
        pytest.importorskip("pydantic_settings")
        os.environ["BUY_TRIGGER_PRICE"] = "0.99"
        try:
            from app.config import AppConfig
            cfg = AppConfig.from_env()
            assert cfg.buy_trigger_price_low == 82
            assert cfg.buy_trigger_price_high == 83
        finally:
            os.environ.pop("BUY_TRIGGER_PRICE", None)

    def test_low_ticker_10pm_max_ask_parses_dollar_value(self):
        import pytest
        pytest.importorskip("pydantic_settings")
        os.environ["LOW_TICKER_10PM_MAX_ASK"] = "0.93"
        try:
            from app.config import AppConfig
            cfg = AppConfig.from_env()
            assert cfg.low_ticker_10pm_max_ask == 93
        finally:
            os.environ.pop("LOW_TICKER_10PM_MAX_ASK", None)

    def test_hedge_max_factor_loaded_as_int(self):
        """HEDGE_MAX_FACTOR=5 in env must produce int hedge_max_factor == 5 via from_env()."""
        import pytest
        pytest.importorskip("pydantic_settings")
        import os
        os.environ['HEDGE_MAX_FACTOR'] = '5'
        try:
            from app.config import AppConfig
            cfg = AppConfig.from_env()
            assert cfg.hedge_max_factor == 5
            assert isinstance(cfg.hedge_max_factor, int)
        finally:
            os.environ.pop('HEDGE_MAX_FACTOR', None)

    def test_hedge_max_factor_default_is_three(self):
        """Missing HEDGE_MAX_FACTOR must default to 3."""
        import pytest
        pytest.importorskip("pydantic_settings")
        import os
        os.environ.pop('HEDGE_MAX_FACTOR', None)
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.hedge_max_factor == 3
        assert isinstance(cfg.hedge_max_factor, int)

    def test_initial_contract_count_loaded_from_env(self):
        """Regression test: INITIAL_CONTRACT_COUNT=4 in env must produce initial_contract_count == 4."""
        import pytest
        pytest.importorskip("pydantic_settings")
        import os
        os.environ['INITIAL_CONTRACT_COUNT'] = '4'
        try:
            from app.config import AppConfig
            cfg = AppConfig.from_env()
            assert cfg.initial_contract_count == 4
            assert isinstance(cfg.initial_contract_count, int)
        finally:
            os.environ.pop('INITIAL_CONTRACT_COUNT', None)

    def test_initial_contract_count_default_is_one(self):
        """Missing INITIAL_CONTRACT_COUNT must default to 1."""
        import pytest
        pytest.importorskip("pydantic_settings")
        import os
        os.environ.pop('INITIAL_CONTRACT_COUNT', None)
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.initial_contract_count == 1
        assert isinstance(cfg.initial_contract_count, int)
        from app.config import AppConfig
        cfg = AppConfig(
            kalshi_api_key='k',
            kalshi_private_key_path='k.pem',
            mysql_database_url='******localhost:3306/test',
            trading_mode='LIVE',
            initial_contract_count=1,
            monitor_start_price=80,
            buy_trigger_price_low=82,
            buy_trigger_price_high=82,
            spread_monitor_price=90,
            minimum_spread=4,
            stop_loss_price=35,
            no_trade_tickers=set(),
        )
        assert cfg.enable_fast_sl_exit is True

    def test_sl_exit_mode_defaults_to_panic_flatten(self):
        import pytest
        pytest.importorskip("pydantic_settings")
        from app.config import AppConfig
        cfg = AppConfig(
            kalshi_api_key='k',
            kalshi_private_key_path='k.pem',
            mysql_database_url='******localhost:3306/test',
            trading_mode='PAPER',
            initial_contract_count=1,
            monitor_start_price=80,
            buy_trigger_price_low=82,
            buy_trigger_price_high=82,
            spread_monitor_price=90,
            minimum_spread=4,
            stop_loss_price=35,
            no_trade_tickers=set(),
        )
        assert cfg.sl_exit_mode == 'PANIC_FLATTEN'


class TestTradeToggles:
    """Tests for LOW_TRADES / HIGH_TRADES env-var config flags."""

    def setup_method(self):
        # Remove any leftover toggle env vars before each test
        for key in ("LOW_TRADES", "HIGH_TRADES"):
            os.environ.pop(key, None)

    def teardown_method(self):
        for key in ("LOW_TRADES", "HIGH_TRADES"):
            os.environ.pop(key, None)

    def test_defaults_to_true_when_env_vars_missing(self):
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.low_trades is True
        assert cfg.high_trades is True

    def test_yes_values_enable_both(self):
        os.environ['LOW_TRADES'] = 'yes'
        os.environ['HIGH_TRADES'] = 'yes'
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.low_trades is True
        assert cfg.high_trades is True

    def test_no_disables_low(self):
        os.environ['LOW_TRADES'] = 'no'
        os.environ['HIGH_TRADES'] = 'yes'
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.low_trades is False
        assert cfg.high_trades is True

    def test_no_disables_high(self):
        os.environ['LOW_TRADES'] = 'yes'
        os.environ['HIGH_TRADES'] = 'no'
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.low_trades is True
        assert cfg.high_trades is False

    def test_no_disables_both(self):
        os.environ['LOW_TRADES'] = 'no'
        os.environ['HIGH_TRADES'] = 'no'
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.low_trades is False
        assert cfg.high_trades is False

    def test_case_insensitive_YES(self):
        os.environ['LOW_TRADES'] = 'YES'
        os.environ['HIGH_TRADES'] = 'NO'
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.low_trades is True
        assert cfg.high_trades is False

    def test_invalid_value_defaults_to_true(self):
        """An unrecognized value must fail safe (default True) without raising."""
        os.environ['LOW_TRADES'] = 'maybe'
        os.environ['HIGH_TRADES'] = 'off'
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.low_trades is True
        assert cfg.high_trades is True

    def test_parse_yes_no_helper_directly(self):
        from app.config import _parse_trade_toggle
        assert _parse_trade_toggle("yes", "X") is True
        assert _parse_trade_toggle("YES", "X") is True
        assert _parse_trade_toggle("true", "X") is True
        assert _parse_trade_toggle("1", "X") is True
        assert _parse_trade_toggle("no", "X") is False
        assert _parse_trade_toggle("NO", "X") is False
        assert _parse_trade_toggle("false", "X") is False
        assert _parse_trade_toggle("0", "X") is False
        assert _parse_trade_toggle(None, "X") is True
        assert _parse_trade_toggle("", "X") is True
        assert _parse_trade_toggle("garbage", "X") is True  # fail safe

    def test_manage_external_positions_defaults_false(self):
        os.environ.pop("MANAGE_EXTERNAL_POSITIONS", None)
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.manage_external_positions is False

    def test_manage_external_positions_true_override(self):
        os.environ["MANAGE_EXTERNAL_POSITIONS"] = "true"
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.manage_external_positions is True
        os.environ.pop("MANAGE_EXTERNAL_POSITIONS", None)

    def test_instance_lock_and_log_rotation_env(self):
        os.environ["INSTANCE_LOCK_ENABLED"] = "false"
        os.environ["INSTANCE_LOCK_FILE"] = "/tmp/instance.lock"
        os.environ["INSTANCE_ID"] = "kalshi-account-a"
        os.environ["LOG_FILE"] = "logs/custom.log"
        os.environ["LOG_MAX_BYTES"] = "2048"
        os.environ["LOG_BACKUP_COUNT"] = "3"
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.instance_lock_enabled is False
        assert cfg.instance_lock_file == "/tmp/instance.lock"
        assert cfg.instance_id == "kalshi-account-a"
        assert cfg.log_file == "logs/custom.log"
        assert cfg.log_max_bytes == 2048
        assert cfg.log_backup_count == 3
        os.environ.pop("INSTANCE_LOCK_ENABLED", None)
        os.environ.pop("INSTANCE_LOCK_FILE", None)
        os.environ.pop("INSTANCE_ID", None)
        os.environ.pop("LOG_FILE", None)
        os.environ.pop("LOG_MAX_BYTES", None)
        os.environ.pop("LOG_BACKUP_COUNT", None)


class TestNoTradeTickers:
    def setup_method(self):
        os.environ.pop("NO_TRADE_TICKERS", None)

    def teardown_method(self):
        os.environ.pop("NO_TRADE_TICKERS", None)

    def test_no_trade_tickers_defaults_empty_when_missing(self):
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.no_trade_tickers == set()

    def test_no_trade_tickers_parses_csv_uppercase(self):
        os.environ["NO_TRADE_TICKERS"] = "kxlowtsea,kxhightsfo"
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.no_trade_tickers == {"KXLOWTSEA", "KXHIGHTSFO"}

    def test_no_trade_tickers_strips_spaces_and_normalizes_case(self):
        os.environ["NO_TRADE_TICKERS"] = " kxlowtsea , KXHIGHTSFO "
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.no_trade_tickers == {"KXLOWTSEA", "KXHIGHTSFO"}


class TestPmTickerCloseConfig:
    def setup_method(self):
        for key in ("LOW_PM_CLOSE_TIME", "LOW_PM_CLOSE_AMOUNT", "PM_TICKERS_CLOSE"):
            os.environ.pop(key, None)

    def teardown_method(self):
        for key in ("LOW_PM_CLOSE_TIME", "LOW_PM_CLOSE_AMOUNT", "PM_TICKERS_CLOSE"):
            os.environ.pop(key, None)

    def test_pm_close_defaults(self):
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.low_pm_close_time == "22:00"
        assert cfg.low_pm_close_amount == 93
        assert cfg.pm_tickers_close == set()

    def test_pm_close_config_parses_csv_case_insensitive(self):
        os.environ["LOW_PM_CLOSE_TIME"] = "21:45"
        os.environ["LOW_PM_CLOSE_AMOUNT"] = "95"
        os.environ["PM_TICKERS_CLOSE"] = " kxlowtchi, KXLOWTBOS, ,"
        from app.config import AppConfig
        cfg = AppConfig.from_env()
        assert cfg.low_pm_close_time == "21:45"
        assert cfg.low_pm_close_amount == 95
        assert cfg.pm_tickers_close == {"KXLOWTCHI", "KXLOWTBOS"}


class TestParseInitialContractCount:
    """Unit tests for _parse_initial_contract_count helper."""

    def test_missing_returns_default_one(self):
        from app.config import _parse_initial_contract_count
        assert _parse_initial_contract_count(None) == 1
        assert _parse_initial_contract_count("") == 1
        assert _parse_initial_contract_count("   ") == 1

    def test_integer_string_parses(self):
        from app.config import _parse_initial_contract_count
        assert _parse_initial_contract_count("4") == 4
        assert _parse_initial_contract_count("1") == 1
        assert _parse_initial_contract_count("10") == 10

    def test_float_string_truncates_with_warning(self):
        from app.config import _parse_initial_contract_count
        assert _parse_initial_contract_count("4.0") == 4
        assert _parse_initial_contract_count("4.9") == 4

    def test_zero_clamped_to_one(self):
        from app.config import _parse_initial_contract_count
        assert _parse_initial_contract_count("0") == 1

    def test_negative_clamped_to_one(self):
        from app.config import _parse_initial_contract_count
        assert _parse_initial_contract_count("-3") == 1

    def test_garbage_returns_default_one(self):
        from app.config import _parse_initial_contract_count
        assert _parse_initial_contract_count("abc") == 1
        assert _parse_initial_contract_count("?!") == 1
