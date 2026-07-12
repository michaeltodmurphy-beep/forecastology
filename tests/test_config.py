"""Tests for app/config.py - verifies .env loading works."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

os.environ['KALSHI_API_KEY'] = 'test_key'
os.environ['KALSHI_PRIVATE_KEY_PATH'] = './test_key.pem'
os.environ['MYSQL_DATABASE_URL'] = '******localhost:3306/test'
os.environ['TRADING_MODE'] = 'PAPER'
os.environ['BUY_TRIGGER_PRICE'] = '0.82'
os.environ['HEDGE_TRIGGER_PRICE'] = '0.48'
os.environ['STOP_LOSS_PRICE'] = '0.35'
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
        assert cfg.buy_trigger_price == 82
        assert cfg.hedge_trigger_price == 48
        assert cfg.stop_loss_price == 35
        assert cfg.initial_contract_count == 1
        assert cfg.minimum_spread == 4
        assert cfg.monitor_start_price == 80
        assert cfg.spread_monitor_price == 90
        assert cfg.dry_run is True
        assert cfg.enable_fast_sl_exit is False

    def test_enable_fast_sl_exit_defaults_true_for_live(self):
        import pytest
        pytest.importorskip("pydantic_settings")
        from app.config import AppConfig
        cfg = AppConfig(
            kalshi_api_key='k',
            kalshi_private_key_path='k.pem',
            mysql_database_url='******localhost:3306/test',
            trading_mode='LIVE',
            initial_contract_count=1,
            monitor_start_price=80,
            buy_trigger_price=82,
            spread_monitor_price=90,
            minimum_spread=4,
            stop_loss_price=35,
            no_trade_tickers=set(),
        )
        assert cfg.enable_fast_sl_exit is True


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
