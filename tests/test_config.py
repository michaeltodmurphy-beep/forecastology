"""Tests for app/config.py — verifies .env loading works."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

os.environ['KALSHI_API_KEY'] = 'test_key'
os.environ['KALSHI_PRIVATE_KEY_PATH'] = './test_key.pem'
os.environ['MYSQL_DATABASE_URL'] = 'mysql+aiomysql://user:pass@localhost:3306/test'
os.environ['TRADING_MODE'] = 'PAPER'
os.environ['BUY_TRIGGER_PRICE'] = '0.82'
os.environ['HEDGE_TRIGGER_PRICE'] = '0.48'
os.environ['STOP_LOSS_PRICE'] = '0.35'
os.environ['INITIAL_CONTRACT_COUNT'] = '1'
os.environ['MINIMUM_SPREAD'] = '0.04'
os.environ['MONITOR_START_PRICE'] = '0.80'
os.environ['SPREAD_MONITOR_PRICE'] = '0.90'


class TestAppConfig:

    def test_from_env_loads_correctly(self):
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
