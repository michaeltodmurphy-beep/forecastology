from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal
import os
from dotenv import load_dotenv
from pydantic import field_validator, model_validator

load_dotenv()


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    kalshi_api_key: str = ''
    kalshi_private_key_path: str = 'kalshi_private_key.pem'
    mysql_database_url: str = 'mysql+aiomysql://user:pass@localhost:3306/forecastology'
    trading_mode: Literal['PAPER', 'LIVE'] = 'PAPER'
    initial_contract_count: int = 1
    monitor_start_price: int = 80
    buy_trigger_price: int = 85
    spread_monitor_price: int = 90
    minimum_spread: int = 4
    hedge_trigger_price: int = 50
    stop_loss_price: int = 25
    rest_base_url: str = 'https://external-api.kalshi.com'
    ws_url: str = 'wss://external-api-ws.kalshi.com/trade-api/ws/v2'
    weather_series_prefix: str = 'KXWEATHER'

    @field_validator(
        'buy_trigger_price', 'spread_monitor_price', 'minimum_spread',
        'hedge_trigger_price', 'stop_loss_price', 'monitor_start_price',
        mode='before'
    )
    @classmethod
    def convert_dollars_to_cents(cls, v):
        """Convert dollar-formatted values from .env to integer cents.
        .env values come as strings like '0.82' -> 82 cents.
        Hardcoded default ints (e.g. 85) are already in cents and left as-is.
        """
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return 0
            # Always treat string inputs as dollars, convert to cents
            return int(float(v) * 100)
        # Already an int or float — it's already in cents
        return int(v)

    @model_validator(mode='after')
    def normalize_trading_mode(self):
        if self.trading_mode:
            self.trading_mode = self.trading_mode.upper()
        return self

    @classmethod
    def from_env(cls) -> 'AppConfig':
        """Load config from .env file (or environment variables).
        Prices in .env may be in dollar format (e.g. 0.85) or already in cents.
        Field validators convert them to integer cents automatically.
        """
        return cls()

