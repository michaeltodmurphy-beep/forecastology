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
    monitor_start_price: int
    buy_trigger_price: int
    spread_monitor_price: int
    minimum_spread: int
    stop_loss_price: int
    rest_base_url: str = 'https://external-api.kalshi.com'
    ws_url: str = 'wss://external-api-ws.kalshi.com/trade-api/ws/v2'
    weather_series_prefix: str = 'KXWEATHER'
    # hedge_max_factor is REPURPOSED as the maximum number of stop-loss "doublings"
    # allowed per (series, day) for martingale recovery sizing.
    # Buy size at BUY_TRIGGER = initial_contract_count * 2**stop_loss_count.
    # Buy is allowed while stop_loss_count <= hedge_max_factor; once it exceeds
    # hedge_max_factor we stop buying that series for the rest of the day.
    # With initial_contract_count=2 and hedge_max_factor=3 -> sizes 2,4,8,16.
    hedge_max_factor: float = 3.0
    eval_price_floor: int = 5
    # DEPRECATED / UNUSED by trading logic. Kept only so existing .env files that
    # still define HEDGE_TRIGGER_PRICE / HEDGE_BUY continue to load, and so .env
    # files that OMIT them do not raise (they now have safe defaults). No code
    # references these for any trading decision after the hedge engine removal.
    hedge_trigger_price: int = 0
    hedge_buy: int = 0
    dry_run: bool = False
    held_position_price_refresh_seconds: int = 10
    stop_loss_max_unfilled_attempts: int = 3
    # Maximum bid-ask spread (in cents) at which the stop-loss is allowed to fire.
    # When the YES spread exceeds this value the bot holds rather than selling into
    # a wide, indecisive book. Set via MAX_SL_SPREAD env var (e.g. "0.20" → 20¢).
    max_sl_spread: int = 20

    @field_validator(
        'buy_trigger_price', 'spread_monitor_price', 'minimum_spread',
        'stop_loss_price', 'monitor_start_price',
        'eval_price_floor', 'hedge_trigger_price', 'hedge_buy',
        'max_sl_spread',
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
        dry_run_raw = os.getenv("DRY_RUN", "")
        dry_run = dry_run_raw.strip().lower() in {"1", "true", "yes"} if dry_run_raw else False
        return cls(dry_run=dry_run)
