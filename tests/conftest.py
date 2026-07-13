import os

import pytest

from app.config import AppConfig


_LEAKY_APP_CONFIG_ENV_VARS = (
    "MINIMUM_SPREAD",
    "BUY_TRIGGER_PRICE",
    "MONITOR_START_PRICE",
    "SPREAD_MONITOR_PRICE",
    "STOP_LOSS_PRICE",
    "INITIAL_CONTRACT_COUNT",
    "HEDGE_MAX_FACTOR",
    "max_sl_spread",
    "MAX_SL_SPREAD",
    "SL_SPREAD_HOLD_MAX_SECONDS",
    "SL_EXIT_MODE",
    "ENABLE_FAST_SL_EXIT",
    "TRADING_MODE",
    "DRY_RUN",
    "LOW_TRADES",
    "HIGH_TRADES",
    "MANAGE_EXTERNAL_POSITIONS",
    "ENABLE_LOCAL_SETTLE_GATE",
    "DEFAULT_ENTRY_START_LOCAL",
    "PHOENIX_ENTRY_START_LOCAL",
    "NO_TRADE_TICKERS",
    "HEDGE_TRIGGER_PRICE",
    "HEDGE_BUY",
)

_INITIAL_ENV_VALUES = {
    env_var: os.environ.get(env_var)
    for env_var in _LEAKY_APP_CONFIG_ENV_VARS
}

_TEST_CONFIG_MODULE_ENV_VARS = {
    "TRADING_MODE",
    "BUY_TRIGGER_PRICE",
    "MONITOR_START_PRICE",
    "SPREAD_MONITOR_PRICE",
    "STOP_LOSS_PRICE",
    "INITIAL_CONTRACT_COUNT",
    "MINIMUM_SPREAD",
    "DRY_RUN",
    "HEDGE_TRIGGER_PRICE",
}


@pytest.fixture(autouse=True)
def isolate_app_config_from_ambient_env(monkeypatch, request):
    monkeypatch.setitem(AppConfig.model_config, "env_file", None)

    if request.node.fspath.basename == "test_config.py":
        for env_var in _LEAKY_APP_CONFIG_ENV_VARS:
            if env_var not in _TEST_CONFIG_MODULE_ENV_VARS:
                monkeypatch.delenv(env_var, raising=False)
        return

    for env_var in _LEAKY_APP_CONFIG_ENV_VARS:
        if os.environ.get(env_var) == _INITIAL_ENV_VALUES[env_var]:
            monkeypatch.delenv(env_var, raising=False)
