-- db/init_schema.sql
-- MySQL schema for forecastology automated trading application
-- Run this once to create the database and tables, or let SQLAlchemy create them automatically.

CREATE DATABASE IF NOT EXISTS forecastology CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE forecastology;

CREATE TABLE IF NOT EXISTS streamed_trades (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    market_ticker VARCHAR(200) NOT NULL,
    event_ticker VARCHAR(200) DEFAULT NULL,
    series_ticker VARCHAR(200) DEFAULT NULL,
    price INT NOT NULL COMMENT 'Price in cents ($0.01 increments)',
    quantity INT NOT NULL,
    side VARCHAR(10) DEFAULT NULL,
    trade_ts DATETIME NOT NULL,
    ingested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_market_ticker (market_ticker),
    INDEX idx_trade_ts (trade_ts)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS streamed_tickers (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    market_ticker VARCHAR(200) NOT NULL,
    last_price INT DEFAULT NULL,
    yes_bid INT DEFAULT NULL,
    yes_ask INT DEFAULT NULL,
    volume BIGINT DEFAULT NULL,
    open_interest BIGINT DEFAULT NULL,
    ticker_ts DATETIME NOT NULL,
    ingested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_market_ticker (market_ticker)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS executed_trades (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    market_ticker VARCHAR(200) NOT NULL,
    action ENUM('BUY','SELL','HEDGE','STOP_LOSS') NOT NULL,
    side VARCHAR(10) NOT NULL,
    price INT NOT NULL,
    quantity INT NOT NULL,
    total_cost_cents INT NOT NULL,
    trade_mode VARCHAR(10) NOT NULL,
    status ENUM('PENDING','FILLED','PARTIAL','CANCELLED','REJECTED') NOT NULL,
    kalshi_order_id VARCHAR(100) DEFAULT NULL,
    executed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    notes TEXT DEFAULT NULL,
    INDEX idx_market_ticker (market_ticker)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS positions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    market_ticker VARCHAR(200) NOT NULL UNIQUE,
    event_ticker VARCHAR(200) DEFAULT NULL,
    series_ticker VARCHAR(200) DEFAULT NULL,
    side VARCHAR(10) NOT NULL,
    quantity INT NOT NULL DEFAULT 0,
    avg_entry_price INT DEFAULT NULL,
    hedge_market_ticker VARCHAR(200) DEFAULT NULL,
    hedge_quantity INT DEFAULT NULL,
    last_price INT DEFAULT NULL,
    unrealized_pnl INT DEFAULT NULL,
    position_ts DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_market_ticker (market_ticker)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    cash_balance_cents BIGINT NOT NULL,
    total_positions INT NOT NULL,
    total_risk_cents BIGINT NOT NULL,
    snapshot_ts DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS event_windows (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    market_ticker VARCHAR(200) NOT NULL UNIQUE,
    event_ticker VARCHAR(200) DEFAULT NULL,
    series_ticker VARCHAR(200) DEFAULT NULL,
    phase VARCHAR(50) NOT NULL,
    bracket_label VARCHAR(50) DEFAULT NULL,
    last_price INT DEFAULT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_market_ticker (market_ticker)
) ENGINE=InnoDB;
