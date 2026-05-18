# Bot Service

AI-driven automated trading bot engine for the broker platform.

## Overview

Activates and manages per-user algorithmic trading bots that analyze real-time market data, dynamically select from four trading strategies, and execute orders through the broker API — all while enforcing multi-layered risk controls. Bot sessions survive service restarts via encrypted JWT storage and Alembic-managed PostgreSQL state.

## Tech Stack

- **Python 3.12** + **FastAPI 0.115** + **uvicorn**
- **PostgreSQL** via SQLAlchemy 2.0 (asyncpg) + Alembic migrations
- **cryptography (Fernet)** — symmetric encryption of stored user JWTs
- **PyJWT 2.10** — JWT validation
- **HTTPX 0.28** — async HTTP client with retry logic (broker API calls)
- **websockets 13.1** — real-time price feed from market-notifications
- **aiokafka 0.12** — Kafka producer for activity logging
- **Pydantic v2** + pydantic-settings

## Project Structure

```
bot-service/
├── app/
│   ├── main.py                    # FastAPI app, lifespan, session restore
│   ├── config.py                  # Pydantic settings
│   ├── database.py                # SQLAlchemy async engine
│   ├── security.py                # JWT validation, internal token checks
│   ├── models/bot.py              # ORM: BotSession, BotTrade, BotDecisionLog, BotEventLog
│   ├── schemas/
│   │   ├── bot.py                 # Response schemas
│   │   └── admin.py               # Admin schemas
│   ├── routers/
│   │   ├── bots.py                # User endpoints
│   │   └── admin_bots.py          # Admin endpoints (X-Internal-Token)
│   └── services/
│       ├── bot_engine.py          # Main 1-second tick loop
│       ├── bot_manager.py         # asyncio task lifecycle
│       ├── broker_client.py       # HTTP client to broker API
│       ├── crypto.py              # Fernet encrypt/decrypt
│       ├── risk_manager.py        # Daily loss, drawdown, position size gates
│       ├── strategy.py            # Technical indicators (EMA, momentum, volatility, pressure)
│       ├── strategy_selector.py   # Priority-based strategy selection
│       ├── activity_logger.py     # Decision + event log writers
│       ├── kafka_producer.py      # Kafka publishing
│       ├── runtime_state.py       # In-memory session state
│       ├── ws_feed.py             # WebSocket client to market-notifications
│       └── strategies/
│           ├── base.py            # MarketState & TradeSignal dataclasses
│           ├── event_driven.py    # React to market events (SECTOR_SLUMP, etc.)
│           ├── martingale.py      # Martingale + EMA ladder strategy
│           └── grid_trading.py    # Symmetric limit-order grid strategy
├── alembic/versions/              # 4 migration versions
├── .env.example
├── alembic.ini
├── requirements.txt
└── Dockerfile
```

## Trading Strategies

Strategy is selected every tick (1 second) based on market conditions:

| Priority | Strategy | Trigger Condition |
|----------|---------|-------------------|
| 1 | **Event-Driven** | Active market event with magnitude ≥ 1.2 |
| 2 | **Martingale + EMA** | Momentum > 0.4 AND EMA signal ≠ NONE |
| 3 | **Grid Trading** | Volatility > 2% AND pressure neutral (|ratio| < 0.2) |
| 4 | **Hold** | Fallback — preserve capital |

**Event-Driven:** Buys on BULL_RUN/SECTOR_BOOM (1% × magnitude of cash), sells to close on BEAR_CRASH/SECTOR_SLUMP. STOCK_SHOCK triggers a 0.5% buy with a 1% stop-loss.

**Martingale + EMA:** 3-level position ladder (1% → 2% → 4% of portfolio). Exit on +3% profit, -3% hard stop-loss, 60-tick timeout, or momentum death (5 ticks < 0.3).

**Grid Trading:** 5 symmetric buy/sell LIMIT levels, 2% total grid width. Dissolves on volatility drop, pressure spike, or trend detection.

## Technical Indicators

Calculated every tick from the WebSocket price buffer:

| Indicator | Formula | Key threshold |
|-----------|---------|---------------|
| **Momentum** | `(up_ticks - down_ticks) / total_ticks` over last 5 prices | > 0.4 enables martingale |
| **Volatility** | `std_dev(% price changes)` over last 10 prices | > 2% enables grid |
| **Pressure** | `(bid_vol - ask_vol) / total_vol` | |ratio| < 0.2 = balanced |
| **EMA Signal** | 5/20 crossover with ≥ 0.06% separation | BUY / SELL / NONE |

## Risk Management

| Gate | Default | Behavior |
|------|---------|---------|
| Daily loss limit | 5% of starting balance | Skip all signals this tick |
| Max drawdown | 10% from equity peak | Skip all signals this tick |
| Max open orders | 3 | Skip BUY signals only |
| Martingale hard stop | -3% from entry (Level 3) | Close position immediately |

## API Endpoints

### User Endpoints (require `Authorization: Bearer <token>` + `X-User-Id` header)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/bots/activate` | Activate a trading bot for a symbol |
| `DELETE` | `/api/bots/deactivate` | Deactivate active bot |
| `GET` | `/api/bots/status` | Check bot status |
| `GET` | `/api/bots/performance` | Trade history and stats |
| `GET` | `/api/health` | Health check |

**Activate request body:**
```json
{
  "symbol": "AAPL",
  "strategy_config": {
    "martingale_level1_pct": 0.01,
    "martingale_profit_target": 0.03,
    "grid_levels": 5
  }
}
```

### Admin Endpoints (require `X-Internal-Token` header)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/internal/admin/bots` | List all bot sessions |
| `GET` | `/internal/admin/bots/{session_id}` | Session details, metrics, holdings |
| `GET` | `/internal/admin/bots/{session_id}/decisions` | Paginated decision log |
| `GET` | `/internal/admin/bots/{session_id}/events` | Paginated event log |
| `POST` | `/internal/admin/bots/{session_id}/close-position` | Force-close position |
| `POST` | `/internal/admin/bots/{session_id}/restart` | Force-restart session |

## Configuration

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL async connection string |
| `BROKER_API_URL` | API Gateway or broker base URL |
| `JWT_SECRET` | Shared JWT signing secret (HS256, 256-bit hex) |
| `BOT_FERNET_KEY` | Fernet key for encrypting stored JWTs in DB |
| `INTERNAL_SERVICE_TOKEN` | Service-to-service auth token (default: `change-me-in-production`) |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker (default: `redpanda:9092`) |
| `KAFKA_ENABLED` | Enable Kafka activity publishing (default: `true`) |
| `DECISION_LOG_THROTTLE_SECS` | Min seconds between decision log writes (default: `10`) |
| `DECISION_LOG_RETENTION_DAYS` | Auto-prune decision logs older than N days (default: `14`) |
| `EVENT_LOG_RETENTION_DAYS` | Auto-prune event logs older than N days (default: `90`) |

**Generating secrets:**
```bash
# JWT_SECRET (256-bit hex)
python -c "import secrets; print(secrets.token_hex(32))"

# BOT_FERNET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Getting Started

### Local Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # Fill in secrets
uvicorn app.main:app --host 0.0.0.0 --port 8002 --reload
```

Database migrations run automatically on startup.

### Docker

```bash
docker build -t bot-service .
docker run -p 8002:8002 --env-file .env bot-service
```

## Session Persistence

On service restart, the app queries all non-deactivated sessions from the DB, decrypts stored JWTs (Fernet), validates expiry, and re-spawns `asyncio.Task` instances. Expired or unreadable tokens are silently marked as `error` status.

## Database Schema

| Table | Description |
|-------|-------------|
| `bot_sessions` | Active/past bot instances — UUID PK, user_id, symbol, Fernet-encrypted JWT, status, position state |
| `bot_trades` | Executed trades with realized P&L |
| `bot_decision_logs` | Tick-level indicator snapshots and strategy selections |
| `bot_event_logs` | Lifecycle events, errors, and admin actions (severity: DEBUG→CRITICAL) |

## Kafka Topics Published

| Topic | Content |
|-------|---------|
| `bot.activity.decisions` | Tick-level indicator snapshots |
| `bot.activity.events` | Lifecycle and error events |
| `bot.activity.lifecycle` | Bot start/stop/pause events |

## Deployment

GitHub Actions CI/CD pushes to GHCR on push to `main`:
```
ghcr.io/lynx-spring-practice-team1/bot-service:latest
```
