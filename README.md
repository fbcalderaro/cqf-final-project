# Multi-Strategy Algorithmic Trading System

This is a robust, event-driven, and fully containerized algorithmic trading system for cryptocurrencies. It is designed for running multiple, independent trading strategies concurrently in both live and paper trading modes. The system includes modules for data ingestion, backtesting, live execution, and real-time monitoring.

## Key Features

- **Multi-Strategy Engine**: Run multiple trading strategies simultaneously, each with its own allocated capital and isolated state.
- **Live & Paper Trading**: Seamlessly switch between live trading (on Binance Testnet) and paper trading via a simple configuration setting.
- **Dockerized Environment**: The entire application stack (trading bots, database) is managed with Docker Compose for easy, one-command setup and consistent deployments.
- **Event-Driven Architecture**: Uses a real-time WebSocket stream for market data, making the system highly responsive to market events.
- **Robust Data Handling**: Includes a dedicated data ingestion service that backfills historical data and ensures data integrity by checking for gaps.
- **Comprehensive Backtesting**: A powerful backtesting engine to evaluate strategy performance over different periods (e.g., in-sample, out-of-sample).
- **Real-Time Monitoring**: Generates a live HTML dashboard and individual reports for each running strategy, showing equity curves, trades, and performance metrics.
- **Modular & Extensible**: Easily add new strategies by creating a new strategy class and defining its parameters in a central YAML configuration file.

---

## System Architecture

The system is composed of several services that run in separate Docker containers, ensuring isolation and stability.

```
┌──────────────────────────┐      ┌──────────────────────────┐
│    Trading Engine        │      │    Data Ingestor         │
│  (trader container)      │      │  (ingestor container)    │
│                          │      │                          │
│  - Strategy Runner 1     │      │  - Historical Backfill   │
│  - Strategy Runner 2     │      │  - Real-time Sync        │
│  - ...                   │      │                          │
│  - Portfolio Manager     │      │                          │
│  - Execution Handler     │      │                          │
└────────────┬─────────────┘      └────────────┬─────────────┘
             │                                │
             │           Reads/Writes         │
             ▼                                ▼
┌────────────────────────────────────────────────────────────┐
│                     PostgreSQL Database                    │
│                    (db container)                          │
│                                                            │
│ - Stores all historical 1-minute candlestick data          │
└────────────────────────────────────────────────────────────┘

┌──────────────────────────┐
│    Backtesting Engine    │
│ (backtester container)   │
│                          │
│ - Runs on-demand tests   │
│ - Reads from DB          │
│ - Generates reports      │
└────────────┬─────────────┘
             │
             │             Reads
             ▼
 (Connects to the same DB)
```

---

## Project Structure

```
cqf-final-project/
├── output/                     # Generated reports (backtests, live monitoring)
├── trading_system/
│   ├── config/
│   │   └── config.yaml         # Main configuration file for all strategies and system params
│   ├── engine/                 # Core components: portfolio, execution, monitoring
│   ├── strategies/             # Strategy logic files (e.g., momentum, mean reversion)
│   ├── utils/                  # Helper scripts and database utilities
│   ├── backtest.py             # The backtesting engine script
│   ├── data_ingestion.py       # The data ingestion and sync script
│   └── trader.py               # The main live/paper trading application
├── .env                        # Stores secret API keys (you must create this)
├── compose.yaml                # Docker Compose file defining all services
├── backtester.Dockerfile       # Docker build instructions for the backtester
├── ingestor.Dockerfile         # Docker build instructions for the data ingestor
└── trader.Dockerfile           # Docker build instructions for the trading engine
```

---

## Getting Started

Follow these steps to set up and run the entire trading system.

### Prerequisites

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/) (usually included with Docker Desktop)

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd cqf-final-project
```

### 2. Create the Environment File

The system needs your Binance Testnet API keys to run in live mode. Create a file named `.env` in the project root and add your keys.

**File: `.env`**
```
BINANCE_KEY_TEST=your_binance_testnet_api_key
BINANCE_SECRET_TEST=your_binance_testnet_secret_key
```

### 3. Build and Run the Services

Use Docker Compose to build the images and start all the services (`db`, `trader`, `ingestor`) in the background.

```bash
docker compose up --build -d
```

- `--build`: Builds the Docker images from the Dockerfiles.
- `-d`: Runs the containers in detached mode (in the background).

Your entire trading system is now running!

### 4. Check the Logs

You can view the real-time logs from all services or a specific one.

```bash
# View logs from all running containers
docker compose logs -f

# View logs from only the trading engine
docker compose logs -f trader

# View logs from only the data ingestor
docker compose logs -f ingestor
```

---

## Usage

### Configuring Strategies

All system behavior is controlled by `trading_system/config/config.yaml`. To activate, deactivate, or tune a strategy, simply edit this file.

- **To activate a strategy**: Uncomment its definition in the `strategies` list and set its `cash_allocation_pct`.
- **To deactivate a strategy**: Comment out its definition.

After changing the config, restart the `trader` service to apply the changes:
```bash
docker compose up -d --force-recreate trader
```

### Backtesting

The backtester runs as a one-off task using `docker compose run`. It reads the strategies defined in `config.yaml` and tests them against the historical data in the database.

```bash
# Run a backtest on the 'in_sample' period
docker compose run --rm backtester --period in_sample

# Run a backtest on the 'out_of_sample' period
docker compose run --rm backtester --period out_of_sample
```

- `--rm`: Automatically removes the container after the backtest is complete.
- Backtest reports are saved to the `output/backtest/` directory.

### Monitoring

The system generates two types of reports in the `output/` directory:

1.  **Live Monitoring Reports**: Found in `output/live_monitoring/`.
    - `dashboard.html`: A master dashboard that summarizes the performance of all running strategies and the master account.
    - `live_<StrategyName>_... .html`: A detailed, auto-refreshing report for each individual strategy.
    - `live_<StrategyName>_... .json`: A machine-readable summary file for each strategy.

2.  **Backtest Reports**: Found in `output/backtest/`.
    - `comparison_report_... .html`: A summary comparing the performance of all tested strategies.
    - `..._individual_report.html`: A detailed report for a single strategy run (if enabled in `config.yaml`).

---

## Development

### Adding a New Strategy

1.  **Create the Strategy File**: Create a new Python file in `trading_system/strategies/`.
2.  **Implement the Strategy Class**: Your new class must inherit from `Strategy` (from `base_strategy.py`) and implement the `name`, `initialize`, and `generate_signals` methods.
3.  **Configure in `config.yaml`**: Add a new entry to the `strategies` list in `config.yaml`, specifying the `name`, `class`, `module`, `asset`, `timeframe`, and any `params` your strategy needs.
4.  **Test It**: Run a backtest to ensure your new strategy works as expected.

---

## Disclaimer

This software is for educational and research purposes only. It is not financial advice. Trading cryptocurrencies involves substantial risk and may not be suitable for all investors. Use this software at your own risk. The authors and contributors are not responsible for any financial losses.

---