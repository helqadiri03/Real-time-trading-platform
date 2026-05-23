# Real-time Trading Platform (Arquitectura Kappa)

This repository contains the implementation of a full Kappa Architecture for real-time processing of financial data via the Finnhub WebSocket API, as per the project requirements. The ultimate objective is to provide a real-time market monitor capable of generating financial charts (candlesticks), tracking volumes, and triggering Native Grafana Alerts based on price limits.

## 1. Architecture & Technologies
This completely containerized solution orchestrates the following components:

- **Ingestion (Python Producer):** A Python script connects to the raw [Finnhub WebSocket](https://finnhub.io/docs/api/websocket-trades). It subscribes to multiple assets (`BINANCE:BTCUSDT`, `BINANCE:ETHUSDT`, `BINANCE:SOLUSDT`) and pushes real-time trades into an Apache Kafka topic called `trades`.
- **Message Broker (Kafka + Zookeeper):** Acts as the core centralized Event Log to buffer live streaming trades.
- **Stream Processing (Apache Flink / PyFlink):** A highly scalable Flink application consumes the `trades` topic and applies a 1-minute `Tumbling Window`. It aggregates incoming trades to calculate Open, High, Low, Close (OHLC) values alongside total transaction volume for each window. It is fully automated using a dedicated Flink job submitter container.
- **Data Storage (TimescaleDB):** We use a PostgreSQL database optimized with TimescaleDB hypertables (`v14`). The backend contains a robust `UNIQUE (time, symbol)` constraint along with an engineered `upsert` mechanism within Flink to avoid any duplicates in case of service restarts.
- **Visualization & Alerting (Grafana):** Grafana is connected safely to TimescaleDB. It features a fully provisioned, dynamic dashboard (Real-Time Trading Dashboard) displaying:
    - **Candlestick Charts** (Evolución de Precio OHLC)
    - **Volume Histograms** (Volumen de transacciones por minuto)
    - **Asset current price and native Grafana alerts** (Alerts for BTCUSDT exceeding price limits).

## 2. How to Run (Entorno Local)

You do not need to install local environments. Everything is dockerized.
Ensure you have Docker and Docker Compose installed.

To deploy the entire structure natively, execute from the repository root:
```bash
docker compose -f docker/compose.yaml up -d --build
```

**What this will do:**
1. Stand up Zookeeper, Kafka, and Redpanda (for visually inspecting Kafka themes).
2. Start TimescaleDB and apply `database/init.sql` & `database/schema.sql`.
3. Stand up Flink JobManager & TaskManager.
4. Launch the `flink-job-submitter`, seamlessly compiling and deploying the PyFlink job in the cluster.
5. Launch the `producer` connecting natively and downloading the stream of data via Websocket into Kafka.
6. Prepare Grafana and automatically preload Dashboards + Data Sources + Native alerting without manual mapping.

**Accessing Services:**
- **Grafana Dashboard:** `http://localhost:3000` (User: `admin`, Password: `admin`)
- **Redpanda Console (Kafka UI):** `http://localhost:8080`
- **Flink UI:** `http://localhost:8081`

The live dashboard will continuously update with 1-minute candlestick and volume aggregations reflecting the ingested real-time market data ticks.
