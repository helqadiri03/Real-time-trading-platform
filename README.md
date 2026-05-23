# Real-time Trading Platform (Arquitectura Kappa)

This repository contains the implementation of a full Kappa Architecture for real-time processing of financial data via the Finnhub WebSocket API. This solution demonstrates enterprise-grade stream processing with durable event storage, fault-tolerant aggregation, and real-time visualization of market data.

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Data Flow Diagram](#data-flow-diagram)
- [Technology Stack](#technology-stack)
- [Getting Started](#getting-started)
- [Project Structure](#project-structure)
- [Key Features](#key-features)
- [System Requirements](#system-requirements)
- [Troubleshooting](#troubleshooting)

## Architecture Overview

This completely containerized solution orchestrates the following components:

- **Ingestion (Python Producer):** A Python script connects to the raw [Finnhub WebSocket](https://finnhub.io/docs/api/websocket-trades). It subscribes to multiple assets (`BINANCE:BTCUSDT`, `BINANCE:ETHUSDT`, etc.) and ingests tick-level trade events in real-time.
  
- **Message Broker (Kafka + Zookeeper):** Acts as the core centralized Event Log to buffer live streaming trades. Kafka ensures durable, ordered, and fault-tolerant message handling with configurable retention policies.
  
- **Stream Processing (Apache Flink / PyFlink):** A highly scalable Flink application consumes the `trades` topic and applies a 1-minute Tumbling Window. It aggregates incoming trades to calculate OHLC (Open, High, Low, Close) and trading volume metrics.
  
- **Data Storage (TimescaleDB):** We use a PostgreSQL database optimized with TimescaleDB hypertables (v14). The backend contains a robust `UNIQUE (time, symbol)` constraint and engineered indexes for sub-millisecond query performance on time-series data.
  
- **Visualization & Alerting (Grafana):** Grafana is connected safely to TimescaleDB. It features a fully provisioned, dynamic dashboard (Real-Time Trading Dashboard) displaying:
    - **Candlestick Charts** (Evolución de Precio OHLC)
    - **Volume Histograms** (Volumen de transacciones por minuto)
    - **Asset Current Price** with real-time updates
    - **Native Grafana Alerts** (Alerts for BTCUSDT exceeding configurable price limits)

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         REAL-TIME TRADING PLATFORM FLOW                             │
└─────────────────────────────────────────────────────────────────────────────────────┘

                                    ┌──────────────────┐
                                    │   Finnhub API    │
                                    │   WebSocket      │
                                    └────────┬─────────┘
                                             │
                                    (Trade Events Stream)
                                             │
                          ┌──────────────────▼─────────────────┐
                          │    INGESTION LAYER                 │
                          │  (producer/producer.py)            │
                          │  - Connect to WebSocket            │
                          │  - Parse trade ticks               │
                          │  - Publish to Kafka topic          │
                          └──────────────────┬─────────────────┘
                                             │
                                    (Raw Trade Events)
                                             │
                    ┌────────────────────────▼────────────────────────┐
                    │   MESSAGE BROKER LAYER (Event Log)              │
                    │   Kafka + Zookeeper                             │
                    │  ┌─────────────────────────────────────────┐   │
                    │  │ Topic: trades                           │   │
                    │  │ - Partitioned by symbol                │   │
                    │  │ - Configurable retention policy         │   │
                    │  │ - Replication factor: 3                │   │
                    │  └─────────────────────────────────────────┘   │
                    │                                                  │
                    │  UI: Redpanda Console (localhost:8080)         │
                    └────────────────────┬─────────────────────────────┘
                                         │
                                (Durable Trade Stream)
                                         │
                    ┌────────────────────▼──────────────────────┐
                    │  STREAM PROCESSING LAYER                  │
                    │  (streaming/flink_job.py)                 │
                    │  ┌──────────────────────────────────────┐ │
                    │  │ Flink Application                    │ │
                    │  │ - Consume from Kafka                 │ │
                    │  │ - 1-minute Tumbling Windows          │ │
                    │  │ - Calculate OHLC metrics             │ │
                    │  │ - Compute volume aggregations        │ │
                    │  │ - Sink to TimescaleDB                │ │
                    │  └──────────────────────────────────────┘ │
                    │                                             │
                    │  UI: Flink WebUI (localhost:8081)          │
                    └────────────────────┬──────────────────────┘
                                         │
                          (Aggregated Candlestick Data)
                                         │
                    ┌────────────────────▼──────────────────────┐
                    │   STORAGE LAYER                           │
                    │   TimescaleDB / PostgreSQL v14            │
                    │  ┌──────────────────────────────────────┐ │
                    │  │ Table: candlesticks                  │ │
                    │  │ - time (TIMESTAMPTZ)                 │ │
                    │  │ - symbol (TEXT)                      │ │
                    │  │ - open (NUMERIC)                     │ │
                    │  │ - high (NUMERIC)                     │ │
                    │  │ - low (NUMERIC)                      │ │
                    │  │ - close (NUMERIC)                    │ │
                    │  │ - volume (BIGINT)                    │ │
                    │  │ - UNIQUE(time, symbol)               │ │
                    │  └──────────────────────────────────────┘ │
                    │                                             │
                    │  Optimized Hypertables for Time-Series    │
                    │  Sub-millisecond Query Performance         │
                    └────────────────────┬──────────────────────┘
                                         │
                         (Real-time Candlestick & Volume)
                                         │
                    ┌────────────────────▼──────────────────────┐
                    │  VISUALIZATION & ALERTING LAYER           │
                    │  Grafana Dashboard                        │
                    │  ┌──────────────────────────────────────┐ │
                    │  │ Real-Time Trading Dashboard          │ │
                    │  │ - OHLC Candlestick Charts            │ │
                    │  │ - Volume Histograms (1-min bars)     │ │
                    │  │ - Current Price Panels                │ │
                    │  │ - Alert Rules (Price Thresholds)     │ │
                    │  │ - Auto-refresh (5-10 seconds)        │ │
                    │  └──────────────────────────────────────┘ │
                    │                                             │
                    │  Access: http://localhost:3000             │
                    │  Credentials: admin/admin                 │
                    └─────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────┐
│  KAPPA ARCHITECTURE PRINCIPLES                                                       │
│  ✓ Single Stream Processing Pipeline                                                │
│  ✓ Recomputable Results (Event Log Retained)                                        │
│  ✓ Fault Tolerance via Durable Message Broker                                      │
│  ✓ Stateless Processing (Flink handles state management)                           │
│  ✓ Real-time Aggregation (1-minute tumbling windows)                               │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Data Source** | Finnhub WebSocket API | Real-time financial market data |
| **Ingestion** | Python 3.9+ | Trade event producer |
| **Message Broker** | Apache Kafka | Durable event log with 3x replication |
| **Stream Processing** | Apache Flink + PyFlink | 1-minute window aggregation |
| **Time-Series DB** | TimescaleDB (PostgreSQL v14) | Optimized for OHLC candlestick storage |
| **Visualization** | Grafana | Real-time dashboards and alerting |
| **Orchestration** | Docker + Docker Compose | Container management and networking |
| **Monitoring** | Redpanda Console | Kafka topic inspection UI |

---

## Getting Started

### System Requirements

- **Docker**: v20.10 or later
- **Docker Compose**: v1.29 or later
- **Memory**: 8GB recommended (4GB minimum)
- **Disk Space**: 20GB for containers and data
- **Finnhub API Key**: Get one free at [finnhub.io](https://finnhub.io/)

### Quick Start

You do not need to install local environments. Everything is dockerized.

To deploy the entire structure, execute from the repository root:

```bash
docker compose -f docker/compose.yaml up -d --build
```

#### What This Command Does:

1. **Stands up Zookeeper** - Kafka coordinator
2. **Initializes Kafka cluster** - Main message broker for trade events
3. **Launches Redpanda Console** - Web UI for inspecting Kafka topics (optional but helpful)
4. **Starts TimescaleDB** - Applies `database/init.sql` and `database/schema.sql` for hypertable setup
5. **Deploys Flink Cluster** - JobManager & TaskManager instances
6. **Submits PyFlink Job** - Compiles and deploys the stream processing job
7. **Launches Producer** - Connects to Finnhub WebSocket and streams trades into Kafka
8. **Provisions Grafana** - Pre-loads dashboards, data sources, and alert rules

### Accessing Services

Once deployment completes (typically 30-60 seconds), services are available at:

| Service | URL | Credentials |
|---------|-----|-------------|
| **Grafana Dashboard** | http://localhost:3000 | `admin` / `admin` |
| **Flink WebUI** | http://localhost:8081 | None required |
| **Redpanda Console** | http://localhost:8080 | None required |
| **TimescaleDB** | `localhost:5432` | See `docker/compose.yaml` |

### Stopping the Platform

```bash
docker compose -f docker/compose.yaml down -v
```

The `-v` flag removes all volumes; omit it to preserve data.

---

## Project Structure

```
Real-time-trading-platform/
├── producer/
│   ├── producer.py              # Finnhub WebSocket consumer, publishes to Kafka
│   └── requirements.txt
├── streaming/
│   ├── flink_job.py             # PyFlink application (1-min tumbling window aggregation)
│   └── requirements.txt
├── database/
│   ├── init.sql                 # TimescaleDB initialization
│   ├── schema.sql               # Hypertable and index definitions
│   └── data/                    # Persistent volume mount
├── docker/
│   ├── compose.yaml             # Main Docker Compose orchestration
│   ├── Dockerfile.producer      # Producer container image
│   ├── Dockerfile.flink-job     # Flink job submitter image
│   └── Dockerfile.grafana       # Grafana provisioning (optional)
├── grafana/
│   ├── dashboards/              # Pre-built Grafana dashboards (JSON)
│   ├── provisioning/            # Grafana data sources and alert configurations
│   └── alerts/                  # Alert rule definitions
├── .env                         # Environment variables (Finnhub API key, etc.)
├── .env.example                 # Template for environment variables
├── README.md                    # This file
└── LICENSE
```

---

## Key Features

### 1. **Real-Time Data Ingestion**
   - Connects directly to Finnhub's WebSocket for live trade ticks
   - Supports multiple asset subscriptions (crypto, stocks)
   - Automatic reconnection on network failure

### 2. **Fault-Tolerant Message Broker**
   - Apache Kafka with 3x replication
   - Configurable retention policies
   - Enables event log replay for recomputation

### 3. **Scalable Stream Processing**
   - PyFlink for distributed, stateful computation
   - 1-minute tumbling windows for OHLC aggregation
   - Horizontal scaling via additional Flink TaskManagers

### 4. **Time-Series Optimized Storage**
   - TimescaleDB hypertables for compression
   - Automatic partitioning by time interval
   - Sub-millisecond query response times
   - Unique constraints prevent duplicate candlesticks

### 5. **Production-Ready Visualization**
   - Grafana dashboards with auto-refresh
   - Multiple chart types (candlestick, volume, price)
   - Native alerting for price breaches
   - Pre-provisioned data sources and dashboards

### 6. **Fully Containerized**
   - Single `docker compose up` deployment
   - Zero external dependencies (except Finnhub API)
   - Reproducible environments across machines

---

## Data Pipeline in Detail

### Step 1: Ingestion (Producer)
```
Finnhub WebSocket Feed
    ↓
[Trade Event: {symbol, price, size, timestamp}]
    ↓
Kafka Topic: "trades"
```

### Step 2: Message Brokering (Kafka)
- **Topic**: `trades`
- **Partitioning**: By symbol (ensures ordering per asset)
- **Retention**: 7 days (configurable in `docker/compose.yaml`)
- **Replication Factor**: 3 (high availability)

### Step 3: Stream Processing (Flink)
```python
# Pseudocode of Flink logic
source = KafkaSource("trades")
windowed = source.window(TumblingWindow(1 minute))
ohlc = windowed.aggregate(calculate_ohlc)
sink = JdbcSink(TimescaleDB)
```

### Step 4: Storage (TimescaleDB)
```sql
-- Hypertable structure
CREATE TABLE candlesticks (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT
);
SELECT create_hypertable('candlesticks', 'time');
CREATE UNIQUE INDEX ON candlesticks (time, symbol);
```

### Step 5: Visualization (Grafana)
- **Refresh Interval**: 5-10 seconds
- **Time Range**: Last 24 hours (configurable)
- **Alerts**: Trigger when price exceeds thresholds

---

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# Finnhub API Key
FINNHUB_API_KEY=your_api_key_here

# Kafka Configuration
KAFKA_BROKER=kafka:29092
KAFKA_TOPIC=trades

# TimescaleDB
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=trading_db

# Flink Configuration
FLINK_JOB_PARALLELISM=4
FLINK_TASKMANAGER_SLOTS=2

# Grafana
GRAFANA_ADMIN_PASSWORD=admin
```

### Docker Compose Overrides

Modify `docker/compose.yaml` to:
- Adjust resource limits (memory, CPU)
- Change port mappings
- Add additional Kafka partitions
- Scale Flink TaskManagers

---

## Troubleshooting

### Issue: Producer Not Connecting to Kafka
**Solution**: Check Docker network connectivity
```bash
docker network ls
docker network inspect real-time-trading-platform_default
```

### Issue: Flink Job Not Submitting
**Solution**: View Flink logs
```bash
docker logs flink-job-submitter
```

### Issue: No Data in Grafana Dashboard
**Solution**: 
1. Verify Kafka has messages: Check Redpanda Console at `http://localhost:8080`
2. Check TimescaleDB: 
   ```sql
   SELECT COUNT(*) FROM candlesticks;
   ```
3. Verify Grafana data source connection in UI

### Issue: Out of Memory Errors
**Solution**: Increase Docker memory allocation
```bash
# On Docker Desktop, increase in settings
# Or in docker-compose.yaml, add under services:
# flink-jobmanager:
#   environment:
#     JVM_ARGS: "-Xms2g -Xmx4g"
```

### Performance Optimization
- Increase Kafka partitions for higher throughput
- Add more Flink TaskManagers for distributed processing
- Tune TimescaleDB `work_mem` for faster aggregations

---

## Monitoring & Logging

### Real-Time Logs
```bash
# Producer logs
docker logs -f producer

# Flink logs
docker logs -f flink-taskmanager

# TimescaleDB logs
docker logs -f timescaledb
```

### Metrics Collection
- Flink metrics: Available via REST API on port 8081
- Kafka metrics: Visible in Redpanda Console
- Database metrics: Query `pg_stat_statements` in TimescaleDB

---

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Open a pull request with a clear description

---

## License

This project is licensed under the MIT License. See the LICENSE file for details.

---

## Support & Resources

- **Finnhub API Docs**: https://finnhub.io/docs/api/websocket-trades
- **Apache Kafka Documentation**: https://kafka.apache.org/documentation/
- **PyFlink Guide**: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/
- **TimescaleDB Docs**: https://docs.timescale.com/
- **Grafana Dashboarding**: https://grafana.com/docs/grafana/latest/dashboards/

---

**Last Updated**: May 2026  
**Kappa Architecture Version**: 1.0  
**Status**: Production Ready ✓
