# Adaptive AI-Based Data Pipeline Defense System

Real-time middleware that intercepts, scores, and routes data streams using
Isolation Forest anomaly detection, adaptive threshold learning, and
intelligent Kafka topic routing — with full performance measurement and
proof that adaptive routing outperforms static pipelines under stress.

---

## Architecture

```
Failure Simulator (spike / injection / bottleneck / patterned / mixed)
    │
    ▼
[raw_ingest] ── Kafka topic
    │
    ▼
Middleware Interceptor  ◄── pipeline:experiment_mode (adaptive | baseline)
  ├── Isolation Forest scoring
  ├── LSTM spike prediction (optional)
  ├── Load monitor (rolling 200-event window)
  └── Adaptive routing rules
       ├── normal_stream   (clean traffic)
       ├── suspicious_stream (anomalies / injections)
       └── priority_stream  (critical / overload shedding)
    │
    ├── routing_decisions ── Kafka topic
    │       │
    │       ▼
    │   Metrics Collector
    │   ├── End-to-end latency (ingested_at → processed_at)
    │   ├── Throughput tracker (sliding 10s window)
    │   ├── Kafka consumer lag
    │   ├── Decision explainer (human-readable reasons)
    │   └── Writes → Redis time-series
    │
    └── Spark Streaming (windowed aggregations → Redis)
    │
    ▼
FastAPI Backend
  /api/status  /api/anomalies  /api/routing  /api/routing/stats
  /api/perf    /api/perf/latency-series      /api/decisions/timeline
  /api/mode    /api/simulate/{scenario}      /ws/live
    │
    ▼
React Dashboard (4 tabs)
  Overview   → traffic chart, routing graph, anomaly feed, load gauges
  Performance → latency chart, spike visualization, percentile panel
  Decisions  → explainable routing timeline
  Simulate   → mode toggle, attack controls, experiment guide
```

---

## Folder Structure

```
├── producer/              Kafka event producer
├── consumer/              Multi-topic consumer
├── middleware/            Core interceptor: scoring + routing + baseline mode
├── ml-model/              Isolation Forest + LSTM predictor
├── metrics-collector/     End-to-end latency, throughput, Kafka lag
├── simulation-engine/     Failure simulator, experiment runner, plot tool
│   ├── failure_simulator.py   spike / injection / bottleneck / patterned / mixed
│   ├── experiment_runner.py   baseline vs adaptive automated comparison
│   ├── plot_report.py         matplotlib comparison charts
│   └── show_logs.py           live log viewer with explanations
├── stream-processing/     Spark Structured Streaming aggregations
├── backend-api/           FastAPI REST + WebSocket
├── frontend-dashboard/    React dashboard (4 tabs)
└── docker-compose.yml
```

---

## Quick Start

```bash
# Full stack
docker-compose up --build

# With simulator
docker-compose --profile simulation up --build
```

| Service    | Port  |
|------------|-------|
| Dashboard  | 3000  |
| API docs   | 8000/docs |
| Kafka      | 9092  |
| Redis      | 6379  |

---

## Failure Simulation Scenarios

```bash
cd simulation-engine
pip install confluent-kafka redis

# Normal baseline traffic
python failure_simulator.py --scenario normal --base-rate 10

# 10x traffic spike for 15 seconds
python failure_simulator.py --scenario spike --spike-multiplier 10 --spike-duration 15

# 15% data injection attacks
python failure_simulator.py --scenario injection --injection-ratio 0.15

# Slow consumer (bottleneck simulation)
python failure_simulator.py --scenario bottleneck --bottleneck-delay 0.3

# Patterned anomaly bursts every 20 events
python failure_simulator.py --scenario patterned --pattern-period 20

# Realistic production mix (default)
python failure_simulator.py --scenario mixed --base-rate 20 --spike-multiplier 8
```

---

## Experiment Mode: Prove Adaptive > Baseline

### Automated comparison (CLI)

```bash
python simulation-engine/experiment_runner.py \
  --events 1000 \
  --base-rate 20 \
  --spike-multiplier 8 \
  --scenario mixed
```

Output example:
```
═══════════════════════════════════════════════════════════════════
  ADAPTIVE AI PIPELINE vs BASELINE — COMPARISON REPORT
═══════════════════════════════════════════════════════════════════
  Scenario : mixed
  Events   : baseline=1000  adaptive=1000

── THROUGHPUT ──────────────────────────────────────────────────
  Throughput: 18.2 ev/s → 24.7 ev/s  (↑35.7%)  ✅ BETTER

── LATENCY ─────────────────────────────────────────────────────
  Avg latency: 312ms → 89ms  (↓71.5%)  ✅ BETTER
  P99 latency: 2840ms → 420ms  (↓85.2%)  ✅ BETTER

── ANOMALY HANDLING ────────────────────────────────────────────
  Anomalies detected  : baseline=12  adaptive=147
  Events rerouted     : baseline=0 (0%)  adaptive=189 (18.9%)
  Overload events     : baseline=43  adaptive=2

── VERDICT ─────────────────────────────────────────────────────
  ✅ Adaptive mode delivered higher throughput under stress
  ✅ Latency reduced by 71.5%
  ✅ Overload events reduced: 43 → 2
  ✅ More anomalies caught: 12 → 147
  ✅ System crash avoided under load
═══════════════════════════════════════════════════════════════════
```

### Plot charts from report

```bash
pip install matplotlib
python simulation-engine/plot_report.py experiment_report_<timestamp>.json
# → saves comparison_report.png
```

### Manual experiment via dashboard

1. Open `http://localhost:3000` → **Simulate** tab
2. Switch to **Baseline** mode
3. Trigger **Traffic Spike** or **Data Injection**
4. Watch latency climb in **Performance** tab
5. Switch to **Adaptive AI** mode
6. Trigger same scenario — watch routing engine quarantine anomalies

---

## Live Log Viewer

```bash
# Show last 20 decisions with explanations
python simulation-engine/show_logs.py --limit 20

# Follow live
python simulation-engine/show_logs.py --follow

# Example output:
🔴 [2024-01-15 14:23:01] suspicious_stream    score=0.847 scenario=injection  | Anomaly score 0.847 > threshold 0.563 → quarantined
🟡 [2024-01-15 14:23:02] priority_stream      score=0.312 scenario=spike      | High load detected → prioritizing critical stream
🟢 [2024-01-15 14:23:03] normal_stream        score=0.124 scenario=normal     | Normal traffic → forwarded to normal_stream
```

---

## API Reference

| Method | Endpoint                      | Description                          |
|--------|-------------------------------|--------------------------------------|
| GET    | /api/status                   | System status + Kafka topics         |
| GET    | /api/anomalies?limit=N        | Recent anomaly events                |
| GET    | /api/routing?limit=N          | Recent routing decisions             |
| GET    | /api/routing/stats            | Aggregated routing breakdown         |
| GET    | /api/load                     | CPU/memory/request rate snapshot     |
| GET    | /api/perf                     | Latency percentiles, throughput, lag |
| GET    | /api/perf/latency-series      | Time-series latency for charting     |
| GET    | /api/decisions/timeline       | Explained routing decision timeline  |
| GET    | /api/mode                     | Current pipeline mode                |
| POST   | /api/mode                     | Set mode: adaptive \| baseline       |
| POST   | /api/threshold                | Override anomaly threshold           |
| POST   | /api/simulate/{scenario}      | Trigger attack simulation            |
| WS     | /ws/live                      | Live stream: decisions+metrics+perf  |

---

## Adaptive Routing Rules

```
IF event.is_critical OR priority_level IN (HIGH, CRITICAL)
    → priority_stream  [reason: critical_event]

ELSE IF anomaly_score > adaptive_threshold
    → suspicious_stream  [reason: anomaly_score=X>threshold=Y]

ELSE IF avg_cpu > 0.85 OR avg_memory > 0.85
    → priority_stream  [reason: load_shedding]

ELSE
    → normal_stream  [reason: normal_traffic]

IF baseline_mode == true
    → normal_stream  [reason: baseline_passthrough]  (AI bypassed)
```

Threshold self-adjusts toward p95 of recent scores (exponential smoothing, rate=0.05, bounds=[0.35, 0.85]).

---

## Enable LSTM Spike Predictor

```bash
pip install tensorflow
LSTM_ENABLED=true python middleware/interceptor.py
```
