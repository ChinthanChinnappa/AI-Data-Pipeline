# IMPLEMENTATION CHECKLIST - PHASE BY PHASE

Use this to track progress during implementation. Check off items as they complete.

---

## PHASE 1: FOUNDATION (Days 1-3)

### 1.1 PostgreSQL Setup
- [ ] PostgreSQL 14+ running (local or cloud)
- [ ] Create database: `pipeline_experiments`
- [ ] Create tables:
  - [ ] `experiments` (id, name, scenario, start_time, end_time, duration_seconds, seed, baseline_config, adaptive_config, created_at)
  - [ ] `experiment_metrics` (id, experiment_id, mode, metric_name, value, timestamp, created_at)
  - [ ] `experiment_results` (id, experiment_id, metric_name, baseline_value, adaptive_value, improvement_pct, significance, effect_size, created_at)
  - [ ] `component_logs` (id, timestamp, component, level, event, details_json, created_at)
- [ ] Create indices on experiment_id, timestamp for fast queries
- [ ] Test connectivity from Python (psycopg2)

### 1.2 Experiment Manager Module
- [ ] Create file: `simulation-engine/experiment_manager.py`
- [ ] Implement `ExperimentManager` class:
  - [ ] `start_experiment(config, seed)` → returns experiment_id
  - [ ] `poll_completion()` → checks if experiment done
  - [ ] `get_experiment_status()` → current phase, progress %
  - [ ] `stop_experiment()` → graceful shutdown
- [ ] State machine:
  - [ ] SETUP state
  - [ ] BASELINE state (phase duration: 1 hour)
  - [ ] ADAPTIVE state (phase duration: 1 hour)
  - [ ] DONE state
- [ ] Redis integration:
  - [ ] Set `pipeline:experiment_mode` flag
  - [ ] Set `pipeline:experiment_id` for scope
  - [ ] Monitor completion via `pipeline:experiment_status`
- [ ] Database logging:
  - [ ] INSERT experiment record at start
  - [ ] UPDATE experiment record at end

### 1.3 Experiment Config Schema
- [ ] Create dataclass: `ExperimentConfig`
  - [ ] scenario: SimScenario enum
  - [ ] duration_seconds: int
  - [ ] base_rate: float (events/sec)
  - [ ] spike_multiplier: float
  - [ ] injection_ratio: float
  - [ ] seed: int (for reproducibility)
  - [ ] name: str
  - [ ] description: str
- [ ] Create sample configs:
  - [ ] config_baseline_mixed.yaml
  - [ ] config_adaptive_mixed.yaml
- [ ] Load configs from YAML files

### 1.4 Redis Namespacing
- [ ] Create namespace manager:
  - [ ] `redis_key(experiment_id, key_suffix)` → scoped keys
  - [ ] Example: `exp-001:pipeline:metrics` instead of just `pipeline:metrics`
- [ ] Ensure isolation between experiments:
  - [ ] When experiment starts, flush old keys
  - [ ] When experiment ends, preserve keys (for analysis)

### 1.5 Testing
- [ ] Test: Can start experiment → Redis mode flag set ✓
- [ ] Test: Can poll completion → state machine works ✓
- [ ] Test: Can write to PostgreSQL → connectivity ✓
- [ ] Test: Reproducibility → same seed → same events ✓

**Deliverable**: `experiment_manager.py` working, PostgreSQL populated

---

## PHASE 2: FAILURE SIMULATION (Days 4-6)

### 2.1 RAMP Scenario
- [ ] Implement in `failure_simulator.py`:
  - [ ] Phase 1 (0-60s): 1x baseline rate
  - [ ] Phase 2 (60-120s): 3x baseline rate
  - [ ] Phase 3 (120-180s): 5x baseline rate
  - [ ] Phase 4 (180-240s): 10x baseline rate
- [ ] Event generation:
  - [ ] Calculate phase duration from total_events
  - [ ] Interpolate rate at each timestamp
  - [ ] Generate events at calculated rate
- [ ] Metrics per phase:
  - [ ] Track: latency, throughput, errors, resources at each phase
  - [ ] Log phase transitions with timestamp
- [ ] Testing: Run RAMP scenario → verify load increases smoothly ✓

### 2.2 PATTERN_INJECTION Scenario
- [ ] Define attack pattern (repeated malicious sequence):
  - [ ] Example: SQL injection marker every 10 events
  - [ ] Example: Oversized payload burst every 20 events
- [ ] Implementation:
  - [ ] Base rate: normal events
  - [ ] Every N events: inject malicious event (same payload each time)
  - [ ] Pattern repeats 20+ times in sequence
- [ ] Measurement:
  - [ ] When does system detect pattern? (event #3 of burst? #5?)
  - [ ] Does anomaly detector learn pattern? (improve over time?)
- [ ] Testing: Run PATTERN scenario → verify attacks detected ✓

### 2.3 RESOURCE_BOTTLENECK Scenario
- [ ] Cascading resource constraints:
  - [ ] Phase 1 (0-60s): Normal load (1x)
  - [ ] Phase 2 (60-120s): Add latency (5x per message)
  - [ ] Phase 3 (120-180s): Reduce memory (30% less available)
  - [ ] Phase 4 (180-240s): Both constraints active
- [ ] Queue buildup tracking:
  - [ ] Monitor Kafka consumer lag
  - [ ] Measure queue depth time-series
  - [ ] Detect when queue exceeds thresholds
- [ ] Recovery measurement:
  - [ ] MTTR after constraint removed
  - [ ] Queue drain rate
- [ ] Testing: Run BOTTLENECK → verify queue buildup + recovery ✓

### 2.4 CASCADING_MIXED Scenario
- [ ] Simultaneous failures:
  - [ ] 5x traffic spike
  - [ ] 15% malicious injection (repeating pattern)
  - [ ] Slow consumer (3x processing latency)
  - [ ] Memory pressure (80% used)
- [ ] Stress points:
  - [ ] Circuit breaker should activate
  - [ ] Backpressure should trigger
  - [ ] DLQ should receive some messages
  - [ ] System should not crash
- [ ] Recovery:
  - [ ] Track when constraints ease
  - [ ] Measure system stabilization time
- [ ] Testing: Run CASCADING → verify system survives chaos ✓

### 2.5 Enhanced Metrics
- [ ] Add to SimConfig/FailureSimulator:
  - [ ] `queue_depth_samples`: time-series of queue depth
  - [ ] `error_rate_samples`: time-series of errors
  - [ ] `recovery_time`: MTTR after each failure phase
  - [ ] `phase_transitions`: log of phase changes
- [ ] Export metrics:
  - [ ] Write to Redis after run
  - [ ] Write to PostgreSQL for archival
- [ ] Testing: Verify metrics collected → stored ✓

**Deliverable**: failure_simulator.py with 4 scenarios, queue tracking

---

## PHASE 3: MIDDLEWARE HARDENING (Days 7-12)

### 3.1 Circuit Breaker Pattern
- [ ] Create class: `CircuitBreaker`
  - [ ] State: CLOSED, OPEN, HALF_OPEN
  - [ ] Failure threshold: 5 failures → OPEN
  - [ ] Timeout: 60 seconds → HALF_OPEN
  - [ ] Success ratio in HALF_OPEN: 9/10 → CLOSED
- [ ] Integration in middleware:
  - [ ] Check circuit breaker before ML inference
  - [ ] if OPEN: use fallback router
  - [ ] if HALF_OPEN: count successes
  - [ ] Log all state transitions
- [ ] Redis storage:
  - [ ] `pipeline:circuit_breaker:state` (CLOSED|OPEN|HALF_OPEN)
  - [ ] `pipeline:circuit_breaker:failure_count` (int)
  - [ ] `pipeline:circuit_breaker:last_transition` (timestamp)
- [ ] Testing:
  - [ ] Trigger failures → verify OPEN ✓
  - [ ] Wait 60s → verify HALF_OPEN ✓
  - [ ] 9/10 success → verify CLOSED ✓

### 3.2 Backpressure Handler
- [ ] Monitor queue depth:
  - [ ] Get Kafka consumer lag (messages behind)
  - [ ] Calculate as % of max queue capacity
- [ ] Define thresholds:
  - [ ] YELLOW (70% capacity): warning, increase shedding to 10%
  - [ ] RED (90% capacity): critical, increase shedding to 50%
  - [ ] ABSOLUTE_MAX (95% capacity): emergency, shed all non-critical
- [ ] Shedding logic:
  - [ ] if random() < shedding_ratio AND NOT critical_message:
  - [ ] Route to dead_letter_queue (preserve for later analysis)
  - [ ] Log shedding event
- [ ] Metrics:
  - [ ] Queue depth time-series
  - [ ] Shedding ratio (% dropped)
  - [ ] Shedding count per minute
- [ ] Testing:
  - [ ] Fill queue to YELLOW → verify shedding ✓
  - [ ] Fill to RED → verify higher shedding ✓
  - [ ] Release pressure → verify recovery ✓

### 3.3 Dynamic Threshold Tuner
- [ ] Algorithm:
  - [ ] Every 100 events: calculate false positive rate
  - [ ] If FPR > 5%: increase threshold by 0.02
  - [ ] If FPR < 2.5%: decrease threshold by 0.01
  - [ ] Keep in range [0.40, 0.80]
- [ ] FPR calculation:
  - [ ] False positives = normal traffic routed to suspicious_stream
  - [ ] FPR = false_positives / total_normal_traffic
- [ ] Tracking:
  - [ ] Log every threshold adjustment
  - [ ] Store current threshold in Redis: `pipeline:anomaly:threshold`
  - [ ] Store tuner history in PostgreSQL
- [ ] Testing:
  - [ ] Start at 0.55
  - [ ] Inject events that trigger high FPR → verify increase ✓
  - [ ] Inject events that trigger low FPR → verify decrease ✓

### 3.4 Priority Scoring System
- [ ] Scoring logic:
  ```
  if message.is_critical:
    priority = CRITICAL
  elif cpu_usage > 0.85 AND system_overloaded:
    priority = HIGH (fast-track through normal_stream)
  else:
    priority = NORMAL or LOW
  ```
- [ ] Routing integration:
  - [ ] CRITICAL → priority_stream (always)
  - [ ] HIGH → normal_stream (but flagged for fast tracking)
  - [ ] NORMAL/LOW → normal_stream (standard)
- [ ] Under extreme load:
  - [ ] Only CRITICAL messages processed
  - [ ] Shed all HIGH and below
- [ ] Metrics:
  - [ ] Priority distribution (% critical, high, normal, low)
  - [ ] Routing by priority
- [ ] Testing:
  - [ ] Mark message as critical → verify routed to priority_stream ✓
  - [ ] Under extreme load → verify only critical survive ✓

### 3.5 Fallback Router
- [ ] Rule-based scorer (NO ML):
  - [ ] if cpu > 0.90: route to suspicious_stream
  - [ ] if memory > 0.90: route to suspicious_stream
  - [ ] if error_rate > 0.5: route to suspicious_stream
  - [ ] otherwise: route to normal_stream
- [ ] Integration:
  - [ ] Use when ML model fails
  - [ ] Use when circuit breaker is OPEN
  - [ ] Log fallback usage (for analysis)
- [ ] Testing:
  - [ ] Disable ML model → verify fallback works ✓
  - [ ] Open circuit breaker → verify fallback works ✓

### 3.6 Structured Decision Logging
- [ ] JSON format (every routing decision):
  ```json
  {
    "decision_id": "uuid",
    "event_id": "uuid",
    "timestamp": "iso8601",
    "routing_decision": "suspicious_stream",
    "anomaly_score": 0.76,
    "confidence": 0.89,
    "reason": "High anomaly + overload detected",
    "system_state": {
      "cpu_usage": 0.82,
      "memory_usage": 0.75,
      "queue_depth": 234,
      "current_threshold": 0.55,
      "load_detected": true,
      "circuit_breaker_state": "CLOSED"
    },
    "decision_chain": [
      "Feature extraction: success",
      "Isolation Forest: score=0.76",
      "Threshold check: 0.76 > 0.55 → anomaly",
      "Load check: CPU=82% → overloaded",
      "Final route: suspicious_stream"
    ]
  }
  ```
- [ ] Storage:
  - [ ] Publish to Kafka: `routing_decisions` topic
  - [ ] Store recent in Redis: `pipeline:decisions:recent` (FIFO, max 1000)
  - [ ] Archive to PostgreSQL: `experiment_decisions` table
- [ ] Testing:
  - [ ] Verify decision JSON format ✓
  - [ ] Verify Redis storage working ✓
  - [ ] Verify Kafka publishing working ✓

### 3.7 Retry + DLQ Handler
- [ ] Retry logic:
  - [ ] Failed message → add to `retry_queue` topic
  - [ ] Set retry_count = 1
  - [ ] Wait: 2^retry_count seconds (exponential backoff)
  - [ ] Retry up to 3 times
- [ ] DLQ logic:
  - [ ] After 3 failed retries → route to `dead_letter_queue` topic
  - [ ] Log DLQ message (why did it fail?)
  - [ ] Alert if DLQ grows too large
- [ ] New Kafka topics:
  - [ ] `retry_queue` (1 partition)
  - [ ] `dead_letter_queue` (1 partition)
- [ ] Metrics:
  - [ ] Retry success rate (% that eventually succeed)
  - [ ] Retry latency (time from failure to success)
  - [ ] DLQ message count
  - [ ] DLQ processing latency
- [ ] Testing:
  - [ ] Trigger transient error → verify retry succeeds ✓
  - [ ] Trigger persistent error → verify DLQ after 3 retries ✓

### 3.8 Graceful Degradation
- [ ] Level 1 (Normal):
  - [ ] Full feature extraction
  - [ ] Isolation Forest + LSTM
  - [ ] Pattern detection enabled
- [ ] Level 2 (Degraded):
  - [ ] Trigger: memory > 85% OR CPU > 95%
  - [ ] Reduce feature extraction depth
  - [ ] Use Isolation Forest only (no LSTM)
  - [ ] Log: "Entering degraded mode"
- [ ] Level 3 (Emergency):
  - [ ] Trigger: memory > 95% OR CPU > 99%
  - [ ] Use fallback router (no ML at all)
  - [ ] Shed all non-critical traffic
  - [ ] Log: "EMERGENCY SHED ACTIVATED"
- [ ] Metrics:
  - [ ] Degradation level active
  - [ ] Duration at each level
  - [ ] Recovery to normal
- [ ] Testing:
  - [ ] Simulate memory pressure → verify Level 2 ✓
  - [ ] Simulate extreme pressure → verify Level 3 ✓
  - [ ] Release pressure → verify recovery ✓

### 3.9 Middleware Integration
- [ ] Update middleware/interceptor.py:
  - [ ] Call circuit breaker check
  - [ ] Call backpressure handler
  - [ ] Call dynamic threshold tuner
  - [ ] Call priority scorer
  - [ ] Call fallback router if needed
  - [ ] Log structured decision
  - [ ] Handle retry + DLQ
  - [ ] Monitor for graceful degradation
- [ ] Testing: Full middleware flow end-to-end ✓

**Deliverable**: Enhanced middleware with all resilience patterns

---

## PHASE 4: METRICS (Days 13-15)

### 4.1 Per-Scenario Breakdown
- [ ] Modify metrics_collector.py:
  - [ ] Track scenario field from each event
  - [ ] Group metrics by scenario: normal, spike, injection, bottleneck
  - [ ] Calculate latency_normal, latency_spike, etc.
  - [ ] Calculate throughput_normal, throughput_spike, etc.
- [ ] Redis storage:
  - [ ] `pipeline:metrics:scenario:normal` (separate metrics)
  - [ ] `pipeline:metrics:scenario:spike`
  - [ ] `pipeline:metrics:scenario:injection`
  - [ ] `pipeline:metrics:scenario:bottleneck`
- [ ] Testing: Verify per-scenario separation ✓

### 4.2 Percentile Tracking
- [ ] Enhanced LatencyTracker:
  - [ ] Track: p50, p75, p90, p95, p99, p99.9
  - [ ] Use sorted list or percentile algorithm
  - [ ] Update every 5 seconds
- [ ] Redis storage:
  - [ ] `pipeline:metrics:latency:percentiles` (dict with all percentiles)
- [ ] Testing: Verify percentile accuracy ✓

### 4.3 Resource Utilization
- [ ] Track from system_state in decisions:
  - [ ] CPU usage (rolling average)
  - [ ] Memory usage (rolling average)
  - [ ] Network I/O (if available)
- [ ] Metrics to calculate:
  - [ ] CPU avg, min, max
  - [ ] Memory peak, sustained
  - [ ] I/O throughput
- [ ] Comparison:
  - [ ] Resource usage: baseline vs adaptive
- [ ] Testing: Verify resource metrics collected ✓

### 4.4 Decision Quality Metrics
- [ ] Precision (relevant detections):
  - [ ] Precision = true_anomalies / total_suspicious_routed
  - [ ] (Normal traffic that shouldn't be routed to suspicious)
- [ ] Recall (coverage):
  - [ ] Recall = detected_anomalies / total_actual_anomalies
  - [ ] (Anomalies that were missed)
- [ ] False Positive Rate (FPR):
  - [ ] FPR = false_positives / total_normal_traffic
- [ ] False Negative Rate (FNR):
  - [ ] FNR = false_negatives / total_actual_anomalies
- [ ] F1 Score:
  - [ ] F1 = 2 * (precision * recall) / (precision + recall)
- [ ] Ground truth:
  - [ ] Use injected anomalies as ground truth
  - [ ] Track which messages were injected
- [ ] Storage:
  - [ ] `pipeline:metrics:decision_quality` (precision, recall, FPR, FNR, F1)
- [ ] Testing: Verify quality metrics calculate correctly ✓

### 4.5 Error Classification
- [ ] Error types:
  - [ ] RETRY (transient, will retry)
  - [ ] TIMEOUT (message took too long)
  - [ ] EXCEPTION (unexpected error)
  - [ ] DLQ (unrecoverable)
- [ ] Tracking:
  - [ ] Count by type per 5-second window
  - [ ] Rate by type (% of total errors)
- [ ] Storage:
  - [ ] `pipeline:metrics:errors:by_type` (counts)
  - [ ] `pipeline:metrics:error_rate` (%)
- [ ] Testing: Verify error classification ✓

### 4.6 SLA Violation Detection
- [ ] Define SLA targets (example):
  - [ ] p99 latency < 200ms
  - [ ] error_rate < 2%
  - [ ] availability > 99.9%
- [ ] Check every 5 seconds:
  - [ ] if p99 > 200ms: log SLA violation
  - [ ] if error_rate > 2%: log SLA violation
- [ ] Metrics:
  - [ ] SLA violation count
  - [ ] Duration of each violation
  - [ ] Recovery time to SLA compliance
- [ ] Storage:
  - [ ] `pipeline:metrics:sla:violations` (list of violations)
- [ ] Testing: Trigger SLA violation → verify detection ✓

### 4.7 Metrics Archival
- [ ] Every 60 seconds:
  - [ ] Fetch current metrics from Redis
  - [ ] Insert into PostgreSQL: `experiment_metrics` table
  - [ ] One row per metric per scenario
- [ ] Table structure:
  - [ ] experiment_id, mode, timestamp, scenario, metric_name, value
- [ ] Testing: Verify archival to PostgreSQL ✓

**Deliverable**: metrics_collector.py with comprehensive tracking

---

## PHASE 5: MODEL HEALTH MONITOR (Days 16-17)

### 5.1 Create model_monitor.py
- [ ] Module: `ml-model/model_monitor.py`
- [ ] Class: `ModelHealthMonitor`

### 5.2 Inference Latency Tracking
- [ ] Measure time for model.predict() call
- [ ] Track rolling average (last 100 inferences)
- [ ] Alert if latency > 50ms
- [ ] Storage: `pipeline:monitor:ml:inference_latency_ms`
- [ ] Testing: Measure inference time ✓

### 5.3 Data Drift Detection
- [ ] Compare feature distributions:
  - [ ] Training data distribution
  - [ ] Current production data distribution
- [ ] Use statistical tests (e.g., Kolmogorov-Smirnov test)
- [ ] Alert if divergence > threshold
- [ ] Storage: `pipeline:monitor:ml:drift_detected` (bool)
- [ ] Testing: Inject drift → verify detection ✓

### 5.4 Feature Validity Checks
- [ ] Check for:
  - [ ] NaN values (invalid)
  - [ ] Out-of-range values (feature bounds)
  - [ ] Inf values (invalid)
- [ ] Alert if invalid features detected
- [ ] Fallback: use fallback router if invalid
- [ ] Storage: `pipeline:monitor:ml:feature_validity` (count of invalid)
- [ ] Testing: Feed invalid features → verify detection ✓

### 5.5 Online Accuracy Monitoring
- [ ] If ground truth available:
  - [ ] Track model predictions vs ground truth
  - [ ] Calculate accuracy, precision, recall
  - [ ] Alert if accuracy drops > 10%
- [ ] Storage: `pipeline:monitor:ml:accuracy` (float)
- [ ] Testing: Validate accuracy calculation ✓

### 5.6 Health Dashboard Endpoint
- [ ] New API: `/api/health/model`
- [ ] Returns:
  ```json
  {
    "inference_latency_ms": 3.2,
    "drift_detected": false,
    "feature_validity_ok": true,
    "accuracy": 0.92,
    "status": "HEALTHY"
  }
  ```
- [ ] Testing: Verify endpoint returns data ✓

**Deliverable**: model_monitor.py with health checks

---

## PHASE 6: BACKEND API (Days 18-20)

### 6.1 Experiment Endpoints
- [ ] `POST /api/experiments` (create new experiment)
  - [ ] Input: config (scenario, duration, seed)
  - [ ] Output: experiment_id, start_time
  - [ ] Action: Call ExperimentManager.start()
- [ ] `GET /api/experiments` (list experiments)
  - [ ] Output: [{id, name, scenario, status, progress_pct}, ...]
- [ ] `GET /api/experiments/{id}` (get details)
  - [ ] Output: full experiment config + current status
- [ ] `DELETE /api/experiments/{id}` (stop experiment)
  - [ ] Action: Call ExperimentManager.stop()
- [ ] Testing: CRUD operations work ✓

### 6.2 Results Endpoints
- [ ] `GET /api/experiments/{id}/results`
  - [ ] Output: {baseline_metrics, adaptive_metrics, comparison}
  - [ ] Example:
    ```json
    {
      "baseline": {"latency_p50": 126.5, "throughput": 8510, ...},
      "adaptive": {"latency_p50": 96.1, "throughput": 10195, ...},
      "comparison": {
        "latency": {
          "baseline": 126.5, "adaptive": 96.1,
          "improvement_pct": 24.0, "p_value": <0.001
        },
        ...
      }
    }
    ```
- [ ] `GET /api/experiments/{id}/report.pdf`
  - [ ] Output: PDF file download
- [ ] Testing: Verify results endpoint ✓

### 6.3 Metrics Endpoints
- [ ] `GET /api/metrics/latency-distribution?limit=500`
  - [ ] Output: [123.5, 124.2, 125.1, ...] (recent samples)
- [ ] `GET /api/metrics/throughput-over-time?window=60&hours=1`
  - [ ] Output: [{timestamp, throughput_evt_sec}, ...]
- [ ] `GET /api/metrics/latency-percentiles?scenario=spike`
  - [ ] Output: {p50, p75, p90, p95, p99, p99.9, min, max}
- [ ] `GET /api/metrics/decision-quality`
  - [ ] Output: {precision, recall, fpr, fnr, f1}
- [ ] `GET /api/metrics/resource-usage`
  - [ ] Output: {cpu_avg, cpu_peak, mem_avg, mem_peak, io_throughput}
- [ ] `GET /api/metrics/error-breakdown`
  - [ ] Output: {retry_count, timeout_count, exception_count, dlq_count}
- [ ] Testing: Verify all metrics endpoints ✓

### 6.4 Health Endpoints
- [ ] `GET /api/health/system`
  - [ ] Output: {status, components: {middleware, ml, kafka, consumer, ...}}
  - [ ] Each component: {status, last_check, issues}
- [ ] `GET /api/health/model` (already done in Phase 5)
- [ ] Testing: Verify health check ✓

### 6.5 Control Endpoints
- [ ] `POST /api/control/circuit-breaker/open`
  - [ ] Action: Force circuit breaker OPEN
- [ ] `POST /api/control/circuit-breaker/close`
  - [ ] Action: Force circuit breaker CLOSED
- [ ] `GET /api/control/circuit-breaker/state`
  - [ ] Output: current state (CLOSED, OPEN, HALF_OPEN)
- [ ] Testing: Verify control operations ✓

### 6.6 Statistics Engine
- [ ] Create file: `backend-api/statistics.py`
- [ ] Class: `StatisticsEngine`
- [ ] Methods:
  - [ ] `t_test(baseline_samples, adaptive_samples)` → p_value
  - [ ] `confidence_interval(samples, confidence=0.95)` → (lower, upper)
  - [ ] `effect_size(baseline_samples, adaptive_samples)` → Cohen's_d
  - [ ] `compare_experiments(exp_id)` → full comparison report (dict)
- [ ] Testing: Verify statistical calculations ✓

### 6.7 WebSocket Live Metrics
- [ ] `ws://localhost:8000/ws/live-metrics`
- [ ] Streams every 500ms:
  ```json
  {
    "timestamp": "2026-04-25T10:30:45.123Z",
    "throughput": 8450,
    "latency_p50": 125.3,
    "latency_p95": 240.5,
    "error_rate": 2.1,
    "cpu_usage": 0.72,
    "memory_usage": 0.68
  }
  ```
- [ ] Testing: Connect WebSocket → verify streams ✓

### 6.8 API Documentation
- [ ] OpenAPI/Swagger auto-generated by FastAPI
- [ ] Verify at: `http://localhost:8000/docs`
- [ ] All endpoints documented with examples
- [ ] Testing: Open /docs in browser ✓

**Deliverable**: Complete REST API + WebSocket + statistics

---

## PHASE 7: DASHBOARD (Days 21-23)

### 7.1 Tab 1: Experiment Overview
- [ ] Components:
  - [ ] Current experiment name + status
  - [ ] Elapsed time / Total time (progress bar)
  - [ ] Load phase indicator (1x, 3x, 5x, 10x)
  - [ ] Side-by-side comparison table:
    - [ ] Metric | Baseline | Adaptive | Improvement
    - [ ] Latency p50 | 128ms | 96ms | ↓ 25%
    - [ ] Throughput | 8500 | 10030 | ↑ 18%
    - [ ] Error rate | 2.8% | 1.6% | ↓ 43%
    - [ ] CPU util | 45% | 42% | ↓ 7%
- [ ] Live updates: fetch every 1 second
- [ ] Testing: Verify all metrics displayed correctly ✓

### 7.2 Tab 2: Performance Analysis
- [ ] Components:
  - [ ] Latency histogram (overlaid baseline vs adaptive)
  - [ ] Throughput line chart (time series)
  - [ ] Load ramp visualization (phases with metrics)
  - [ ] Resource utilization stacked area (CPU + memory)
  - [ ] Percentile table (p50, p75, p90, p95, p99, p99.9)
- [ ] Libraries: Plotly, React-Plotly, or Chart.js
- [ ] Testing: Verify plots render correctly ✓

### 7.3 Tab 3: Decision Timeline
- [ ] Components:
  - [ ] Scrollable decision log (newest first, 100 per page)
  - [ ] Each row: [timestamp] [routing_decision] [score] [reason]
  - [ ] Click to expand: show full decision_chain JSON
  - [ ] Filters:
    - [ ] Routing type (checkboxes): normal, suspicious, priority, DLQ
    - [ ] Anomaly score range (slider): 0.0 - 1.0
    - [ ] System state: overloaded or not
    - [ ] Reason keywords (text search)
  - [ ] System state heatmap (time vs components)
  - [ ] Routing distribution pie chart (% to each stream)
  - [ ] Decision latency trend (ms over time)
- [ ] Data source: `/api/decisions/timeline?limit=100&offset=0`
- [ ] Testing: Verify filtering + expansion ✓

### 7.4 Tab 4: System Health
- [ ] Components:
  - [ ] Circuit breaker status (green/red indicator + state text)
  - [ ] Queue depth gauge (% capacity with color coding)
  - [ ] Consumer lag monitor (events behind)
  - [ ] Anomaly detection accuracy (precision, recall, F1)
  - [ ] Component error rates table (middleware, ML, consumer, etc.)
  - [ ] DLQ message count alert
  - [ ] Model health: drift detected? yes/no
- [ ] Alerts: highlight critical issues in red
- [ ] Testing: Verify all health indicators ✓

### 7.5 Tab 5: Experiment Control
- [ ] Components:
  - [ ] Start new experiment form:
    - [ ] Scenario dropdown (NORMAL, SPIKE, INJECTION, etc.)
    - [ ] Duration input (hours)
    - [ ] Seed input or "Generate random"
    - [ ] Start button
  - [ ] Compare experiments:
    - [ ] Select 2 experiments from dropdown
    - [ ] Generate comparison button
    - [ ] Shows side-by-side metrics + p-values
  - [ ] Export results:
    - [ ] Download CSV button
    - [ ] Download PDF button
    - [ ] Download JSON button
  - [ ] Model health:
    - [ ] Last checked time
    - [ ] Drift status: yes/no
- [ ] Testing: Verify all controls functional ✓

### 7.6 Styling & Responsiveness
- [ ] Responsive design (works on desktop + tablet)
- [ ] Dark mode support (if desired)
- [ ] Accessible color scheme (distinguish red/green safely)
- [ ] Mobile-friendly (fonts, spacing, touch targets)
- [ ] Testing: Test on multiple screen sizes ✓

**Deliverable**: Complete React dashboard with 5 tabs

---

## PHASE 8: REPORT GENERATION (Days 24-25)

### 8.1 Report Generator Module
- [ ] Create file: `simulation-engine/report_generator.py`
- [ ] Class: `ReportGenerator`
- [ ] Method: `generate_pdf(experiment_id)` → PDF file path

### 8.2 Latency Comparison Plot
- [ ] Histogram: baseline vs adaptive overlaid
- [ ] X-axis: latency (ms)
- [ ] Y-axis: frequency/count
- [ ] Title: "Latency Distribution: Baseline vs Adaptive"
- [ ] Annotation: "24.0% improvement, p < 0.001, d = 0.98"
- [ ] Testing: Generate plot ✓

### 8.3 Throughput Comparison Plot
- [ ] Line chart: baseline vs adaptive over time
- [ ] X-axis: time (0-60 minutes)
- [ ] Y-axis: events/sec
- [ ] Title: "Throughput Trend: Baseline vs Adaptive"
- [ ] Annotation: "19.8% improvement, p < 0.001"
- [ ] Testing: Generate plot ✓

### 8.4 Error Rate Comparison Plot
- [ ] Bar chart: baseline vs adaptive
- [ ] X-axis: [Baseline, Adaptive]
- [ ] Y-axis: error rate (%)
- [ ] Title: "Error Rate Comparison"
- [ ] Annotation: "42.6% reduction, p < 0.001"
- [ ] Testing: Generate plot ✓

### 8.5 Load Ramp Visualization
- [ ] Scatter plot: phases (1x, 3x, 5x, 10x) vs latency/throughput
- [ ] Two series: baseline (blue), adaptive (red)
- [ ] Shows performance degradation at each load level
- [ ] Comparison: which system handles high load better?
- [ ] Testing: Generate plot ✓

### 8.6 Resource Utilization Plot
- [ ] Stacked area: CPU + memory over time
- [ ] Baseline and adaptive side-by-side or overlaid
- [ ] Title: "Resource Utilization"
- [ ] Shows if adaptive is more resource-efficient
- [ ] Testing: Generate plot ✓

### 8.7 PDF Template & Export
- [ ] Page structure:
  1. Cover page (title, date, scenario)
  2. Executive summary (key findings)
  3. Methodology (statistical rigor explanation)
  4. Results (latency, throughput, errors, quality)
  5. Plots (embedded images)
  6. Detailed metrics table
  7. Recommendations
- [ ] Use: ReportLab or similar PDF library
- [ ] Formatting: Professional, easy to read
- [ ] Testing: Generate complete PDF ✓

### 8.8 CLI Tool
- [ ] Command: `python report_generator.py --experiment exp-001`
- [ ] Output: `reports/experiment-exp-001.pdf`
- [ ] Also support: `--all-experiments` (generate for all)
- [ ] Testing: Run CLI tool ✓

**Deliverable**: report_generator.py with PDF export

---

## PHASE 9: E2E TESTING (Days 26-27)

### 9.1 Baseline Experiment (1 hour, MIXED scenario)
- [ ] Start: ExperimentManager.start(config)
- [ ] Mode: baseline (rules only)
- [ ] Duration: 1 hour
- [ ] Scenario: MIXED (spike + injection + bottleneck)
- [ ] Verify metrics collected:
  - [ ] Latency samples: 720 points (every 5s)
  - [ ] Throughput: 720 points
  - [ ] Error rate: 720 points
  - [ ] Resource usage: 720 points
- [ ] Check PostgreSQL populated:
  - [ ] 720 rows in experiment_metrics table (mode='baseline')
- [ ] Result: Baseline metrics archived ✓

### 9.2 Adaptive Experiment (1 hour, SAME scenario, SAME seed)
- [ ] Start: ExperimentManager.start(config)
- [ ] Mode: adaptive (AI routing enabled)
- [ ] Duration: 1 hour
- [ ] Scenario: MIXED (SAME seed as baseline)
- [ ] Verify reproducibility:
  - [ ] Same events generated as baseline
  - [ ] Same ingestion timestamps
- [ ] Verify metrics collected:
  - [ ] 720 latency points
  - [ ] 720 throughput points
  - [ ] 720 error rate points
  - [ ] 720 resource points
  - [ ] Plus decision quality metrics
- [ ] Check PostgreSQL populated:
  - [ ] 720 rows in experiment_metrics table (mode='adaptive')
- [ ] Result: Adaptive metrics archived ✓

### 9.3 Generate Comparison Report
- [ ] Trigger: StatisticsEngine.compare_experiments(exp_id)
- [ ] Verify calculations:
  - [ ] Mean baseline: 126.5ms ± 1.2ms (95% CI)
  - [ ] Mean adaptive: 96.1ms ± 0.9ms (95% CI)
  - [ ] T-test p-value: < 0.001
  - [ ] Cohen's d: 0.98 (large effect)
  - [ ] Improvement: 24.0% ± 3.0%
- [ ] Verify results stored:
  - [ ] INSERT into experiment_results table
  - [ ] Query: SELECT * FROM experiment_results WHERE experiment_id = X
  - [ ] Verify all metrics have p-values + effect sizes
- [ ] Verify PDF report generated:
  - [ ] File exists: `reports/experiment-{id}.pdf`
  - [ ] Contains all required sections
- [ ] Result: Comparison report complete ✓

### 9.4 SPIKE Scenario E2E
- [ ] Run SPIKE scenario (controlled spike)
- [ ] Verify:
  - [ ] System handles spike without crash
  - [ ] Latency increases but recovers
  - [ ] Throughput drops then recovers
  - [ ] Routing changes appropriately (more to suspicious)
- [ ] Result: SPIKE test passed ✓

### 9.5 PATTERN_INJECTION Scenario E2E
- [ ] Run PATTERN_INJECTION scenario (repeating attacks)
- [ ] Verify:
  - [ ] System detects pattern
  - [ ] Anomaly detection precision/recall reasonable
  - [ ] False positive rate acceptable
  - [ ] System learns over time (threshold tunes)
- [ ] Result: PATTERN test passed ✓

### 9.6 CASCADING_MIXED Scenario E2E
- [ ] Run CASCADING_MIXED scenario (multi-failure chaos)
- [ ] Verify:
  - [ ] Circuit breaker activates
  - [ ] Backpressure triggers shedding
  - [ ] DLQ captures failed messages
  - [ ] System survives without crash
  - [ ] Recovery time < 10 seconds
- [ ] Result: CASCADING test passed ✓

### 9.7 Circuit Breaker Triggering & Recovery
- [ ] Inject ML failures (5 in a row)
- [ ] Verify:
  - [ ] Circuit breaker transitions to OPEN
  - [ ] Fallback router used
  - [ ] System continues operating
  - [ ] Log shows state transitions
- [ ] Wait 60+ seconds
- [ ] Verify:
  - [ ] Circuit breaker transitions to HALF_OPEN
  - [ ] 1 success in HALF_OPEN
  - [ ] Circuit breaker transitions to CLOSED
  - [ ] Normal operation resumes
- [ ] Result: Circuit breaker test passed ✓

### 9.8 Extreme Load (10x) Without Crash
- [ ] Run RAMP scenario to 10x load
- [ ] Verify:
  - [ ] No crashes or exceptions
  - [ ] System degrades gracefully
  - [ ] Queue depth monitored
  - [ ] Backpressure shedding active
  - [ ] Circuit breaker stable (< 5 trips)
- [ ] Verify metrics:
  - [ ] Latency increases (acceptable)
  - [ ] Throughput remains high (backpressure allows through)
  - [ ] Error rate manageable (< 5%)
  - [ ] Resource usage stays within limits
- [ ] Result: 10x load test passed ✓

### 9.9 Validation Summary
- [ ] All metrics > 0 and sensible ✓
- [ ] All statistical tests passed ✓
- [ ] All decision logs complete ✓
- [ ] Dashboard loads all data ✓
- [ ] No crashes under any scenario ✓
- [ ] Reproducibility verified (seeded) ✓

**Deliverable**: All tests passing, system ready for production

---

## PHASE 10: DOCUMENTATION (Days 28-30)

### 10.1 Update README.md
- [ ] Add section: "ELITE Upgrade (v2.0)"
- [ ] Add architecture diagram
- [ ] Add success metrics (latency, throughput, stability)
- [ ] Add experiment running instructions
- [ ] Update quick start with new services

### 10.2 Architecture Diagrams
- [ ] Component interaction diagram (visual)
- [ ] Data flow (baseline vs adaptive)
- [ ] Middleware decision chain flowchart
- [ ] Metrics pipeline flowchart
- [ ] Comparison report generation flowchart

### 10.3 Experiment Running Guide
- [ ] Prerequisites (PostgreSQL, Redis, Kafka running)
- [ ] Step 1: Configure experiment (YAML file)
- [ ] Step 2: Start experiment (`python experiment_manager.py start`)
- [ ] Step 3: Monitor dashboard (`http://localhost:3000`)
- [ ] Step 4: Wait for completion (2 hours)
- [ ] Step 5: View results (`http://localhost:3000/experiments/{id}/results`)
- [ ] Step 6: Download PDF report

### 10.4 Metrics Interpretation Guide
- [ ] What does latency p95 mean?
- [ ] How to read percentile charts?
- [ ] What is a good precision/recall?
- [ ] What does false positive rate mean?
- [ ] Statistical significance explanation
- [ ] Effect size interpretation (Cohen's d)

### 10.5 Troubleshooting Runbook
- [ ] Issue: Dashboard shows no metrics
  - [ ] Check Redis connection
  - [ ] Check Kafka topics exist
  - [ ] Check metrics_collector running
- [ ] Issue: Circuit breaker stuck OPEN
  - [ ] Check ML model health
  - [ ] Check error logs
  - [ ] Manual reset: `/api/control/circuit-breaker/close`
- [ ] Issue: Queue depth growing
  - [ ] Check consumer lag
  - [ ] Check if processing bottlenecked
  - [ ] Verify backpressure shedding
- [ ] Issue: Experiment not starting
  - [ ] Check PostgreSQL connection
  - [ ] Check experiment config
  - [ ] Check log files for errors

### 10.6 Deployment Checklist
- [ ] PostgreSQL running and accessible
- [ ] Kafka running with topics created
- [ ] Redis running
- [ ] All services building successfully
- [ ] Environment variables set (KAFKA_BOOTSTRAP, REDIS_URL, etc.)
- [ ] Docker-compose.yml updated with PostgreSQL service
- [ ] Seed scripts for initial data
- [ ] All ports available (3000, 8000, 9092, 6379, 5432)

### 10.7 Update docker-compose.yml
- [ ] Add PostgreSQL service
  - [ ] Image: postgres:14
  - [ ] Environment: POSTGRES_PASSWORD
  - [ ] Volume for persistence
  - [ ] Port: 5432
- [ ] Add migration initialization (db schema)
- [ ] Verify all services start in correct order

### 10.8 Create Seed Scripts
- [ ] Script 1: Initialize PostgreSQL schema
- [ ] Script 2: Create sample experiment config
- [ ] Script 3: Load training data (ML models)
- [ ] Run: `docker-compose up` → seeds execute automatically

### 10.9 API Documentation
- [ ] Export OpenAPI spec to YAML
- [ ] Create markdown docs for each endpoint
- [ ] Add curl examples for common operations
- [ ] Add WebSocket usage examples

**Deliverable**: Complete documentation + deployment guide

---

## FINAL VALIDATION

### Pre-Deployment Checks
- [ ] All phases complete
- [ ] All unit tests passing
- [ ] All integration tests passing
- [ ] All E2E tests passing
- [ ] Code review completed
- [ ] Security audit (if applicable)
- [ ] Performance profiling done
- [ ] Documentation complete and accurate

### Deployment
- [ ] Provision production PostgreSQL
- [ ] Configure production environment variables
- [ ] Deploy all services to production
- [ ] Verify all endpoints accessible
- [ ] Run smoke tests
- [ ] Monitor logs for errors

### Post-Deployment
- [ ] Run full baseline experiment (1 hour)
- [ ] Run full adaptive experiment (1 hour)
- [ ] Generate and review comparison report
- [ ] Verify all metrics and dashboards
- [ ] Present results to stakeholders
- [ ] Archive all experiment data

---

## PROJECT COMPLETION CHECKLIST

- [ ] Phase 1: Foundation (DB + experiment manager) ✓
- [ ] Phase 2: Failure simulation (4 scenarios) ✓
- [ ] Phase 3: Middleware hardening (resilience patterns) ✓
- [ ] Phase 4: Metrics (comprehensive tracking) ✓
- [ ] Phase 5: Model health (monitoring + alerts) ✓
- [ ] Phase 6: Backend API (all endpoints) ✓
- [ ] Phase 7: Dashboard (5 tabs, live data) ✓
- [ ] Phase 8: Report generation (PDF + statistics) ✓
- [ ] Phase 9: E2E testing (all scenarios passing) ✓
- [ ] Phase 10: Documentation (complete + accurate) ✓

### Proof Generation
- [ ] Baseline experiment completed ✓
- [ ] Adaptive experiment completed ✓
- [ ] Statistical comparison report generated ✓
- [ ] All metrics > 0 and sensible ✓
- [ ] P-values reported for all claims ✓
- [ ] Effect sizes (Cohen's d) calculated ✓
- [ ] Confidence intervals provided ✓
- [ ] PDF report generated + reviewed ✓

### Success Metrics Achieved
- [ ] Latency improvement: ≥ 15% (statistically significant) ✓
- [ ] Throughput improvement: ≥ 10% (drop rate < 0.5%) ✓
- [ ] System stability: 0 crashes at 10x load ✓
- [ ] Decision quality: Precision > 85%, Recall > 90% ✓
- [ ] All metrics reproducible (seeded) ✓
- [ ] All decisions logged with full context ✓

### ELITE Status Achieved
- [ ] ✓ Experiment framework (baseline vs adaptive)
- [ ] ✓ Realistic failure scenarios (RAMP, PATTERN, BOTTLENECK, CASCADING)
- [ ] ✓ Production-grade resilience (circuit breaker, backpressure, DLQ)
- [ ] ✓ Comprehensive metrics (10+ dimensions)
- [ ] ✓ Explainable decisions (structured logging + reasoning)
- [ ] ✓ Statistical rigor (significance testing, confidence intervals)
- [ ] ✓ Visual proof (dashboard + comparison charts)
- [ ] ✓ Production readiness (documentation + deployment guide)

---

**Status**: Ready for Implementation | May 25, 2026 (30 days to completion)
