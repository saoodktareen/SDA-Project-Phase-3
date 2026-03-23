# Phase 3 — Generic Concurrent Real-Time Pipeline

## 1. Main File
The main entry point is:

    main.py  (located at the project root)

To run the pipeline:

    python main.py

No command-line arguments are needed. Everything is driven by config.json.

---

## 2. Where to Place Files
Before running, place the following in the correct locations:

| File                | Location                        |
|---------------------|---------------------------------|
| Your dataset (CSV)  | data/<your_filename>.csv        |
| Configuration file  | config.json (root of project)   |

The `data/` folder already exists. Drop the unseen CSV file into it.

Then open `config.json` and update the `"dataset_path"` field:

    "dataset_path": "data/<your_filename>.csv"

That is the only change required. Everything else — column names, types,
processing parameters, chart layout, and parallelism — is controlled by config.json.

---

## 3. Project Structure
project_root/
│
├── main.py                   ← START HERE — central orchestrator
├── config.json               ← pipeline configuration (edit this)
├── readme.md                 ← this file
│
├── data/
│   └── <your_dataset>.csv    ← place the unseen CSV here
│
├── config/
│   ├── init.py
│   ├── read_config.py        ← loads and validates config.json
│   └── validate_config.py    ← full structural validation of config
│
├── core/
│   ├── init.py
│   ├── functional_core.py    ← pure functions: verify_signature, compute_average
│   └── imperative_shell.py   ← CoreWorker (parallel) + Aggregator (stateful)
│
└── plugins/
├── init.py
├── input_module.py       ← CSV reader, schema mapper, packet producer
├── output_module.py      ← two-screen matplotlib dashboard
└── telemetry.py          ← Observer pattern: queue health monitoring

---

## 4. Dependencies
Install required packages:

    pip install pandas matplotlib

Standard library modules used (no installation needed):
- multiprocessing, threading, hashlib, heapq, queue
- collections, time, json, sys, abc

---

## 5. How the Pipeline Works
The pipeline runs as 7+ concurrent processes wired together by main.py.  
Data flows through three bounded queues:
InputModule → Q1 → CoreWorkers (parallel) → Q2  →  Aggregator → Q3 → OutputModule
                                                │
                                      PipelineTelemetry Thread


**Execution steps:**
1. `main.py` loads and validates config.json.
2. Three bounded queues (Q1, Q2, Q3) are created.
3. Processes start in consumer-first order: Output → Aggregator → CoreWorkers → Input.
4. Dashboard opens immediately (Status Screen).
5. InputModule validates CSV, maps schema, and streams packets into Q1.
6. Screen shows GREEN/AMBER/RED depending on validation state.
7. On “▶ Start Pipeline”, data flows through workers, aggregator, and output.
8. CoreWorkers verify signatures in parallel.
9. Aggregator re-sequences packets and computes running average.
10. OutputModule updates charts every 150ms.
11. Telemetry monitors queue health.
12. Sentinels (`None`) propagate to cleanly terminate all processes.

---

## 6. Configuration Reference (config.json)
Key fields:

- **dataset_path**: Path to CSV file.  
- **pipeline_dynamics**: Controls input delay, parallelism, queue size, telemetry interval.  
- **schema_mapping**: Maps CSV columns to internal keys (`metric_value`, `security_hash`, `time_period`, `entity_name`).  
- **processing**: Stateless (signature verification) and stateful (running average).  
- **visualizations**: Telemetry bars and chart definitions.

**Supported chart types:**
- real_time_line_graph_values  
- real_time_line_graph_average  
- real_time_bar_chart_values  
- real_time_bar_chart_average  
- real_time_scatter_values  
- real_time_scatter_average  

---

## 7. Quick Setup Checklist
1. Unzip the project folder.  
2. Copy unseen CSV file into `data/`.  
3. Update `config.json` with dataset path and secret key.  
4. Install dependencies (`pandas`, `matplotlib`).  
5. Run `python main.py`.  
6. Wait for validation screen.  
7. If GREEN/AMBER, click “▶ Start Pipeline”.  
8. Observe real-time charts and telemetry.  

---

## 8. Design Patterns Implemented
- Dependency Inversion (DIP)  
- Producer-Consumer  
- Scatter-Gather  
- Functional Core / Imperative Shell  
- Observer Pattern  
- Priority Queue (Min-Heap)  
- Poison Pill / Sentinel  

---

## 9. Important Notes
- Source code is domain-agnostic.  
- `secret_key` must match dataset signature key.  
- On Windows, multiprocessing uses `spawn` — `if __name__ == "__main__":` guard is required.  
- Closing dashboard before start still exits cleanly.  
- Queue backpressure is automatic via bounded queues.  

