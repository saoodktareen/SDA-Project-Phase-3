================================================================================
  Phase 3 — Generic Concurrent Real-Time Pipeline
  README
================================================================================

================================================================================
  1. MAIN FILE
================================================================================

  The main file is:

      main.py  (located at the project root)

  To run the pipeline:

      python main.py

  No command-line arguments are needed. Everything is driven by config.json.


================================================================================
  2. WHERE TO PLACE FILES
================================================================================

  Before running, place the following in the correct locations:

  ┌──────────────────────────────┬──────────────────────────────────────────┐
  │ File                         │ Location                                 │
  ├──────────────────────────────┼──────────────────────────────────────────┤
  │ Your dataset (CSV)           │ data/<your_filename>.csv                 │
  │ Configuration file           │ config.json  (root of project)           │
  └──────────────────────────────┴──────────────────────────────────────────┘

  The data/ folder already exists. Simply drop the unseen CSV file into it.

  Then open config.json and update this field to match the unseen dataset:

      "dataset_path": "data/<your_filename>.csv"

  That is the ONLY change required. No source code needs to be touched.


================================================================================
  3. PROJECT STRUCTURE
================================================================================

  project_root/
  │
  ├── main.py                        ← START HERE — central orchestrator
  ├── config.json                    ← pipeline configuration (edit this)
  ├── readme.txt                     ← this file
  │
  ├── data/
  │   └── <your_dataset>.csv         ← place the unseen CSV here
  │
  ├── Architecture/
  │   ├── ClassDiagram.puml          ← PlantUML source — class diagram
  │   ├── ClassDiagram.png           ← exported class diagram image
  │   ├── SequenceDiagram.puml       ← PlantUML source — sequence diagram
  │   └── SequenceDiagram.png        ← exported sequence diagram image
  │
  ├── config/
  │   ├── __init__.py
  │   ├── read_config.py             ← loads and validates config.json
  │   └── validate_config.py         ← full structural validation of config
  │
  ├── core/
  │   ├── __init__.py
  │   ├── functional_core.py         ← pure functions: verify_signature, compute_average
  │   └── imperative_shell.py        ← CoreWorker (parallel) + Aggregator (stateful)
  │
  └── plugins/
      ├── __init__.py
      ├── input_module.py            ← CSV reader, schema mapper, packet producer
      ├── output_module.py           ← two-screen matplotlib dashboard
      └── telemetry.py               ← Observer pattern: queue health monitoring


================================================================================
  4. DEPENDENCIES
================================================================================

  Install all required packages with:

      pip install pandas matplotlib

  Standard library modules used (no installation needed):
    - multiprocessing, threading, hashlib, heapq, queue,
      collections, time, json, sys, abc


================================================================================
  5. HOW THE PIPELINE WORKS
================================================================================

  The pipeline runs as 7+ concurrent processes wired together by main.py.
  Data flows through three bounded queues:

      InputModule
          │  (raw_stream Q1 — bounded, max size from config)
          ▼
      CoreWorker-0 ─┐
      CoreWorker-1 ─┤  (parallel signature verification — Scatter)
      CoreWorker-2 ─┤
      CoreWorker-3 ─┘
          │  (intermediate_queue Q2 — bounded)
          ▼
      Aggregator
          │  (re-sequences, computes sliding window average — Gather)
          │  (processed_queue Q3 — bounded)
          ▼
      OutputModule (Dashboard)
          │
      PipelineTelemetry Thread (polls Q1/Q2/Q3 sizes for health bars)


  STEP-BY-STEP EXECUTION:

  1.  main.py loads config.json via read_config.py. validate_config.py checks
      every field and exits with a clear message if anything is invalid.

  2.  Three bounded multiprocessing.Queue instances are created (Q1, Q2, Q3).
      Queue.put() blocks automatically when full — this IS the backpressure.

  3.  Processes start in consumer-first order:
        Output → Aggregator → CoreWorkers → Input

  4.  The dashboard (Screen 1 — Status Screen) opens immediately showing
      "Validating..." while InputModule checks the CSV in the background.

  5.  InputModule validates CSV columns, casts types, applies internal mapping,
      adds priority_index, then fires ready_event so Screen 1 redraws.

  6.  Screen 1 shows one of three states:
        - GREEN  (no errors)    → button shown, user can start.
        - AMBER  (row warnings) → skipped rows shown in table, button shown.
        - RED    (fatal errors) → error messages shown, button HIDDEN.

  7.  When the user clicks "▶ Start Pipeline":
        - start_event is set — unblocks InputModule and Aggregator.
        - Screen 1 is replaced by Screen 2 (live charts + telemetry).
        - FuncAnimation begins updating the dashboard every 150ms.

  8.  N CoreWorkers pull from Q1 in parallel, call verify_signature()
      (pure PBKDF2-HMAC). Verified packets go to Q2. Failed packets are dropped.

  9.  Aggregator re-sequences packets via min-heap, computes running average
      via compute_average() (pure function), attaches computed_metric, pushes
      to Q3. Cutoff timer skips dropped packets so pipeline never stalls.

  10. OutputModule drains Q3 every 150ms and updates all configured charts.
      When the final None sentinel arrives, charts are marked "✔ Complete".

  11. PipelineTelemetry polls Q1/Q2/Q3 sizes and color-codes health bars:
        - Green  → fill ratio < 40%  (flowing smoothly)
        - Yellow → fill ratio 40-75% (filling up)
        - Red    → fill ratio > 75%  (heavy backpressure)

  12. After InputModule finishes, main.py pushes N None sentinels into Q1.
      Each worker forwards one to Q2. Aggregator drains heap and sends one
      to Q3. Output marks stream complete. All processes exit cleanly.


================================================================================
  6. CONFIGURATION REFERENCE (config.json)
================================================================================

  Key fields:

    "dataset_path"       : Path to CSV file relative to project root.
    "input_delay_seconds": Seconds to sleep between packets. Lower = faster.
    "core_parallelism"   : Number of parallel CoreWorker processes (>= 1).
    "stream_queue_max_size": Max items per queue. Controls backpressure.
    "secret_key"         : Must exactly match the key used to sign the dataset.
    "running_average_window_size": Number of recent values to average over.

  Supported chart types:
    - real_time_line_graph_values     - real_time_line_graph_average
    - real_time_bar_chart_values      - real_time_bar_chart_average
    - real_time_scatter_values        - real_time_scatter_average

  NOTE: "y_axis" in data_charts is a display label only. The actual data
  plotted is determined by the chart type (types ending in "_values" plot
  metric_value, types ending in "_average" plot computed_metric).


================================================================================
  7. QUICK SETUP CHECKLIST FOR LIVE GRADING
================================================================================

  [ ] 1. Unzip the project folder.
  [ ] 2. Copy the unseen CSV file into the  data/  folder.
  [ ] 3. Open config.json and update:
             "dataset_path": "data/<unseen_file_name>.csv"
             "secret_key":   "<key provided for the unseen dataset>"
         (Replace other fields too if a custom config.json is provided.)
  [ ] 4. Install dependencies if not already present:
             pip install pandas matplotlib
  [ ] 5. Run:
             python main.py
  [ ] 6. A matplotlib window opens showing the Status Screen.
         Wait for validation to complete (a few seconds).
  [ ] 7. If the screen shows GREEN or AMBER, click "▶ Start Pipeline".
         The live dashboard (Screen 2) will appear and data will flow.
  [ ] 8. Observe real-time charts and queue health telemetry bars.
         Close the window when done — all processes exit cleanly.


================================================================================
  8. DESIGN PATTERNS IMPLEMENTED
================================================================================

  Pattern                      Location                  Purpose
  ───────────────────────────────────────────────────────────────────────────
  Dependency Inversion (DIP)   All modules               Modules depend only on
                                                          queues + config dicts,
                                                          never on each other.

  Producer-Consumer            main.py + all modules     Input produces into Q1,
                                                          workers consume Q1 and
                                                          produce into Q2, etc.

  Scatter-Gather               CoreWorker + Aggregator   N workers scatter across
                                                          packets in parallel;
                                                          Aggregator gathers and
                                                          re-sequences results.

  Functional Core /            functional_core.py        verify_signature() and
  Imperative Shell             imperative_shell.py       compute_average() are
                                                          pure functions (no state).
                                                          Aggregator owns all state.

  Observer Pattern             telemetry.py +            PipelineTelemetry (Subject)
                               output_module.py          polls queue sizes and
                                                          notifies OutputModule
                                                          (Observer) via update().

  Priority Queue (Min-Heap)    Aggregator._try_release() heapq re-sequences
                                                          out-of-order verified
                                                          packets by priority_index.

  Poison Pill / Sentinel       main.py                   None values cleanly
                                                          terminate each worker,
                                                          Aggregator, and Output.


================================================================================
  9. IMPORTANT NOTES
================================================================================

  - The source code of Input, Core, and Output modules is domain-agnostic.
    They operate entirely on the generic internal_mapping keys from config.

  - The secret_key in config.json MUST exactly match the key used to generate
    the Auth_Signature column in the CSV. A mismatch causes all packets to fail
    verification and be dropped — the charts will show no data.

  - On Windows, multiprocessing uses the 'spawn' start method. The
    if __name__ == "__main__": guard in main.py is required and must not
    be removed.

  - If the dashboard window is closed before clicking "▶ Start Pipeline",
    main.py sets start_event at the end so all waiting processes exit cleanly
    without requiring Ctrl+C.

  - Queue backpressure is automatic: bounded queues block put() when full.
    No extra throttling code is needed.

================================================================================