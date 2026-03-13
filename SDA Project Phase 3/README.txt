================================================================
  Phase 3 — Generic Concurrent Real-Time Pipeline
  README
================================================================

MAIN FILE
---------
  main.py

  Run with:
      python main.py

  No command-line arguments needed. All behaviour is driven
  by config.json.

----------------------------------------------------------------

PROJECT STRUCTURE
-----------------
  phase3/
  ├── main.py              ← Entry point (run this file)
  ├── config.json          ← Pipeline configuration
  ├── readme.txt           ← This file
  ├── input_module.py      ← Generic CSV producer
  ├── core_module.py       ← Verification workers + aggregator
  ├── output_module.py     ← Live matplotlib dashboard
  ├── telemetry.py         ← Observer pattern telemetry monitor
  └── data/
      └── sample_sensor_data.csv   ← Place dataset here

----------------------------------------------------------------

DATA & CONFIG PLACEMENT (FOR GRADER)
--------------------------------------
  1. Place the unseen CSV dataset inside the data/ folder.

  2. Open config.json and update the following fields:

     "dataset_path" → path to your CSV file
                      e.g. "data/unseen_climate_data.csv"

     "schema_mapping" → update each column entry:
       "source_name"      → exact column header in your CSV
       "internal_mapping" → leave as-is OR update to match
       "data_type"        → "string", "integer", or "float"

     "processing.stateless_tasks.secret_key"
                          → the secret key used to sign your data

     All other settings (parallelism, queue sizes, window size,
     chart titles, axis labels) can also be changed in config.json
     without touching any Python source files.

  3. Run:  python main.py

----------------------------------------------------------------

REQUIREMENTS
------------
  Python 3.8+

  Standard library only + ONE extra package:
    pip install dash plotly


----------------------------------------------------------------

PIPELINE OVERVIEW
-----------------
  Input Process
    Reads CSV row by row → maps columns via schema_mapping
    → assigns sequential packet_id → pushes to Queue 1

  Core Workers (N parallel processes, N = core_parallelism)
    Pull from Queue 1 → verify PBKDF2 signature
    → drop invalid packets → push (id, packet) to Queue 2

  Aggregator Process
    Pulls from Queue 2 → re-sequences using min-heap priority queue
    → applies cutoff timeout for dropped packets
    → computes sliding window running average
    → pushes completed packets to Queue 3

  Output Process (Dashboard)
    Consumes Queue 3 → renders live line charts
    Telemetry thread polls queue sizes → color-coded health bars
      Green  = queue < 40% full
      Yellow = queue 40-75% full
      Red    = queue > 75% full (backpressure)

----------------------------------------------------------------

NOTES
-----
  - Close the dashboard window to end the program cleanly.
  - All processes shut down via sentinel (None) signals —
    no force-killing required.
  - Console logs from all processes print to the terminal
    in real time for debugging.

================================================================