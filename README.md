# Real-Time Ride-Sharing Analytics with Apache Spark

## Overview
This project implements a **real-time analytics pipeline** for a ride-sharing platform using **Apache Spark Structured Streaming**. The pipeline processes streaming data, performs real-time aggregations, and analyzes trends over time.

## Technologies Used
- **Apache Spark (Structured Streaming)**
- **Python**
- **JSON (Streaming Data Format)**
- **CSV (Output Storage)**
- **Socket Streaming**

## Project Structure
```
├── task1.py  # Ingest and parse real-time ride data
├── task2.py  # Perform real-time aggregations
├── task3.py  # Windowed time-based analytics
├── generator.py  # Simulates real-time ride-sharing data
├── README.md  # Project documentation
└── output/   # Stores the output CSV files
```

---

## Installation
Before running the project, install the required dependencies:
```bash
pip install faker
pip install pyspark
```

---

## Task 1: Basic Streaming Ingestion and Parsing
### Objective:
- Ingest streaming data from a socket (`localhost:9999`).
- Parse JSON messages into a structured DataFrame with columns:
  - `trip_id`, `driver_id`, `distance_km`, `fare_amount`, `timestamp`.

### Steps:
1. Run the Python data generator script to simulate streaming ride data.
2. Execute `task1.py` to:
   - Read data from `localhost:9999`.
   - Parse the JSON into structured columns.
   - Print the parsed data in the console.

---

## Task 2: Real-Time Aggregations (Driver-Level)
### Objective:
- Compute real-time aggregations:
  - **Total fare amount per driver**.
  - **Average trip distance per driver**.
- Store the results in CSV files.

### Steps:
1. Ensure `task1.py` is running to stream parsed data.
2. Execute `task2.py` to:
   - Aggregate `fare_amount` and `distance_km` grouped by `driver_id`.
   - Print results to the console.
   - Store the results in `output/driver_aggregations/`.

---

## Task 3: Windowed Time-Based Analytics
### Objective:
- Perform a **5-minute** sliding window aggregation on `fare_amount` (sliding by 1 minute).

### Steps:
1. Ensure `task1.py` is running.
2. Execute `task3.py` to:
   - Convert the `timestamp` column to a proper `TimestampType`.
   - Apply **Spark's window function**.
   - Store the results in `output/windowed_aggregations/`.

---

## How to Run the Project
### Step 1: Start the Data Generator
Open a terminal and run:
```bash
python generator.py
```

### Step 2: Run Task 1 (Streaming Ingestion)
Open a **new terminal** and run:
```bash
python task1.py
```

### Step 3: Run Task 2 (Aggregations)
Open another **new terminal** and run:
```bash
python task2.py
```

### Step 4: Run Task 3 (Windowed Analytics)
Open yet another **new terminal** and run:
```bash
python task3.py
```

---

## Output Files
- **Driver Aggregations:** `output/driver_aggregations/`
- **Windowed Aggregations:** `output/windowed_aggregations/`

---

## Troubleshooting
### 1. Spark Streaming Process Keeps Running
To stop a running Spark job, use:
```bash
Ctrl + C
```

### 2. Port Already in Use
If you see an error that `localhost:9999` is in use, restart the terminal or change the port number in the scripts.

### 3. CSV Output Not Being Stored
Ensure you have a **valid checkpoint directory** set in `task2.py` and `task3.py`.

---

## Conclusion
By completing this project, you gain hands-on experience in **real-time data ingestion, processing, and analysis** using **Apache Spark Structured Streaming**.

