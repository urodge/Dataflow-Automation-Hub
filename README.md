# ⚡ DataFlow Automation Hub — Weather ETL Pipeline

> A beginner-to-intermediate **ETL automation pipeline** built with Python, Apache Airflow, SQL, and cloud-ready architecture. Designed to demonstrate core skills required for Automation Engineer roles.

![Python](https://img.shields.io/badge/Python-3.10+-blue?style=flat-square&logo=python)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7-017CEE?style=flat-square&logo=apacheairflow)
![SQLite](https://img.shields.io/badge/SQLite-Database-003B57?style=flat-square&logo=sqlite)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Transform-150458?style=flat-square&logo=pandas)
![GitHub](https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square)

---

## 📌 What This Project Does

This pipeline automatically:
1. **Extracts** live weather data from a public REST API
2. **Transforms** and cleans it using Python + Pandas
3. **Loads** the results into a SQL database
4. **Schedules** the whole workflow daily using Apache Airflow

It simulates a real-world data engineering pipeline — the same type of system used by cloud automation engineers at scale.

---

## 🗂 Project Structure

```
dataflow-automation-hub/
│
├── dags/
│   └── weather_pipeline_dag.py   # Airflow DAG — orchestrates all tasks
│
├── pipeline/
│   ├── extract.py                # Pulls data from weather API
│   ├── transform.py              # Cleans & reshapes with Pandas
│   └── load.py                   # Writes to SQLite database
│
├── sql/
│   └── queries.sql               # Analysis queries (SELECT, GROUP BY, JOIN)
│
├── logs/                         # Auto-generated Airflow run logs
├── data/
│   ├── raw_weather.csv           # Raw API output
│   └── clean_weather.csv         # Transformed data
│
├── weather.db                    # SQLite database (auto-created)
├── requirements.txt              # Python dependencies
├── docker-compose.yml            # Run Airflow locally via Docker
└── README.md
```

---

## 🛠 Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.10+ | Core scripting & automation |
| Apache Airflow | Workflow orchestration & scheduling |
| Pandas | Data transformation & cleaning |
| SQLite | Local SQL database |
| Requests | REST API calls |
| Docker | Containerised Airflow setup |
| Git / GitHub | Version control |
| GCP Cloud Composer *(stretch)* | Cloud-based Airflow deployment |

---

## 🚀 Quick Start

### 1. Clone the repo
```bash
git clone https://github.com/urodge/dataflow-automation-hub.git
cd dataflow-automation-hub
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the pipeline manually (no Airflow needed)
```bash
python pipeline/extract.py
python pipeline/transform.py
python pipeline/load.py
```

### 4. Run with Airflow (Docker)
```bash
docker-compose up -d
# Open http://localhost:8080
# Username: airflow | Password: airflow
# Trigger DAG: weather_etl_pipeline
```

---

## 🔁 Pipeline Architecture

```
  ┌─────────────┐     ┌──────────────┐     ┌─────────────┐
  │   EXTRACT   │────▶│  TRANSFORM   │────▶│    LOAD     │
  │  wttr.in    │     │   Pandas     │     │   SQLite    │
  │   REST API  │     │  clean/shape │     │  weather.db │
  └─────────────┘     └──────────────┘     └─────────────┘
         │                                        │
         ▼                                        ▼
   raw_weather.csv                      SQL queries + reports

  ┌──────────────────────────────────────────────────────┐
  │          Apache Airflow DAG — runs daily @ 06:00     │
  │   extract_task >> transform_task >> load_task        │
  └──────────────────────────────────────────────────────┘
```

---

## 🗄 Sample SQL Queries

```sql
-- Average temperature per city
SELECT city, ROUND(AVG(temp_c), 2) AS avg_temp, COUNT(*) AS records
FROM weather
GROUP BY city;

-- Pipeline audit: rows loaded per day
SELECT DATE(loaded_at) AS run_date, COUNT(*) AS rows_loaded
FROM weather
GROUP BY DATE(loaded_at)
ORDER BY run_date DESC;
```

---

## 📅 Development Roadmap

- [x] Week 1 — Python basics + API calls (`extract.py`)
- [x] Week 2 — Pandas data cleaning (`transform.py`)
- [x] Week 3 — SQL + SQLite database (`load.py` + queries)
- [ ] Week 4 — Apache Airflow DAG + scheduling
- [ ] Week 5 — GitHub publish + README + Docker
- [ ] Week 6 — GCP Cloud Composer deployment *(stretch)*

---

## 💡 Key Concepts Demonstrated

- **ETL Pipeline Design** — real-world extract → transform → load pattern
- **Task Orchestration** — Airflow DAGs with task dependencies (`>>` operator)
- **Cron Scheduling** — `"0 6 * * *"` = run daily at 6am UTC
- **Error Handling** — `try/except` blocks + Python `logging` module
- **SQL Analytics** — `GROUP BY`, `WHERE`, `ORDER BY`, aggregate functions
- **Data Validation** — null checks, type casting, range filtering

---

## 📖 What I Learned

This project was built as a self-directed learning exercise to develop automation engineering skills including:
- Writing modular Python scripts for data pipelines
- Using Pandas for real-world data cleaning
- Understanding Airflow DAG structure and scheduling
- Writing analytical SQL queries
- Using Git for version control and project documentation

---
## 📬 Contact
**Uddhav Rodge** — [LinkedIn](https://linkedin.com/in/uddhav-rodge-1501b7274/) · [GitHub](https://github.com/urodge)
