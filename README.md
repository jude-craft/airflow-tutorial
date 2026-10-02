# Airflow Tutorial

A structured, hands-on tutorial project for learning Apache Airflow 3.x. This repository covers core Airflow concepts progressively, from writing a first DAG to advanced patterns such as asset-driven scheduling, branching, parallel execution, and multi-DAG orchestration.

The environment is fully containerized using Docker Compose with a CeleryExecutor backend, PostgreSQL metadata database, and Redis as the message broker.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [Running the Project](#running-the-project)
- [DAG Reference](#dag-reference)
  - [1. First DAG](#1-first-dag)
  - [2. DAG Versioning](#2-dag-versioning)
  - [3. Operators](#3-operators)
  - [4. XComs - Automatic Passing](#4-xcoms---automatic-passing)
  - [5. XComs - Manual Push and Pull](#5-xcoms---manual-push-and-pull)
  - [6. Parallel Tasks](#6-parallel-tasks)
  - [7. Conditional Branching](#7-conditional-branching)
  - [8. Schedule Presets](#8-schedule-presets)
  - [9. Cron-Based Scheduling](#9-cron-based-scheduling)
  - [10. Delta-Based Scheduling](#10-delta-based-scheduling)
  - [11. Incremental Load](#11-incremental-load)
  - [12. Event-Based Scheduling](#12-event-based-scheduling)
  - [13. Asset Producer](#13-asset-producer)
  - [14. Asset Consumer](#14-asset-consumer)
  - [DAG Orchestration](#dag-orchestration)
- [Key Concepts Covered](#key-concepts-covered)
- [Infrastructure Reference](#infrastructure-reference)
- [Configuration Reference](#configuration-reference)
- [Security Notes](#security-notes)

---

## Project Overview

This tutorial walks through the major building blocks of Apache Airflow 3.x using real, runnable DAGs. Each DAG file is numbered sequentially and introduces one or two new concepts on top of the previous one. By the end, you will have practical experience with:

- The TaskFlow API and the decorator-based authoring style
- Task dependencies and execution order
- XCom-based data passing between tasks
- Parallel task execution and fan-out/fan-in patterns
- Conditional branching with `@task.branch`
- All major scheduling strategies available in Airflow 3.x
- Incremental data loading using data interval context
- Asset-driven scheduling (data-aware DAGs)
- Multi-DAG orchestration using `TriggerDagRunOperator`

---

## Technology Stack

| Component | Version / Detail |
|---|---|
| Apache Airflow | 3.3.2 |
| Python | 3.14 |
| Executor | CeleryExecutor |
| Message Broker | Redis 7.2 |
| Metadata Database | PostgreSQL 16 |
| Package Manager | uv |
| Containerization | Docker Compose |

---

## Project Structure

```
airflow-tutorial/
├── dags/                          # All DAG definitions
│   ├── 1_first_dag.py             # Basic DAG with sequential tasks
│   ├── 2_dag_versioning.py        # DAG versioning demonstration
│   ├── 3_operators.py             # Python and Bash operators
│   ├── 4_XCOMs_auto.py            # Automatic XCom passing via return values
│   ├── 5_XCOMs_kwargs.py          # Manual XCom push/pull via kwargs
│   ├── 6_parallel_task.py         # Fan-out / fan-in parallel execution
│   ├── 7_conditioned_parallel.py  # Branching with parallel upstream tasks
│   ├── 8_schedule_preset.py       # Scheduling with built-in presets
│   ├── 9_schedule_cron.py         # Cron-based scheduling with CronTriggerTimetable
│   ├── 10_schedule_delta.py       # Interval scheduling with DeltaTriggerTimetable
│   ├── 11_incremental_load.py     # Incremental loading with data interval context
│   ├── 12_special_schedule.py     # Event-driven scheduling with EventsTimetable
│   ├── asset_13.py                # Asset producer DAG
│   ├── 14_asset_dependent.py      # Asset consumer DAG triggered by asset_13
│   ├── dag_orchestrate_1.py       # Child DAG 1 for orchestration
│   ├── dag_orchestrate_2.py       # Child DAG 2 for orchestration
│   └── dag_orchestrate_parent.py  # Parent DAG that triggers child DAGs
├── src/
│   └── airflow_tutorial/
│       └── __init__.py            # Python package entry point
├── config/
│   └── airflow.cfg                # Airflow configuration file
├── logs/                          # Task and scheduler logs (gitignored)
├── plugins/                       # Custom Airflow plugins directory
├── docker-compose.yaml            # Full Airflow stack definition
├── pyproject.toml                 # Project metadata and Python dependencies
├── .python-version                # Pin: Python 3.14
├── .env                           # Environment variables (gitignored)
└── .gitignore
```

---

## Prerequisites

Before setting up this project, ensure the following are installed and available on your machine:

- **Docker** (version 24 or later)
- **Docker Compose** (version 2.x or later, included with Docker Desktop)
- **uv** (Python package manager) — install via the [official installer](https://docs.astral.sh/uv/)
- Minimum system resources for Docker: **4 GB RAM**, **2 CPUs**, **10 GB free disk space**

---

## Environment Setup

**1. Clone the repository**

```bash
git clone https://github.com/jude-craft/airflow-tutorial.git
cd airflow-tutorial
```

**2. Create the `.env` file**

The `.env` file is excluded from version control. Create it in the project root with the following variables:

```bash
# The UID of your host user. Run `id -u` on Linux to get this value.
AIRFLOW_UID=1000

# Fernet key used to encrypt sensitive data in the metadata database.
# Generate a new key with:
# python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
FERNET_KEY=<your-fernet-key>
```

> **Important:** Never commit the `.env` file. It is listed in `.gitignore` to prevent accidental exposure of secrets.

**3. Install Python dependencies (optional, for local development outside Docker)**

```bash
uv sync
```

---

## Running the Project

**Start the full Airflow stack:**

```bash
docker compose up -d
```

The `airflow-init` service runs first. It performs database migrations, creates required directories, and provisions the default admin user. All other services wait until this completes.

**Access the Airflow UI:**

```
URL:      http://localhost:8080
Username: techwhiz@airflow
Password: airflow
```

**Stop the stack:**

```bash
docker compose down
```

**Stop and remove all data volumes (full reset):**

```bash
docker compose down --volumes --remove-orphans
```

**Monitor Celery workers with Flower (optional):**

```bash
docker compose --profile flower up -d
# Access at http://localhost:5555
```

**Run Airflow CLI commands:**

```bash
docker compose --profile debug run --rm airflow-cli airflow dags list
```

---

## DAG Reference

All DAGs are located in the `dags/` directory and are automatically picked up by the Airflow DAG processor.

---

### 1. First DAG

**File:** `dags/1_first_dag.py`
**DAG ID:** `first_dag`

The entry point of the tutorial. Demonstrates the minimum viable structure of an Airflow DAG using the TaskFlow API.

**Concepts introduced:**
- The `@dag` decorator
- The `@task.python` decorator
- Sequential task dependency using the `>>` bitshift operator
- DAG instantiation by calling the decorated function

**Task flow:**

```
first_task >> second_task >> third_task
```

---

### 2. DAG Versioning

**File:** `dags/2_dag_versioning.py`
**DAG ID:** `versioned_dag`

Builds on the first DAG by adding a fourth task, demonstrating how Airflow handles DAG updates. When a new task is added to an existing DAG, Airflow preserves prior run history and tracks the structural change.

**Concepts introduced:**
- DAG versioning behavior in Airflow 3.x
- Adding tasks to an existing DAG without breaking historical runs

**Task flow:**

```
first_task >> second_task >> third_task >> version_task
```

---

### 3. Operators

**File:** `dags/3_operators.py`
**DAG ID:** `operators_dag`

Demonstrates two ways to use the Bash operator: the modern decorator style (`@task.bash`) and the classic `BashOperator` class from the provider package.

**Concepts introduced:**
- `@task.bash` decorator — the TaskFlow-style Bash task; the decorated function returns a shell command string which Airflow executes
- `BashOperator` — the traditional operator class approach, imported from `airflow.providers.standard.operators.bash`
- Mixing TaskFlow tasks with classic operators in the same DAG

**Task flow:**

```
first_task >> second_task >> bash_task_modern >> bash_task_old_school
```

---

### 4. XComs - Automatic Passing

**File:** `dags/4_XCOMs_auto.py`
**DAG ID:** `xcoms_dag_auto`

Simulates a basic ETL pipeline (Extract, Transform, Load) where data flows between tasks automatically. In the TaskFlow API, any value returned by a `@task.python` function is automatically pushed to XCom. Passing the task call result as an argument to the next task creates an implicit XCom pull and establishes a task dependency.

**Concepts introduced:**
- Automatic XCom push via `return`
- Automatic XCom pull by passing a task's result as a function argument
- Implicit dependency declaration through data flow

**Task flow:**

```
first_task (extract) >> second_task (transform) >> third_task (load)
```

**Data flow:**

| Step | Data |
|---|---|
| Extract | `{"data": [1, 2, 3, 4, 5]}` |
| Transform | `{"trans_data": [1, 2, 3, 4, 5, 1, 2, 3, 4, 5]}` (list doubled) |
| Load | Receives and returns the transformed dict |

---

### 5. XComs - Manual Push and Pull

**File:** `dags/5_XCOMs_kwargs.py`
**DAG ID:** `xcoms_dag_kwargs`

Demonstrates manual, explicit control over XCom operations using the `TaskInstance` object. This approach is useful when you need to push multiple named values from a single task or pull values from a specific upstream task by key.

**Concepts introduced:**
- Accessing the `TaskInstance` object via `**kwargs['ti']`
- `ti.xcom_push(key, value)` — explicitly push a named value to XCom
- `ti.xcom_pull(task_ids, key)` — explicitly pull a named value from a specific task

**Task flow:**

```
first_task >> second_task >> third_task
```

**Note:** Because no argument passing is used between tasks, dependencies must be declared explicitly with `>>`.

---

### 6. Parallel Tasks

**File:** `dags/6_parallel_task.py`
**DAG ID:** `parallel_dag`

Implements a fan-out / fan-in ETL pattern where a single extract task produces data that is consumed independently by three parallel transform tasks, before converging into a single load task.

**Concepts introduced:**
- Fan-out dependency using a list: `extract >> [t1, t2, t3]`
- Fan-in dependency: `[t1, t2, t3] >> load`
- Parallel execution of independent tasks by the CeleryExecutor
- Accessing keyed XCom data from a shared upstream task

**Task flow:**

```
                      >> transform_task_api >>
extract_task (xcom)   >> transform_task_db  >> load_task
                      >> transform_task_s3  >>
```

**Data transformations:**

| Source | Input | Operation |
|---|---|---|
| API | `[1, 2, 3]` | Multiply each element by 10 |
| DB | `[4, 5, 6]` | Multiply each element by 100 |
| S3 | `[7, 8, 9]` | Multiply each element by 1000 |

---

### 7. Conditional Branching

**File:** `dags/7_conditioned_parallel.py`
**DAG ID:** `branch_dag`

Extends the parallel ETL pattern with a branching decision node. After the transformation tasks complete, a `@task.branch` task reads a flag from XCom and routes execution to either a load task or a no-load task based on the condition.

**Concepts introduced:**
- `@task.branch` decorator — defines a task that returns the `task_id` (as a string) of the branch to execute
- Conditional routing based on runtime data
- Tasks not selected by the branch task are automatically marked as skipped

**Task flow:**

```
                      >> transform_task_api >>
extract_task (xcom)   >> transform_task_db  >> decider_task >> load_task
                      >> transform_task_s3  >>              >> no_load_task
```

**Branch logic:**

| Condition | Route |
|---|---|
| `weekend_flag == "true"` | `no_load_task` |
| `weekend_flag == "false"` | `load_task` |

---

### 8. Schedule Presets

**File:** `dags/8_schedule_preset.py`
**DAG ID:** `first_schedule_dag`

Introduces DAG scheduling using Airflow's built-in schedule preset strings.

**Concepts introduced:**
- `start_date` — the earliest date from which the schedule applies
- `schedule` — accepts preset strings: `@daily`, `@hourly`, `@weekly`, `@monthly`, `@yearly`, `@once`, `@never`
- `is_paused_upon_creation=False` — the DAG begins scheduling runs immediately upon deployment
- Timezone-aware `start_date` using `pendulum`

**Configuration:**

| Parameter | Value |
|---|---|
| Schedule | `@daily` |
| Start date | 2026-09-01 (Africa/Nairobi) |
| Paused on creation | No |

---

### 9. Cron-Based Scheduling

**File:** `dags/9_schedule_cron.py`
**DAG ID:** `schedule_cron_dag`

Demonstrates precise scheduling using a cron expression via `CronTriggerTimetable`. This timetable type triggers the DAG at the exact moment the cron expression fires. Unlike `CronDataIntervalTimetable`, it does not define a data interval.

**Concepts introduced:**
- `CronTriggerTimetable` — trigger-based cron scheduling with no data interval
- Cron syntax: `0 16 * * MON-FRI` (every weekday at 4:00 PM)
- `end_date` — the DAG will not schedule runs after this date
- `catchup=True` — Airflow will backfill missed runs between `start_date` and the current time

**Configuration:**

| Parameter | Value |
|---|---|
| Schedule | `0 16 * * MON-FRI` (weekdays at 16:00 EAT) |
| Start date | 2026-09-30 (Africa/Nairobi) |
| End date | 2026-10-01 (Africa/Nairobi) |
| Catchup | True |

---

### 10. Delta-Based Scheduling

**File:** `dags/10_schedule_delta.py`
**DAG ID:** `schedule_delta_dag`

Demonstrates scheduling on a fixed time interval using `DeltaTriggerTimetable`. Instead of a cron pattern, a `pendulum.duration` object defines the cadence between runs.

**Concepts introduced:**
- `DeltaTriggerTimetable` — schedules DAGs on a fixed duration interval
- `pendulum.duration` — a time span object (e.g., `duration(days=3)`)
- Use case: pipelines that must run on a repeating cadence not naturally expressed in cron

**Configuration:**

| Parameter | Value |
|---|---|
| Schedule | Every 3 days |
| Start date | 2026-09-30 (Africa/Nairobi) |
| End date | 2026-10-01 (Africa/Nairobi) |
| Catchup | True |

---

### 11. Incremental Load

**File:** `dags/11_incremental_load.py`
**DAG ID:** `incremental_load_dag`

Demonstrates the standard incremental data loading pattern in Airflow. Each scheduled run receives a `data_interval_start` and `data_interval_end` from the timetable, which the task uses to fetch only the data belonging to that time window.

**Concepts introduced:**
- `CronDataIntervalTimetable` — interval-based cron scheduling; each run is associated with a `data_interval_start` and `data_interval_end`
- `kwargs['data_interval_start']` and `kwargs['data_interval_end']` — context variables injected into each task at runtime
- The distinction between `CronTriggerTimetable` (trigger-only, no interval) and `CronDataIntervalTimetable` (defines a processing window)
- `catchup=True` — enables backfill processing of historical intervals

**Task flow:**

```
incremental_load_fetch >> incremental_load_process
```

---

### 12. Event-Based Scheduling

**File:** `dags/12_special_schedule.py`
**DAG ID:** `special_dates_dag`

Demonstrates scheduling a DAG to run only on a predefined list of specific dates using `EventsTimetable`. This pattern is suited for end-of-quarter reports, public holiday processing, or any pipeline tied to a known, irregular set of dates.

**Concepts introduced:**
- `EventsTimetable` — schedules the DAG to execute only on the listed `event_dates`
- `kwargs['logical_date']` — the context variable representing the current event date

**Scheduled event dates:**

| Date |
|---|
| 2026-09-01 |
| 2026-09-05 |
| 2026-09-14 |
| 2026-10-02 |

---

### 13. Asset Producer

**File:** `dags/asset_13.py`
**Asset name:** `fetch_data`
**Asset URI:** `/opt/airflow/logs/data/data_extract.txt`

Introduces Airflow's data-aware scheduling model using the `@asset` decorator. An asset is a logical representation of a data artifact. When this function completes successfully, Airflow records an asset materialization event that downstream asset consumers can react to.

**Concepts introduced:**
- `@asset` decorator — defines a data asset and the function that produces it
- Asset URI — a logical identifier for the produced data artifact
- Asset-based scheduling with a preset: `schedule="@daily"`

**What it does:**

Ensures the output directory exists, then writes `"Data fetched successfully"` to the file at the configured URI.

---

### 14. Asset Consumer

**File:** `dags/14_asset_dependent.py`
**Asset name:** `process_data`
**Asset URI:** `/opt/airflow/logs/data/data_processed.txt`

Defines a downstream asset that declares a dependency on the `fetch_data` asset from DAG 13. Airflow triggers this DAG automatically whenever the upstream `fetch_data` asset is materialized.

**Concepts introduced:**
- Asset dependency: `schedule=fetch_data` — triggers on upstream asset materialization
- Data lineage tracking through the Airflow asset catalog
- Composing a data pipeline through asset dependencies without explicit DAG-to-DAG coupling

**What it does:**

Writes `"Data processed successfully"` to `/opt/airflow/logs/data/data_processed.txt` after the upstream asset is available.

---

### DAG Orchestration

A three-DAG set demonstrating how a parent DAG can trigger and coordinate multiple child DAGs in sequence using `TriggerDagRunOperator`.

---

#### Child DAG 1

**File:** `dags/dag_orchestrate_1.py`
**DAG ID:** `first_orchestrator_dag`

A self-contained DAG with three sequential tasks. The third task writes output to `/opt/airflow/logs/data/third_task.log`.

**Task flow:**

```
first_task >> second_task >> third_task (writes to disk)
```

---

#### Child DAG 2

**File:** `dags/dag_orchestrate_2.py`
**DAG ID:** `second_orchestrator_dag`

Mirrors the structure of Child DAG 1. The third task writes output to `/opt/airflow/logs/data/third_task_2.log`.

**Task flow:**

```
first_task >> second_task >> third_task (writes to disk)
```

---

#### Parent DAG

**File:** `dags/dag_orchestrate_parent.py`
**DAG ID:** `parent_orchestrator_dag`
**Schedule:** `None` (manual trigger only)

The parent DAG uses `TriggerDagRunOperator` to trigger the two child DAGs in sequence. Each trigger task is configured with `wait_for_completion=True`, meaning the parent will not advance until the triggered child DAG finishes successfully.

**Concepts introduced:**
- `TriggerDagRunOperator` — triggers another DAG run from within a running DAG
- `wait_for_completion=True` — blocks the parent task until the child DAG completes
- `schedule=None` — the DAG is only run manually or via the API; no automatic scheduling
- Cross-DAG orchestration as a complement to asset-based dependency management

**Task flow:**

```
trigger_first_orchestrator_dag >> trigger_second_orchestrator_dag
```

---

## Key Concepts Covered

| Concept | DAGs |
|---|---|
| TaskFlow API (`@dag`, `@task`) | All DAGs |
| Task dependencies (`>>`) | All DAGs |
| Classic operators (`BashOperator`) | DAG 3 |
| XCom automatic passing | DAG 4 |
| XCom manual push/pull (`ti.xcom_push`, `ti.xcom_pull`) | DAGs 5, 6, 7 |
| Parallel execution (fan-out / fan-in) | DAGs 6, 7 |
| Conditional branching (`@task.branch`) | DAG 7 |
| Schedule presets (`@daily`, etc.) | DAG 8 |
| Cron scheduling (`CronTriggerTimetable`) | DAG 9 |
| Delta scheduling (`DeltaTriggerTimetable`) | DAG 10 |
| Incremental loading (`data_interval_start` / `data_interval_end`) | DAG 11 |
| Event-based scheduling (`EventsTimetable`) | DAG 12 |
| Asset-driven scheduling (`@asset`) | DAGs 13, 14 |
| Cross-DAG orchestration (`TriggerDagRunOperator`) | Orchestration set |

---

## Infrastructure Reference

The Docker Compose stack defines the following services:

| Service | Image | Role | Exposed Port |
|---|---|---|---|
| `postgres` | `postgres:16` | Airflow metadata database | 5433 (host) |
| `redis` | `redis:7.2-bookworm` | Celery message broker | Internal only |
| `airflow-apiserver` | `apache/airflow:3.3.2` | REST API and Web UI | 8080 |
| `airflow-scheduler` | `apache/airflow:3.3.2` | DAG scheduling and run management | Internal only |
| `airflow-dag-processor` | `apache/airflow:3.3.2` | DAG file parsing | Internal only |
| `airflow-worker` | `apache/airflow:3.3.2` | Celery task execution | Internal only |
| `airflow-triggerer` | `apache/airflow:3.3.2` | Deferred task triggering | Internal only |
| `airflow-init` | `apache/airflow:3.3.2` | One-time initialization job | — |
| `airflow-cli` | `apache/airflow:3.3.2` | CLI access (debug profile) | — |
| `flower` | `apache/airflow:3.3.2` | Celery monitoring UI (flower profile) | 5555 |

**Volume mounts:**

| Host Path | Container Path | Purpose |
|---|---|---|
| `./dags` | `/opt/airflow/dags` | DAG files |
| `./logs` | `/opt/airflow/logs` | Task and scheduler logs |
| `./config` | `/opt/airflow/config` | Airflow configuration |
| `./plugins` | `/opt/airflow/plugins` | Custom plugins |

---

## Configuration Reference

The active Airflow configuration is stored in `config/airflow.cfg` and is mounted into all containers at `/opt/airflow/config/airflow.cfg`. The path is set via the `AIRFLOW_CONFIG` environment variable in `docker-compose.yaml`.

Key configuration values set via environment variables:

| Variable | Value | Description |
|---|---|---|
| `AIRFLOW__CORE__EXECUTOR` | `CeleryExecutor` | Task execution backend |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | PostgreSQL URI | Metadata database connection |
| `AIRFLOW__CELERY__BROKER_URL` | Redis URI | Celery message broker |
| `AIRFLOW__CELERY__RESULT_BACKEND` | PostgreSQL URI | Celery result backend |
| `AIRFLOW__CORE__FERNET_KEY` | From `.env` | Encryption key for stored connection credentials |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `false` | Disables built-in example DAGs |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | `true` | New DAGs start paused unless overridden per DAG |

---

## Security Notes

- The `.env` file contains sensitive values (`FERNET_KEY`) and is excluded from version control via `.gitignore`. Always generate a fresh Fernet key for any environment other than local development.
- The default admin credentials (`techwhiz@airflow` / `airflow`) are intended for local development only. Rotate them before deploying to any shared or production environment.
- The PostgreSQL port is exposed on the host as `5433` rather than the standard `5432` to avoid conflicts with a locally installed PostgreSQL instance.
- The `docker-compose.yaml` configuration is explicitly marked as unsuitable for production use. For production deployments, refer to the [official Airflow production deployment guide](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/production-deployment.html).

---

## Author

**Derick Juma** — [derekjude254@gmail.com](mailto:derekjude254@gmail.com)

This documentation was developed with technical assistance from Claude.
