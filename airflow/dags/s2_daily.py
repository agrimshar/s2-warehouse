"""Daily pipeline for tile 17TPJ. Every task owns one data interval."""

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

PY = "cd /opt/s2 && /opt/s2/.venv/bin/python -m"
DBT = "cd /opt/s2/dbt && /opt/s2/.venv/bin/dbt"
INTERVAL = "--start {{ data_interval_start | ds }} --end {{ data_interval_end | ds }}"

with DAG(
    dag_id="s2_daily",
    description="Sentinel-2 17TPJ: catalog, ingest, silver, dbt for one data interval",
    schedule=CronDataIntervalTimetable("0 12 * * *", timezone="UTC"),    
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["s2-warehouse"],
):
    catalog = BashOperator(task_id="catalog", bash_command=f"{PY} s2warehouse.catalog")
    gate = BashOperator(
        task_id="gate",
        bash_command=f"{PY} s2warehouse.gate {INTERVAL}",
        skip_on_exit_code=99,
    )
    ingest = BashOperator(task_id="ingest", bash_command=f"{PY} s2warehouse.ingest {INTERVAL}")
    silver_compute = BashOperator(
        task_id="silver_compute",
        bash_command=f"{PY} s2warehouse.silver {INTERVAL} --compute-only",
    )
    silver_merge = BashOperator(
        task_id="silver_merge",
        bash_command=f"{PY} s2warehouse.silver --from-staging",
    )
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"{DBT} deps && {DBT} run")
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"{DBT} test")

    catalog >> gate >> ingest >> silver_compute >> silver_merge >> dbt_run >> dbt_test