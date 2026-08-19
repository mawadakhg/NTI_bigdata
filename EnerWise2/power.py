from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime
import os


LANDING_ZONE = "/home/jovyan/work/ntiProject/data_raw_power_pings/"
SCRIPTS_PATH = "/home/jovyan/work/ntiProject/"

HDFS_RAW_ZONE = "/user/root/datalake/raw1/power_consumption/"

# لازم يبقي زي start_date عشان ترتيب ال batches
PIPELINE_START = datetime(2026, 8, 18)

TOTAL_BATCHES = 20  # 1000 rows / 50 rows-per-batch

# Airflow يحسب الـ batch_name من تاريخ الـ run


def resolve_batch(**context):
    try:
        run_date   = context["logical_date"].replace(tzinfo=None)
        run_number = (run_date - PIPELINE_START).days + 1

        print(f"  run_date       : {run_date}")
        print(f"  run_number     : {run_number}")
        print(f"  PIPELINE_START : {PIPELINE_START}")

        if run_number > TOTAL_BATCHES or run_number < 1:
            print(f"  run_number={run_number} out of range.")
            return False

        batch_name     = f"power_batch_{run_number:04d}.json"
        batch_file     = os.path.join(LANDING_ZONE, batch_name)
        hdfs_raw_file  = os.path.join(HDFS_RAW_ZONE, batch_name)

        context["ti"].xcom_push(key="batch_file", value=batch_file)
        context["ti"].xcom_push(key="hdfs_raw_file", value=hdfs_raw_file)
        context["ti"].xcom_push(key="run_number", value=run_number)

        print(f"  batch_file     : {batch_file}")
        print(f"  hdfs_raw_file  : {hdfs_raw_file}")
        return True

    except Exception as e:
        print(f"ERROR: {e}")
        return False


# default_args = {
#     "owner":            "airflow",
#     "retries":          2,
#     "retry_delay":      timedelta(minutes=5),
#     "email_on_failure": False,
#     "email_on_retry":   False,
# }

# DAG

with DAG(
    dag_id            = "powerc_etl_pipeline",
    # default_args      = default_args,
    description       = "Daily power-consumption ETL — one new batch per run via batch_name",
    start_date        = datetime(2026, 8, 18),
    schedule_interval = "@daily",
    catchup           = False,
) as dag:

    # task 1 --> Airflow بيجيب الـ batch_name
    resolve_batch_task = ShortCircuitOperator(
        task_id         = "resolve_batch",
        python_callable = resolve_batch,
        provide_context = True,
    )

    # task 2 --> INGESTION: tell NiFi to move today's batch from the
    # local landing zone into the HDFS raw zone. Airflow stays in control
    # of *when* — it POSTs the exact file path, NiFi's flow (ListenHTTP ->
    # EvaluateJsonPath -> FetchFile -> UpdateAttribute -> PutHDFS) does the
    # actual move. See NIFI_SETUP.md for building this flow.
    ingest_to_hdfs_raw = BashOperator(
        task_id      = "ingest_to_hdfs_raw",
        bash_command = (
            "curl -sf -X POST http://nifi:9998/ingest "
            "-H 'Content-Type: application/json' "
            "-d '{\"batch_file\": \"{{ ti.xcom_pull(task_ids=\"resolve_batch\", key=\"batch_file\") }}\"}'"
        ),
    )

    # task 3 --> extraction (reads from HDFS raw zone now, not local disk)
    extract = BashOperator(
        task_id      = "extract_to_bronze",
        bash_command = (
            "docker exec spark-jupyter spark-submit "
            f"{SCRIPTS_PATH}extraction.py "
            "\"{{ ti.xcom_pull(task_ids='resolve_batch', key='hdfs_raw_file') }}\""
        ),
    )

    # task 4 --> Transform
    transform = BashOperator(
        task_id      = "transform_to_gold",
        bash_command = (
            f"docker exec spark-jupyter spark-submit {SCRIPTS_PATH}transformation.py"
        ),
    )

    # task 5 --> Archive Bronze Parquet (منع إعادة معالجة نفس الباتش تاني)
    # archive_bronze = BashOperator(
    #     task_id      = "archive_bronze_hdfs",
    #     bash_command = (
    #         "docker exec hadoop-namenode bash -c \""
    #         "hdfs dfs -mkdir -p /user/root/datalake/bronzeLayer/archived/ && "
    #         "hdfs dfs -mv /user/root/datalake/bronzeLayer/power_consumption/*.parquet "
    #         "/user/root/datalake/bronzeLayer/archived/ || true\""
    #     ),
    # )
    archive_bronze = BashOperator(
    task_id      = "archive_bronze_hdfs",
    bash_command = (
        "docker exec hadoop-namenode bash -c \""
        "set -e; "
        "hdfs dfs -mkdir -p /user/root/datalake/bronzeLayer/archived/; "
        "FILES=$(hdfs dfs -ls /user/root/datalake/bronzeLayer/power_consumption/*.parquet 2>/dev/null | wc -l); "
        "if [ \\\"$FILES\\\" -eq 0 ]; then "
        "  echo 'No parquet files to archive — nothing to do.'; "
        "  exit 0; "
        "fi; "
        "hdfs dfs -mv /user/root/datalake/bronzeLayer/power_consumption/*.parquet "
        "/user/root/datalake/bronzeLayer/archived/; "
        "REMAINING=$(hdfs dfs -ls /user/root/datalake/bronzeLayer/power_consumption/*.parquet 2>/dev/null | wc -l); "
        "if [ \\\"$REMAINING\\\" -ne 0 ]; then "
        "  echo 'ERROR: files still present in Bronze after mv — archive incomplete.'; "
        "  exit 1; "
        "fi; "
        "echo 'Archive verified: Bronze is clean.'\""
    ),
)

    # task 6 --> load to snowflake
    load = BashOperator(
        task_id      = "load_to_snowflake",
        bash_command = (
            "docker exec spark-jupyter spark-submit "
            "--packages net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,"
            "net.snowflake:snowflake-jdbc:3.15.0 "
            f"{SCRIPTS_PATH}loading.py"
        ),
        # execution_timeout = timedelta(minutes=30),
    )

    resolve_batch_task >> ingest_to_hdfs_raw >> extract >> transform >> archive_bronze >> load
    