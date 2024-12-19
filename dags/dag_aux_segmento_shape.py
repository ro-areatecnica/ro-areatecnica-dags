import pendulum
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.email import EmailOperator


TZ = pendulum.timezone("America/Sao_Paulo")

PROJECT_ID = "ro-areatecnica"
REGION = "us-central1"
CLUSTER_NAME = "cluster-segmento-shape"
BQ_DATASET = "planejamento_staging"
BQ_TABLE_RESULT = "aux_segmento_shape_raw"
TABLE_INPUT = "aux_shapes_geom_filtrada"

GCS_BUCKET = "segmentacao-shapes-jobs"
SPARK_JOB_PATH = f"gs://{GCS_BUCKET}/scripts/process_segmentos.py"

default_args = {
    "owner": "ro-areatecnica",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 18, tzinfo=TZ),
    "retries": 0,
    "retry_delay": 0,
}

with DAG(
    dag_id="dag_dataproc_to_bigquery_shapes",
    default_args=default_args,
    description="Orquestração do processamento de dados no Dataproc e carga no BigQuery",
    schedule_interval=None,
    catchup=False,
    tags=["segmento_shape"],
) as dag:

    submit_dataproc_job = DataprocSubmitJobOperator(
        task_id="submit_dataproc_job",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": SPARK_JOB_PATH,
                "args": [
                    "--input_table", TABLE_INPUT,
                    "--output_table", f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_RESULT}",
                ],
                "properties": {
                    "spark.jars": "gs://spark-lib/bigquery/spark-3.4-bigquery-0.34.0.jar",
                    "spark.sql.catalog.bq": "com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider",
                    "spark.datasource.bigquery.viewsEnabled": "true",
                },
            },
        },
    )

    email_on_failure = EmailOperator(
        task_id="send_email_on_failure",
        to=["raphael.miranda@rioonibus.com", "alex.perfeito@rioonibus.com ", "miguel.dias@rioonibus.com"],
        subject="Falha no Job Dataproc",
        html_content="<h3>O processamento no Dataproc falhou. Verifique os logs no Airflow e no Dataproc.</h3>",
        trigger_rule="one_failed",
    )

    submit_dataproc_job >> email_on_failure
