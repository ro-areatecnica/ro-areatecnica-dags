import pendulum
import logging
from datetime import datetime, timedelta
from airflow.utils.email import send_email
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from google.cloud import bigquery
import json
import requests
import google.oauth2.id_token
import google.auth.transport.requests


logger = logging.getLogger(__name__)

TZ = pendulum.timezone("America/Sao_Paulo")


def check_failed_endpoints(context=None):
    client = bigquery.Client()
    query = """
        SELECT api, endpoint, last_extraction
        FROM `ro-areatecnica.zirix_raw.control_table_viagens_consolidadas`
        WHERE status = 'failed'
        AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_extraction, MINUTE) <= 1
        ORDER BY last_extraction DESC
        LIMIT 1;
    """
    query_job = client.query(query)
    results = query_job.result()

    failed_endpoints = [
        {
            "api": row.api,
            "endpoint": row.endpoint,
            "last_extraction": row.last_extraction,
        }
        for row in results
    ]

    if failed_endpoints:
        logger.warning(f"Falhas encontradas: {failed_endpoints}")

        if context is None:
            context = {}

        context['task_instance'] = {
            'task_id': 'verify_failed_endpoints'
        }
        context['execution_date'] = datetime.now(TZ)

        subject = "FALHA NA TABELA DE CONTROLE VIAGENS CONSOLIDADAS DA ZIRIX"
        body = f"""
        <p>Foram encontradas falhas na tabela de controle:</p>
        <ul>
        {''.join([f"<li>{ep['api']} - {ep['endpoint']} (última execução: {ep['last_extraction']})</li>" for ep in failed_endpoints])}
        </ul>
        """
        send_email(
            to=["raphael.miranda@rioonibus.com", "miguel.dias@rioonibus.com", "alex.perfeito@rioonibus.com"],
            subject=subject,
            html_content=body,
        )
    else:
        logger.info("Nenhum endpoint com falha encontrado na tabela de controle.")


def run_cf():
    request = google.auth.transport.requests.Request()
    audience = 'https://us-central1-ro-areatecnica.cloudfunctions.net/cf-gps-zirix-viagens-consolidadas'
    token = google.oauth2.id_token.fetch_id_token(request, audience)
    response = requests.post(
        audience,
        headers={'Authorization': f"Bearer {token}", "Content-Type": "application/json"},
        data=json.dumps({}),
    )

    if response.status_code != 200:
        raise AirflowException(response.reason)


default_args = {
    "owner": "ro-areatecnica",
    "depends_on_past": False,
    "start_date": datetime(2024, 11, 12, tzinfo=TZ),
    "email_on_failure": False,
    "email_on_retry": False,
    "execution_timeout": timedelta(seconds=300),
    "retries": 1,
    "retry_delay": 0,
}

with DAG(
        dag_id="dag_zirix_data_consolidadas",
        default_args=default_args,
        description="Dag que aciona a Cloud Functions cf-gps-zirix-viagens-consolidadas e notifica em caso de falha por email.",
        schedule_interval=timedelta(hours=1),
        catchup=False,
        tags=["zirix_consolidadas"],
) as dag:
    verify_failed_task = PythonOperator(
        task_id="verify_failed_endpoints",
        python_callable=check_failed_endpoints,
    )

    python_task = PythonOperator(
        task_id="trigger_zirix_cf",
        python_callable=run_cf
    )

    bash_task = BashOperator(
        task_id="log_execution_time",
        bash_command="echo 'Execução realizada em $(date)'"
    )

    verify_failed_task >> python_task >> bash_task