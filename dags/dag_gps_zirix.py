import pendulum
import logging
from datetime import datetime, timedelta
from airflow.utils.email import send_email
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
import json
import requests
import google.oauth2.id_token
import google.auth.transport.requests

logger = logging.getLogger(__name__)

TZ = pendulum.timezone("America/Sao_Paulo")


def failure_email(context):
    task_instance = context['task_instance']
    task_status = 'Falhou'
    subject = f'Airflow Task {task_instance.task_id} - {task_status}'
    body = f"""
                <p><strong>A tarefa:</strong> {task_instance.task_id} foi concluída com status: <strong>{task_status}</strong>.</p>
                <p><strong>Data de execução da tarefa:</strong> {context['execution_date']}</p>
                <p><strong>URL do log:</strong> <a href="{task_instance.log_url}" target="_blank">{task_instance.log_url}</a></p>
                <p><strong>OBS:</strong> Por favor, verifique o log para mais detalhes sobre o erro ocorrido.</p>
            """
    to_email = ['raphael.miranda@rioonibus.com', 'miguel.dias@rioonibus.com', 'alex.perfeito@rioonibus.com']
    send_email(to=to_email, subject=subject, html_content=body)


default_args = {
    "owner": "ro-areatecnica",
    "depends_on_past": False,
    "start_date": datetime(2024, 11, 12, tzinfo=TZ),
    "email_on_failure": False,
    "email_on_retry": False,
    "execution_timeout":timedelta(seconds=300),
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="dag_zirix_data",
    default_args=default_args,
    description="Dag que aciona a Cloud Functions zirix_data e notifica em caso de falha por email.",
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["zirix"],
) as dag:

    def run_cf():
        request = google.auth.transport.requests.Request()
        audience = 'https://us-central1-ro-areatecnica.cloudfunctions.net/cf_gps_zirix_data'
        token = google.oauth2.id_token.fetch_id_token(request, audience)
        response = requests.post(audience,
                                 headers={'Authorization': f"Bearer {token}", "Content-Type": "application/json"},
                                 data=json.dumps({}))

        if response.status_code != 200:
            raise AirflowException(response.reason)

    python_task = PythonOperator(
        task_id='trigger_zirix_cf',
        python_callable=run_cf,
        on_failure_callback=failure_email,
    )

    bash_task = BashOperator(
        task_id='log_execution_time',
        bash_command='echo "Execução realizada em $(date)"',
        on_failure_callback=lambda context: failure_email(context),
    )

    python_task >> bash_task
