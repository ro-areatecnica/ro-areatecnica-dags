import pendulum
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.operators.email import EmailOperator

TZ = pendulum.timezone("America/Sao_Paulo")

# Constantes
DAG_ID = "dag_validacao_viagem_smtr"
PROJECT_ID = "ro-areatecnica"
REPOSITORY_ID = "validacao_viagem_smtr"
REGION = "us-central1"
GIT_COMMITISH = "main"
EXECUTION_TAG = "validacao_smtr"

default_args = {
    "owner": "ro-areatecnica",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 19, tzinfo=TZ),
    "retries": 0,
    "retry_delay": 0,
}

with models.DAG(
    dag_id=DAG_ID,
    schedule_interval=None,
    catchup=False,
    tags=['validacao_smtr'],
) as dag:

    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={"git_commitish": GIT_COMMITISH},
    )

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create_workflow_invocation",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
            "invocation_config": {
                "included_tags": [EXECUTION_TAG],
            },
        },
    )

    email_on_failure = EmailOperator(
        task_id="send_email_on_failure",
        to=["raphael.miranda@rioonibus.com", "alex.perfeito@rioonibus.com ", "miguel.dias@rioonibus.com"],
        subject="Falha no Dataform [validacao_viagem_smtr]",
        html_content="<h3>O processamento no Dataform falhou. Verifique os logs no Airflow e no Dataform.</h3>",
        trigger_rule="one_failed",
    )

    create_compilation_result >> create_workflow_invocation >> email_on_failure
