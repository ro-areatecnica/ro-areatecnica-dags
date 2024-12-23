import pendulum
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.operators.email import EmailOperator

TZ = pendulum.timezone("America/Sao_Paulo")

DAG_ID = "dag_dataform_validacao_planejamento_monitoramento_viagem"
PROJECT_ID = "ro-areatecnica"
REPOSITORY_ID = "validacao_viagem_smtr"
REGION = "us-central1"
GIT_COMMITISH = "main"


default_args = {
    "owner": "ro-areatecnica",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 18, tzinfo=TZ),
    "retries": 0,
    "retry_delay": 0,
}


with models.DAG(
    dag_id=DAG_ID,
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=['validacao_smtr', 'geolocalizacao'],
) as dag:

    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={"git_commitish": GIT_COMMITISH},
    )

    create_workflow_invocation_geolocalizacao = DataformCreateWorkflowInvocationOperator(
        task_id="create_workflow_invocation_geolocalizacao",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
            "invocation_config": {
                "included_tags": ["geolocalizacao"],
            },
        },
    )

    create_workflow_invocation_monitoramento_viagem = DataformCreateWorkflowInvocationOperator(
        task_id="create_workflow_invocation_monitoramento_viagem",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
            "invocation_config": {
                "included_tags": ["monitoramento_viagem"],
            },
        },
    )

    email_on_failure = EmailOperator(
        task_id="send_email_on_failure",
        to=[
            "raphael.miranda@rioonibus.com",
            "alex.perfeito@rioonibus.com",
            "miguel.dias@rioonibus.com",
        ],
        subject="Falha no Dataform [validacao_viagem_smtr]",
        html_content="<h3>O processamento no Dataform falhou. Verifique os logs no Airflow e no Dataform.</h3>",
        trigger_rule="one_failed",
    )

    create_compilation_result >> create_workflow_invocation_geolocalizacao >> create_workflow_invocation_monitoramento_viagem >> email_on_failure
