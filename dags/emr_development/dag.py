from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator
)
from airflow.operators.python import PythonOperator

from emr_development.constants import BUCKET_NAME, SCRIPT_TYPES, STEPS_DIR
from emr_development.utils import (
    get_scripts_dir,
    get_s3_script_dir,
    _upload_scripts_to_s3
)
from emr_development.emr_config import JOB_FLOW_OVERRIDES


# ------------- DAG ------------- #
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 15),
}

dag = DAG(
    dag_id='emr_development',
    default_args=default_args,
    description='A DAG to develop on EMR',
    schedule=None,
    catchup=False
)

upload_scripts_to_s3 = PythonOperator(
    dag=dag,
    task_id='upload_scripts_to_s3',
    python_callable=_upload_scripts_to_s3,
    op_kwargs={'bucket_name': BUCKET_NAME, 'script_types': SCRIPT_TYPES}
)

create_emr_cluster = EmrCreateJobFlowOperator(
    dag=dag,
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    deferrable=True,
    aws_conn_id='aws_default'
)

upload_scripts_to_s3 >> create_emr_cluster

terminate_emr_cluster = EmrTerminateJobFlowOperator(
    dag=dag,
    task_id='terminate_emr_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default'
)

STEPS_SCRIPTS_DIR = get_scripts_dir(STEPS_DIR)
for step_script_dir in STEPS_SCRIPTS_DIR:
    s3_script_dir = get_s3_script_dir(step_script_dir)
    step_file_name = step_script_dir.split('/')[-1].split('.')[0]
    
    step = {
        'Name': f'run {s3_script_dir}',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                f's3://{BUCKET_NAME}/{s3_script_dir}',
                '--bucket_name', BUCKET_NAME,
            ]
        },
    }
    
    run_step_script = EmrAddStepsOperator(
        dag=dag,
        task_id=f'run_step_{step_file_name}',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        deferrable=True,
        steps=[step],
        aws_conn_id='aws_default'
    )

    create_emr_cluster >> run_step_script >> terminate_emr_cluster
