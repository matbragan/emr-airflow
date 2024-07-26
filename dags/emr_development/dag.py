from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator

from emr_development.emr_config import JOB_FLOW_OVERRIDES
from emr_development.constants import BUCKET_NAME, S3_SCRIPT, LOCAL_SCRIPT


# ---------- Variables ---------- #
spark_step = {
    'Name': 'run spark job',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            '/usr/bin/spark-submit',
            '--deploy-mode',
            'cluster',
            '--master',
            'yarn',
            f's3://{BUCKET_NAME}/{S3_SCRIPT}'
        ]
    },
}


# -------- Aux Functions -------- #
def _local_to_s3(bucket_name, s3_file, local_file):
    s3 = S3Hook(aws_conn_id='aws_conn')
    s3.load_file(bucket_name=bucket_name, key=s3_file, filename=local_file, replace=True)


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

upload_script_to_s3 = PythonOperator(
    dag=dag,
    task_id='upload_script_to_s3',
    python_callable=_local_to_s3,
    op_kwargs={'bucket_name': BUCKET_NAME, 's3_file': S3_SCRIPT, 'local_file': LOCAL_SCRIPT}
)

create_emr_cluster = EmrCreateJobFlowOperator(
    dag=dag,
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_conn'
)

wait_for_cluster_ready = EmrJobFlowSensor(
    dag=dag,
    task_id='wait_for_cluster_ready',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_conn',
    target_states=['RUNNING', 'WAITING'],
    failed_states=['TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS'],
    mode="reschedule"
)

run_spark_script = EmrAddStepsOperator(
    dag=dag,
    task_id='run_spark_script',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    steps=[spark_step],
    aws_conn_id='aws_conn'
)

wait_for_step_completion = EmrStepSensor(
    dag=dag,
    task_id='wait_for_step_completion',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='run_spark_script', key='return_value')[0] }}",
    aws_conn_id='aws_conn',
    target_states=['COMPLETED'],
    failed_states=['CANCELLED', 'FAILED', 'INTERRUPTED'],
    mode='reschedule'
)

terminate_emr_cluster = EmrTerminateJobFlowOperator(
    dag=dag,
    task_id='terminate_emr_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_conn'
)

upload_script_to_s3 >> create_emr_cluster >> wait_for_cluster_ready >> run_spark_script >> wait_for_step_completion >> terminate_emr_cluster
