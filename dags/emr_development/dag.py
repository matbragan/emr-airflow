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
from emr_development.constants import BUCKET_NAME, SCRIPTS


# -------- Aux Functions -------- #
def _local_to_s3(bucket_name, s3_file, local_file):
    s3 = S3Hook(aws_conn_id='aws_default')
    s3.load_file(bucket_name=bucket_name, key=s3_file, filename=local_file, replace=True)

def upload_files_to_s3(bucket_name, scripts):
    for script in scripts:
        _local_to_s3(bucket_name, script['s3_script'], script['local_script'])


def create_emr_step(bucket_name, s3_script):
    step = {
        'Name': f'run {s3_script}',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                f's3://{bucket_name}/{s3_script}'
            ]
        },
    }
    return step


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
    python_callable=upload_files_to_s3,
    op_kwargs={'bucket_name': BUCKET_NAME, 'scripts': SCRIPTS}
)

create_emr_cluster = EmrCreateJobFlowOperator(
    dag=dag,
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default'
)

wait_for_cluster_ready = EmrJobFlowSensor(
    dag=dag,
    task_id='wait_for_cluster_ready',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    target_states=['RUNNING', 'WAITING'],
    failed_states=['TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS'],
    mode="reschedule"
)

upload_scripts_to_s3 >> create_emr_cluster >> wait_for_cluster_ready

terminate_emr_cluster = EmrTerminateJobFlowOperator(
    dag=dag,
    task_id='terminate_emr_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default'
)

for script in SCRIPTS:
    s3_script = script['s3_script']
    
    if s3_script.endswith('.py'):
        spark_step = create_emr_step(BUCKET_NAME, s3_script)
        spark_file_name = s3_script.split('/')[-1]
        
        run_spark_script = EmrAddStepsOperator(
            dag=dag,
            task_id=f'step_{spark_file_name}',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
            steps=[spark_step],
            aws_conn_id='aws_default'
        )

        wait_for_step_completion = EmrStepSensor(
            dag=dag,
            task_id=f'wait_for_step_{spark_file_name}',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
            step_id=f"{{ task_instance.xcom_pull(task_ids='step_{spark_file_name}', key='return_value')[0] }}",
            aws_conn_id='aws_default',
            target_states=['COMPLETED'],
            failed_states=['CANCELLED', 'FAILED', 'INTERRUPTED'],
            mode='reschedule'
        )

        wait_for_cluster_ready >> run_spark_script >> wait_for_step_completion >> terminate_emr_cluster
