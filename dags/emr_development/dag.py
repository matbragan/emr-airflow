from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.operators.python import PythonOperator

from emr_development.constants import BUCKET_NAME, STEPS_DIR
from emr_development.utils import (
    _upload_scripts_to_s3,
    get_scripts_dir,
    create_emr_step
)
from emr_development.emr_config import JOB_FLOW_OVERRIDES


STEPS_SCRIPTS_DIR = get_scripts_dir(STEPS_DIR)


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
    op_kwargs={'bucket_name': BUCKET_NAME}
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

for step_script_dir in STEPS_SCRIPTS_DIR:
    spark_step = create_emr_step(BUCKET_NAME, step_script_dir)
    step_file_name = step_script_dir.split('/')[-1].split('.')[0]
    
    run_step_script = EmrAddStepsOperator(
        dag=dag,
        task_id=f'run_step_{step_file_name}',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=[spark_step],
        aws_conn_id='aws_default'
    )

    wait_for_step_completion = EmrStepSensor(
        dag=dag,
        task_id=f'wait_for_step_{step_file_name}',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{" + f" task_instance.xcom_pull(task_ids='run_step_{step_file_name}', key='return_value')[0] " + "}}",
        aws_conn_id='aws_default',
        target_states=['COMPLETED'],
        failed_states=['CANCELLED', 'FAILED', 'INTERRUPTED'],
        mode='reschedule'
    )

    wait_for_cluster_ready >> run_step_script >> wait_for_step_completion >> terminate_emr_cluster
