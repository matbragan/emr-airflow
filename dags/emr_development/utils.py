import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def get_scripts_dir(script_type, directory='scripts'):
    absdir = os.path.dirname(os.path.abspath(__file__))
    files_path = os.path.join(absdir, directory, script_type)
    return [os.path.relpath(os.path.join(files_path, f)) for f in os.listdir(files_path) if os.path.isfile(os.path.join(files_path, f))]


def get_s3_script_dir(script_dir):
    return script_dir.replace('dags/', '')


def _local_to_s3(bucket_name, s3_file, local_file):
    s3 = S3Hook(aws_conn_id='aws_default')
    s3.load_file(bucket_name=bucket_name, key=s3_file, filename=local_file, replace=True)


def _upload_scripts_to_s3(bucket_name):
    script_types = ['bootstrap', 'steps']
    for script_type in script_types:
        scripts_dir = get_scripts_dir(script_type)
        for script_dir in scripts_dir:
            s3_script_dir = get_s3_script_dir(script_dir)
            _local_to_s3(bucket_name, s3_script_dir, script_dir)


def create_emr_step(bucket_name, script_dir):
    s3_script_dir = get_s3_script_dir(script_dir)
    step = {
        'Name': f'run {s3_script_dir}',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                f's3://{bucket_name}/{s3_script_dir}'
            ]
        },
    }
    return step
