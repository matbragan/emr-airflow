from emr_development.constants import BUCKET_NAME, PROJECT_NAME, BOOTSTRAP_DIR
from emr_development.utils import get_scripts_dir, get_s3_script_dir


bootstrap_actions = []
BOOTSTRAP_SCRIPTS_DIR = get_scripts_dir(BOOTSTRAP_DIR)
for bootstrap_script_dir in BOOTSTRAP_SCRIPTS_DIR:
    s3_script_dir = get_s3_script_dir(bootstrap_script_dir)
    bootstrap_file_name = bootstrap_script_dir.split('/')[-1].split('.')[0]

    bootstrap_actions.append({
        'Name': bootstrap_file_name,
        'ScriptBootstrapAction': {
            'Path': f's3://{BUCKET_NAME}/{s3_script_dir}'
        }
    })


JOB_FLOW_OVERRIDES = {
    'Name': 'emr_development',
    'LogUri': f's3://{BUCKET_NAME}/{PROJECT_NAME}/emr_logs/',
    'ReleaseLabel': 'emr-7.1.0',
    'Applications': [{'Name': 'Spark'}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Primary node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'BootstrapActions': bootstrap_actions,
    'AutoTerminationPolicy': {
        'IdleTimeout': 300
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}