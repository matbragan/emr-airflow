from emr_development.constants import BUCKET_NAME, BOOTSTRAP_DIR
from emr_development.utils import get_scripts_dir, get_s3_script_dir

BOOTSTRAP_SCRIPT_DIR = get_s3_script_dir(get_scripts_dir(BOOTSTRAP_DIR)[0])

JOB_FLOW_OVERRIDES = {
    'Name': 'emr_development',
    'LogUri': f's3://{BUCKET_NAME}/emr_logs/',
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
    'BootstrapActions': [
        {
            'Name': 'Install Libs',
            'ScriptBootstrapAction': {
                'Path': f's3://{BUCKET_NAME}/{BOOTSTRAP_SCRIPT_DIR}'
            }
        }
    ],
    'AutoTerminationPolicy': {
        'IdleTimeout': 300
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}