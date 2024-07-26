from emr_development.constants import BUCKET_NAME

JOB_FLOW_OVERRIDES = {
    "Name": "emr_development",
    "LogUri": f"s3://{BUCKET_NAME}/emr_logs/",
    "ReleaseLabel": "emr-7.1.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "AutoTerminationPolicy": {
        "IdleTimeout": 300
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}