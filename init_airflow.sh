#!/bin/bash

# Create the AWS connection if it doesn't exist
echo "Creating Airflow connection..."

airflow connections get aws_default > /dev/null
if [[ "$?" == "1" ]]; then
  conn_command="airflow connections add aws_default --conn-type aws \
                                                    --conn-login $AWS_ACCESS_KEY_ID \
                                                    --conn-password $AWS_SECRET_ACCESS_KEY \
                                                    --conn-extra '{\"region_name\": \"${AWS_DEFAULT_REGION}\""
  
  # Add the AWS_SESSION_TOKEN if it exists
  if [[ -n "${AWS_SESSION_TOKEN}" ]]; then
    conn_command+=", \"aws_session_token\": \"${AWS_SESSION_TOKEN}\""
  fi

  conn_command+="}'"
  eval $conn_command
fi

# Start the Airflow webserver
echo "Starting Airflow webserver..."
airflow webserver
