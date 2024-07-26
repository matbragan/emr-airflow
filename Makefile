airflow-up:
	echo "AIRFLOW_UID=$$(id -u)" > .env
	echo "AIRFLOW_GID=0" >> .env
	echo "AWS_ACCESS_KEY_ID=$$(aws configure get aws_access_key_id)" >> .env 
	echo "AWS_SECRET_ACCESS_KEY=$$(aws configure get aws_secret_access_key)" >> .env
	aws_session_token=$$(aws configure get aws_session_token); \
	if [ -n "$$aws_session_token" ]; then \
		echo "AWS_SESSION_TOKEN=$$aws_session_token" >> .env; \
	fi
	echo "AWS_DEFAULT_REGION=us-east-1" >> .env
	docker-compose up -d --build

airflow-down:
	docker-compose down -v
