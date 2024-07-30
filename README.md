# emr-airflow

### Developing a Flow with EMR and Airflow

Airflow routines to create, execute and terminate clusters on EMR

### Airflow

Use the [`Makefile`](https://github.com/matbragan/emr-airflow/blob/main/Makefile) to upload a Airflow local server, but before that, configure the host machine environment with [AWS credentials](https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-files.html), this way when the make command is executed the Airflow server will have the necessary attributes to run AWS products.   

Make command to upload the local Airflow:
~~~sh
make airflow-up
~~~

### EMR

Change the [`constants.py`](https://github.com/matbragan/emr-airflow/blob/main/dags/emr_development/constants.py) with the correct application bucket.  
Change the scripts located on [dags/emr_development/scripts](https://github.com/matbragan/emr-airflow/tree/main/dags/emr_development/scripts) to have the required scripts to run the required project.  
If necessary, change the [`emr_config.py`](https://github.com/matbragan/emr-airflow/blob/main/dags/emr_development/emr_config.py) to have the required cluster settings to run the required project.  
