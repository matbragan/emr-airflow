# emr-airflow

en-us

### Developing a Flow with EMR and Airflow

Airflow routines to create, execute and terminate clusters on EMR

### Airflow

Use the [`Makefile`](https://github.com/matbragan/emr-airflow/blob/main/Makefile) to upload a Airflow local server, but before that, configure the host machine environment with [AWS credentials](https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-files.html), this way when the make command is executed the Airflow server will have the necessary attributes to run AWS products.   

make command to upload the local Airflow:
~~~sh
make airflow-up
~~~

### EMR

Change the [`constants.py`](https://github.com/matbragan/emr-airflow/blob/main/dags/emr_development/constants.py) with the correct application bucket.  
Change the scripts located on [dags/emr_development/scripts](https://github.com/matbragan/emr-airflow/tree/main/dags/emr_development/scripts) to have the required scripts to run the required project.  
If necessary, change the [`emr_config.py`](https://github.com/matbragan/emr-airflow/blob/main/dags/emr_development/emr_config.py) to have the required cluster settings to run the required project.  

If EMR is being run for the first time in this AWS account, it's necessary to create its default roles, which can be done through the code below:
~~~sh
aws emr create-default-roles
~~~

<hr>

pt-br

### Desenvolvendo um Fluxo com EMR e Airflow

Rotinas no Airflow para criar, executar e encerrar clusters no EMR

### Airflow

Use o [`Makefile`](https://github.com/matbragan/emr-airflow/blob/main/Makefile) para subir um servidor local do Airflow, mas antes disso, configure a máquina hospedeira com as [Credenciais da AWS](https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-files.html), dessa forma quando o comando make for executado o servidor do Airflow terá os atributos necessários para rodar produtos da AWS.   

Comando make para subir o Airflow local:
~~~sh
make airflow-up
~~~

### EMR

Mude o [`constants.py`](https://github.com/matbragan/emr-airflow/blob/main/dags/emr_development/constants.py) com o bucket correto de sua aplicação.  
Altere os scripts localizados em [dags/emr_development/scripts](https://github.com/matbragan/emr-airflow/tree/main/dags/emr_development/scripts) para ter os scripts necessários para rodar seu projeto.   
Se necessário, altere o [`emr_config.py`](https://github.com/matbragan/emr-airflow/blob/main/dags/emr_development/emr_config.py) para ter as configurações de cluster necessárias para rodar seu projeto.   

Se o EMR estiver sendo executado pela primeira vez nesta conta da AWS, é necessário criar suas funções padrões, que pode ser feito através do código abaixo:
~~~sh
aws emr create-default-roles
~~~
