import airflow
import airflow.utils
import boto3
from datetime import timedelta
import time as time 
from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date' : airflow.utils.dates.days_ago(0)
}

CLUSTER_ID = 'j-1ALLPL5ZCJII7'       # AWS EMR Cluster ID 

SPARK_STEPS = [
    {
        'Name': 'pyspark_data_processing_engine',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit', 
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                '--num-executors', '2',
                '--executor-memory', '3g',
                '--driver-memory', '1g',
                '--executor-cores', '2',
                '--py-files', 's3://wcd-midterm-proj/pyspark-engine-2.1/pyspark-engine-v2.1.zip',
                's3://wcd-midterm-proj/pyspark-engine-2.1/workflow_entry.py',
                '-p', "{{ task_instance.xcom_pull(task_ids='parse_request', key='runtime_params') }}"
                ],
        },
    }
]

CRAWLER_NAME = 'de-midterm-proj-crawler'    # name of Glue crawler
MAX_CRAWLER_WAIT_TIME = 600   # max time in sec to wait for crawler completion (for in case crawler hangs/gets stuck)
GLUE_DB_NAME = 'de_midterm_proj_db'    # target database name in Glue
GLUE_TABLE_NAME = 'pysparkdataoutput'   # target table namein Glue
ATHENA_QUERY_OUTPUT_PATH = 's3://wcd-midterm-proj/queryresults'    # target path for boto3-run Athena query outputs

dag = DAG(
    dag_id='pyspark_emr_job_flow_manual_steps_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    tags=['emr']
)

# retrieve the parameters required by the pyspark engine 
def retrieve_runtime_parameters(**kwargs):
    runtime_params = kwargs['dag_run'].conf['runtime_params']
    kwargs['ti'].xcom_push(key = 'runtime_params', value=runtime_params)


# function to invoke the AWS Glue crawler
# 
def invoke_crawler(**kwargs):
    client = boto3.client('glue')

    # check if glue crawler exists
    try:
        client.get_crawler(Name=CRAWLER_NAME)
    except glue.exceptions.EntityNotFoundException:
        print("Crawler not found")

    # run the glue crawler
    try:
        client.start_crawler(Name=CRAWLER_NAME)
    except:
        print("Error when trying to start crawler")

    # wait for crawler to complete by polling status every second
    for cnt in range(0, MAX_CRAWLER_WAIT_TIME ):
        time.sleep( 1 )
        if (client.get_crawler(Name=CRAWLER_NAME)['Crawler']['State']) == 'READY':
            print("Crawler finished and completed")
            break

# function to invoke msck repair table sql command in Athena in the case of additonal partitions or data 
#
def repair_table(**kwargs):

    try:
        client = boto3.client('athena')
    except:
        print("Error when trying to start crawler")
    
    # run query in Athena
    response = client.start_query_execution(
        QueryString = 'msck repair table ' + GLUE_TABLE_NAME,
        QueryExecutionContext = { 'Database' : GLUE_DB_NAME },
        ResultConfiguration = { 'OutputLocation': ATHENA_QUERY_OUTPUT_PATH }
    )


# DAG to parse parameters passed in (via Lambda) for spark-submit
parse_request = PythonOperator(
    task_id = 'parse_request',
    provide_context = True,
    python_callable = retrieve_runtime_parameters,
    dag=dag
)

# DAG to configure and run spark job on EMR
step_adder = EmrAddStepsOperator(
    task_id = 'add_steps',
    job_flow_id = CLUSTER_ID,
    aws_conn_id = 'aws_default',
    steps = SPARK_STEPS,
    dag=dag
)

# DAG to wait and sense the end of the spark EMR cluster processing 
step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id = CLUSTER_ID,
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
    aws_conn_id = 'aws_default',
    dag=dag
)

# DAG to start the glue crawler 
invoke_glue_crawler = PythonOperator(
    task_id = 'invoke_glue_crawler',
    provide_context = True,
    python_callable = invoke_crawler,
    dag=dag
)

# DAG to invoke repair table query command in Athena
repair_athena_table = PythonOperator(
    task_id = 'repair_athena_table',
    provide_context = True,
    python_callable = repair_table,
    dag=dag
)

parse_request >> step_adder >> step_checker >> invoke_glue_crawler >> repair_athena_table
