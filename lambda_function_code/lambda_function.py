import json
import subprocess
import boto3

def lambda_handler(event, context):

    airflow_ec2_ip = '54.175.53.47'
    json_parameters_filename = 'pyspark_submit_parameters.json'
    main_dagname = 'pyspark_emr_job_flow_manual_steps_dag'
    
    # initial default status settings
    status_code = 200
    status_msg = 'OK'
    
    # from the event (i.e. upload of file to s3), retrieve the filename and path
    records = event['Records'][0]['s3']
    bucket_name = records['bucket']['name']
    file_name = records['object']['key']
    incoming_file = 's3://' + bucket_name + '/' + file_name

    # retrieve runtime parameters for spark-submit from json file
    # add in the incoming_file to the dictionary too
    s3 = boto3.client('s3')
    bucket = 'wcd-midterm-proj'
    key = 'pyspark-engine-2/' + json_parameters_filename
    
    try:
        data2 = s3.get_object(Bucket=bucket, Key=key)
        json_data = data2['Body'].read()
        pyspark_submit_params = json.loads(json_data)
        pyspark_submit_params['input_path'] = incoming_file
    except Exception as e:
        status_msg = e
        status_code = 400

    print(f'************ WCD_Logging. pyspark_submit_params = {pyspark_submit_params}')

    # call Airflow api endpoint to trigger DAG
    # Note: this is using airflow 1.1, therefore the call is to the experimental API
    # if upgrading to Airflow 2 in future, this will need to be modified to use the 'stable REST API'
    endpoint = 'http://' + airflow_ec2_ip + ':8080/api/experimental/dags/' + main_dagname + '/dag_runs'
    data = json.dumps( {'conf': { 'runtime_params': pyspark_submit_params }} )

    # TODO edwin says to enrich the step to make more robust!
    subprocess.run(['curl', '-X', 'POST', endpoint, '--insecure', '-d', data])
    
    return {
        'statusCode': status_code,
        'body': json.dumps(status_msg)
    }
