import argparse
import ast
from pipeline_workflow.default_workflow import DefaultWorkflow
from pipeline_utils.package import SparkParams 
from pyspark.sql import SparkSession

"""
Spark submit example:
To build the artifact
zip -r job.zip . && cp job.zip ~/data/ && cp ../workflow_entry.py ~/data/

docker run -dit --name pyspark -v /home/eguo/data:/data -v /home/eguo/spark-repo:/spark-repo jupyter/pyspark-notebook
spark-submit --master local --py-files /data/job.zip /data/workflow_entry.py -p "{'input_path':'/data/banking.csv','name':'demo', 'file_type':'txt', 'output_path':'/data/pyspark', 'partition_column': 'job'}"

spark-submit --master local --py-files /data/job.zip /data/workflow_entry.py -p "{'input_path':'/data/banking.csv','name':'demo', 'file_type':'txt', 'output_path':'/data/pyspark', 'partition_column': 'job'}"
"""

# parse submitted arguments from command line
parser = argparse.ArgumentParser()
parser.add_argument("-p", "--params", required=True, help="Spark input parameters")
args = parser.parse_args()

print('args ' + str(args))

def parse_command_line(args):
    # Convert command line argument to a dict
    return ast.literal_eval(args)


# Initiallize the sparkSession 
#
def spark_init(parser_name):
    ss = SparkSession \
        .builder \
        .appName(parser_name) \
        .getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")
    return ss


params = parse_command_line(args.params)
params = SparkParams(params)

print('****************** Parameters passed to SparkParams: PARAMS=' + str(params))

spark = spark_init(params.args['name'])     # create sparksession

# checking if the script is being run via command 'python script'
if __name__ == "__main__":
    print("****************** Executing script via python")
    dataflow = DefaultWorkflow(params, spark)
    dataflow.run()
else:
    print("****************** Importing script")