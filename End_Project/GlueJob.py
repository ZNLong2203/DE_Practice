import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from joblib import variable as V

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from S3
df = spark.read.csv("s3://ai4e-ap-southeast-1-dev-s3-data-landing/golden_zone/nnlong/phimmoi.csv", header=True, inferSchema=True)

top_views = df.orderBy(df["Views"].desc())

top_views.write.mode("overwrite").csv("s3://ai4e-ap-southeast-1-dev-s3-data-landing/golden_zone/nnlong/top_views")