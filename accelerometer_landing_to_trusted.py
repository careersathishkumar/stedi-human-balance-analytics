import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


accelerometer_df = spark.read.json(
    "s3://stedi-project-sathish/accelerometer/landing/"
)


customer_df = spark.read.json(
    "s3://stedi-project-sathish/customer/trusted/"
)


accelerometer_df.printSchema()
customer_df.printSchema()

result_df = accelerometer_df.join(
    customer_df,
    accelerometer_df["user"] == customer_df["email"],
    "inner"
).select(
    accelerometer_df["user"],
    accelerometer_df["timestamp"],
    accelerometer_df["x"],
    accelerometer_df["y"],
    accelerometer_df["z"]
)

result_df.write.mode("overwrite").json(
    "s3://stedi-project-sathish/accelerometer/trusted/"
)

job.commit()