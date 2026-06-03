import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from S3
accelerometer_df = spark.read.json(
    "s3://stedi-project-sathish/accelerometer/landing/"
)
customer_df = spark.read.json(
    "s3://stedi-project-sathish/customer/trusted/"
)

# Join on email
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

# Convert to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(
    result_df,
    glueContext,
    "dynamic_frame"
)

# Write and update Glue Catalog
sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-project-sathish/accelerometer/trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE"
)
sink.setFormat("json")
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="accelerometer_trusted"
)
sink.writeFrame(dynamic_frame)

job.commit()
