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


step_trainer_df = spark.read.json(
    "s3://stedi-project-sathish/step-trainer/landing/"
)
customers_df = spark.read.json(
    "s3://stedi-project-sathish/customer/curated/"
)


result_df = step_trainer_df.join(
    customers_df,
    step_trainer_df["serialNumber"] == customers_df["serialnumber"],
    "inner"
).select(
    step_trainer_df["sensorReadingTime"],
    step_trainer_df["serialNumber"],
    step_trainer_df["distanceFromObject"]
)


dynamic_frame = DynamicFrame.fromDF(
    result_df,
    glueContext,
    "dynamic_frame"
)


sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-project-sathish/step_trainer/trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE"
)
sink.setFormat("json")
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="step_trainer_trusted"
)
sink.writeFrame(dynamic_frame)

job.commit()
