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


result_df.write.mode("overwrite").json(
    "s3://stedi-project-sathish/step_trainer/trusted/"
)

job.commit()