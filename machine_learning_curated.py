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
    "s3://stedi-project-sathish/step_trainer/trusted/"
)


accelerometer_df = spark.read.json(
    "s3://stedi-project-sathish/accelerometer/trusted/"
)


result_df = step_trainer_df.join(
    accelerometer_df,
    step_trainer_df["sensorReadingTime"] == accelerometer_df["timestamp"],
    "inner"
).select(
    step_trainer_df["sensorReadingTime"],
    step_trainer_df["serialNumber"],
    step_trainer_df["distanceFromObject"],
    accelerometer_df["user"],
    accelerometer_df["timestamp"],
    accelerometer_df["x"],
    accelerometer_df["y"],
    accelerometer_df["z"]
)

result_df.write.mode("overwrite").json(
    "s3://stedi-project-sathish/machine_learning/curated/"
)

job.commit()