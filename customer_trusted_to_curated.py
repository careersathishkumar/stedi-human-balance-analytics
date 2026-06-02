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


customer_df = spark.read.json(
    "s3://stedi-project-sathish/customer/trusted/"
)


accelerometer_df = spark.read.json(
    "s3://stedi-project-sathish/accelerometer/trusted/"
)


result_df = customer_df.join(
    accelerometer_df,
    customer_df["email"] == accelerometer_df["user"],
    "inner"
).select(
    customer_df["customername"],
    customer_df["email"],
    customer_df["phone"],
    customer_df["birthday"],
    customer_df["serialnumber"],
    customer_df["registrationdate"],
    customer_df["lastupdatedate"],
    customer_df["sharewithresearchasofdate"],
    customer_df["sharewithpublicasofdate"],
    customer_df["sharewithfriendsasofdate"]
).distinct()


result_df.write.mode("overwrite").json(
    "s3://stedi-project-sathish/customer/curated/"
)

job.commit()