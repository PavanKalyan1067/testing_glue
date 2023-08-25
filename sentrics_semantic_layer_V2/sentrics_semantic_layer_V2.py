import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://test.processing.for.glue.prod/alarm/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

alarm_df = S3bucket_node1.toDF()
events_df = spark.read.parquet("s3://test.processing.for.glue.prod/events/")
final_df1 = alarm_df.join(events_df,alarm_df.alarmid ==  events_df.alarmid,"left")
final_df1.show(10)

final_dynamic_frame = DynamicFrame.fromDF(final_df1, glueContext, "final_dynamic_frame")

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=S3bucket_node1,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://test.processing.for.glue.prod.output/",
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="S3bucket_node3",
)

job.commit()
