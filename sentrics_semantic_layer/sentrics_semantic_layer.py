import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import *
spark=SparkSession.builder.appName('sentrics_semantic_layer').getOrCreate()
spark
alarms_df = spark.read.parquet("s3://test.processing.for.glue.prod/alarm/")
# alarms_df.show(10)
alarms_df.printSchema()
events_df = spark.read.parquet("s3://test.processing.for.glue.prod/events/")
# events_df.show(10)
events_df.printSchema()
loc_df = spark.read.parquet("s3://test.processing.for.glue.prod/locations/")
# loc_df.show(10)
loc_df.printSchema()
trans_loc_df = spark.read.parquet("s3://test.processing.for.glue.prod/transformed_location_details/")
# trans_loc_df.show(10)
trans_loc_df.printSchema()
sfdc_df = spark.read.parquet("s3://test.processing.for.glue.prod/sfdc/")
# sfdc_df.show(10)
sfdc_df.printSchema()
# Perform an inner join
join_condition = loc_df["locationid"] == trans_loc_df["_id"]
location_df = loc_df.join(trans_loc_df, join_condition, "inner")
location = location_df.select(["locationkey", "location","facilitykey","locationid","hierarchicalindex","parentkey",loc_df["createdat"],loc_df["updatedat"],"locationType"])
location.show(10)
location.printSchema()
resident = alarms_df.select([ alarms_df["originalsubjectid"].alias("residentid"), alarms_df["originalsubjectname"].alias("residentname")])
resident = resident.dropDuplicates()
resident.show(10)
resident.printSchema()
events = events_df.select([events_df["id"].alias("eventid"),"alarmid","closing","createdat","facilitykey","closerid","accepterid"])
# events.show(10)
events_df.printSchema()
join_condition_2 = events_df["facilitykey"] == sfdc_df["ensureid"]
community_df =  events_df.select(["communityid", events_df["community"].alias("communityname"), "facilitykey"]).join(sfdc_df.select([sfdc_df["parentaccountid"].alias("corporateid"),sfdc_df["parentaccount"].alias("corporatename"), "ensureid"]),join_condition_2, "inner")
# community_df.show(10)

community = community_df.select("communityid", "communityname", "corporateid", "corporatename")
community = community.dropDuplicates()
community.show(10, truncate=False)
community.printSchema()
alarms = alarms_df.select(["alarmid", alarms_df["originalsubjectid"].alias("residentid"),"locationkey","dispositiontype","dispositionnote","dispositionauthorkey","openedat","createdat","updatedat","closedat",alarms_df["status"].alias("alarms_status"), alarms_df["type"].alias("alarms_type"),"topic","domain","class"])

# Convert the string date column to a timestamp column
alarms = alarms.withColumn("openedat_ts", to_timestamp(col("openedat"), "yyyy-MM-dd HH:mm:ss")).withColumn("createdat_ts", to_timestamp(col("createdat"), "yyyy-MM-dd HH:mm:ss")).withColumn("updatedat_ts", to_timestamp(col("updatedat"), "yyyy-MM-dd HH:mm:ss"))

# List of column names to drop
columns_to_drop = ["openedat", "createdat","updatedat"]

# Drop the specified columns
alarms = alarms.drop(*columns_to_drop)

# Rename columns
alarms = alarms.withColumnRenamed("openedat_ts", "openedat").withColumnRenamed("createdat_ts", "createdat").withColumnRenamed("updatedat_ts", "updatedat")

alarms = alarms.join(events_df.select("alarmid","communityid"), "alarmid")

alarms.show(10)
alarms = alarms.dropDuplicates()
alarms = alarms.select("alarmid","communityid","residentid","locationkey","dispositiontype","dispositionnote","dispositionauthorkey","openedat","createdat","updatedat","closedat","alarms_status","alarms_type","topic","domain","class")
alarms.printSchema()
earliest_resp_df.select("alarmid","openedat","createdat","earliest_response").s
# spark.stop()
job.commit()