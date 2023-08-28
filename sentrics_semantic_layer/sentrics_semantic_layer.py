import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark=SparkSession.builder.appName('sentrics_semantic_layer').getOrCreate()
spark
alarms_df = spark.read.parquet("s3://test.processing.for.glue.prod/alarm/")
# alarms_df.show(10)
# alarms_df.printSchema()
events_df = spark.read.parquet("s3://test.processing.for.glue.prod/events/")
# events_df.show(10)
# events_df.printSchema()
loc_df = spark.read.parquet("s3://test.processing.for.glue.prod/locations/")
# loc_df.show(10)
# loc_df.printSchema()
trans_loc_df = spark.read.parquet("s3://test.processing.for.glue.prod/transformed_location_details/")
# trans_loc_df.show(10)
# trans_loc_df.printSchema()
sfdc_df = spark.read.parquet("s3://test.processing.for.glue.prod/sfdc/")
# sfdc_df.show(10)
# sfdc_df.printSchema()
facilities_df = spark.read.parquet("s3://test.processing.for.glue.prod/facilities/")
# facilities_df.show(10)
# facilities_df.printSchema()
# Perform an inner join
join_condition = loc_df["locationid"] == trans_loc_df["_id"]
location_df = loc_df.join(trans_loc_df, join_condition, "inner")
location = location_df.select(["locationkey", "location","facilitykey","locationid","hierarchicalindex","parentkey",loc_df["createdat"],loc_df["updatedat"],"locationType"])
# location.show(10)
# location.printSchema()
resident = alarms_df.select([ alarms_df["originalsubjectid"].alias("residentid"), alarms_df["originalsubjectname"].alias("residentname")])
resident = resident.dropDuplicates()
# resident.show(10)
# resident.printSchema()
events = events_df.select([events_df["id"].alias("eventid"),"alarmid","closing","createdat","facilitykey","closerid","accepterid"])
# events.show(10)
# events.printSchema()
join_condition_2 = events_df["facilitykey"] == sfdc_df["ensureid"]
community_df =  events_df.select(["communityid", events_df["community"].alias("communityname"), "facilitykey"]).join(sfdc_df.select([sfdc_df["parentaccountid"].alias("corporateid"),sfdc_df["parentaccount"].alias("corporatename"), "ensureid"]),join_condition_2, "inner")
# community_df.show(10)
community = community_df.select("communityid", "communityname", "corporateid", "corporatename")
community = community.dropDuplicates()
# community.show(10, truncate=False)
# community.printSchema()
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
alarms = alarms.dropDuplicates()
alarms = alarms.select("alarmid","communityid","residentid","locationkey","dispositiontype","dispositionnote","dispositionauthorkey","openedat","createdat","updatedat","closedat","alarms_status","alarms_type","topic","domain","class")
# alarms.show(10)
# alarms.printSchema()
earliest_event_date_df = alarms.select("alarmid","openedat","closedat").join(events_df.select("alarmid","event","id","createdat"), "alarmid")
# earliest_event_date_df.show(10)
# earliest_event_date_df.printSchema()

# Calculate the earliest response time for each alarmid
earliest_event_date = earliest_event_date_df.groupBy("alarmid").agg(min("createdat").alias("earliest_event_date"))

# Join the result with the original DataFrame
earliest_event_date_df = earliest_event_date_df.join(earliest_event_date, on="alarmid")

# earliest_event_date_df.show(50, truncate = False)
# earliest_event_date_df.printSchema()
# Calculate the earliest event id for each alarmid
earliest_event_id = earliest_event_date_df.orderBy("createdat").groupBy("earliest_event_date").agg(first("id").alias("earliest_event_id"))

# Join the result with the original DataFrame
earliest_event_id_df = earliest_event_date_df.join(earliest_event_id, on="earliest_event_date")

# earliest_event_id_df.show(50, truncate = False)
# earliest_event_id_df.printSchema()

# Calculate the earliest event name for each alarmid
earliest_event_name = earliest_event_id_df.orderBy("createdat").groupBy("earliest_event_date").agg(first("event").alias("earliest_event_name"))

# Join the result with the original DataFrame
earliest_event_name_df = earliest_event_id_df.join(earliest_event_name, on="earliest_event_date")

# earliest_event_name_df.show(50, truncate = False)
# earliest_event_name_df.printSchema()
# Calculate the acceptance time in minutes and round to 3 decimal places
acceptance_time_df = earliest_event_name_df.withColumn(
    "acceptance_time",
    round((unix_timestamp(col("earliest_event_date")) - unix_timestamp(col("openedat"))) / 60, 3)
)

# Show the resulting DataFrame
# acceptance_time_df.show(10, truncate = False)
# acceptance_time_df.printSchema()
# Calculate the transit+caregiver_time time in minutes and round to 3 decimal places
transit_caregiver_time_df = acceptance_time_df.withColumn(
    "transit+caregiver_time",
    round((unix_timestamp(col("closedat")) - unix_timestamp(col("earliest_event_date"))) / 60, 3)
)

# Show the resulting DataFrame
# transit_caregiver_time_df.show(10, truncate = False)
# transit_caregiver_time_df.printSchema()
# Calculate the resolution_time time in minutes and round to 3 decimal places
resolution_time_df = transit_caregiver_time_df.withColumn(
    "resolution_time",
    round((unix_timestamp(col("closedat")) - unix_timestamp(col("openedat"))) / 60, 3)
)

# Show the resulting DataFrame
# resolution_time_df.show(10, truncate = False)
# resolution_time_df.printSchema()
# Add a "alarms_shift" column based on the "closedat"
alarms_shift_df = resolution_time_df.withColumn(
    "alarms_shift",
    when((hour(col("closedat")) >= 8) & (hour(col("closedat")) < 16), "8am-4pm")
    .when((hour(col("closedat")) >= 16) & (hour(col("closedat")) <= 23), "4pm-12am")
    .otherwise("12am-8am")
)
# alarms_shift_df .show(10)
# alarms_shift_df .printSchema()
# Add a "acceptance_time_thresholds" column based on the "closedat"
acceptance_time_thresholds_df = alarms_shift_df.withColumn(
    "acceptance_time_thresholds",
    when(alarms_shift_df["acceptance_time"] < 2, "<2 min")
    .when((alarms_shift_df["acceptance_time"] >= 2) & (alarms_shift_df["acceptance_time"] < 7), "2 - 7 min")
    .when(alarms_shift_df["acceptance_time"] >= 7, "7+ min")
    .otherwise(None)
)
# acceptance_time_thresholds_df.show(10, truncate = False)
# acceptance_time_thresholds_df.printSchema()
# Add a "transit+caregiver_time_thresholds" column based on the "closedat"
transit_caregiver_time_thresholds_df = acceptance_time_thresholds_df.withColumn(
    "transit+caregiver_time_thresholds",
    when(acceptance_time_thresholds_df["transit+caregiver_time"] < 3, "<3 min")
    .when(acceptance_time_thresholds_df["transit+caregiver_time"] >= 3, "3+ min")
    .otherwise(None)
)
# transit_caregiver_time_thresholds_df.show(10, truncate = False)
# transit_caregiver_time_thresholds_df.printSchema()
# Add a "resolution_time_thresholds" column based on the "closedat"
resolution_time_thresholds_df = transit_caregiver_time_thresholds_df.withColumn(
    "resolution_time_thresholds",
    when(transit_caregiver_time_thresholds_df["resolution_time"] < 5, "<5 min")
    .when((transit_caregiver_time_thresholds_df["resolution_time"] >= 5) & (transit_caregiver_time_thresholds_df["resolution_time"] < 15), "5 - 15 min")
    .when(transit_caregiver_time_thresholds_df["resolution_time"] >= 15, "15+ min")
    .otherwise(None)
)
# resolution_time_thresholds_df.show(10, truncate = False)
# resolution_time_thresholds_df.printSchema()
derived_fields_df = resolution_time_thresholds_df.withColumn("closedat_date", date_format(resolution_time_thresholds_df["closedat"], "yyyy-MM-dd"))
derived_fields_df = derived_fields_df.withColumn("closedat_time", date_format(derived_fields_df["closedat"], "HH:mm:ss"))
derived_fields_df = derived_fields_df.withColumn("closedat_day", dayofmonth(derived_fields_df["closedat"]))
derived_fields_df = derived_fields_df.withColumn("closedat_month", month(derived_fields_df["closedat"]))
derived_fields_df = derived_fields_df.withColumn("closedat_year", year(derived_fields_df["closedat"]))
derived_fields_df = derived_fields_df.withColumn("closedat_hour", hour(derived_fields_df["closedat"]))
derived_fields_df = derived_fields_df.withColumn("closedat_minute", minute(derived_fields_df["closedat"]))
derived_fields_df = derived_fields_df.withColumn("closedat_second", second(derived_fields_df["closedat"]))
derived_fields_df = derived_fields_df.withColumn("closedat_week", weekofyear(derived_fields_df["closedat"]))
derived_fields_df = derived_fields_df.withColumn("closedat_quarter", quarter(derived_fields_df["closedat"]))

# derived_fields_df.show(10, truncate = False)
# derived_fields_df.printSchema()
alarms = derived_fields_df.select("alarmid",
                         "earliest_event_id",
                        "earliest_event_date",
                         "earliest_event_name",
                         "earliest_event_date",
                         "acceptance_time",
                         "transit+caregiver_time",
                         "resolution_time",
                         "alarms_shift",
                         "acceptance_time_thresholds",
                         "transit+caregiver_time_thresholds",
                         "resolution_time_thresholds",
                         "closedat_date",
                         "closedat_time",
                         "closedat_day",
                         "closedat_month",
                         "closedat_year",
                         "closedat_hour",
                         "closedat_minute",
                         "closedat_second",
                         "closedat_week",
                         "closedat_quarter").join(alarms, "alarmid")

# ordering the columns
alarms = alarms.select("alarmid",
                        "communityid",
                        "residentid",
                        "locationkey",
                        "dispositiontype",
                        "dispositionnote",
                        "dispositionauthorkey",
                        "openedat",
                        "createdat",
                        "updatedat",
                        "closedat",
                        "alarms_status",
                        "alarms_type",
                        "topic",
                        "domain",
                        "class",
                         "earliest_event_id",
                         "earliest_event_date",
                        "earliest_event_name",
                        "alarms_shift",
                         "closedat_date",
                         "closedat_time",
                         "closedat_day",
                         "closedat_month",
                         "closedat_year",
                         "closedat_hour",
                         "closedat_minute",
                         "closedat_second",
                         "closedat_week",
                         "closedat_quarter",
                         "acceptance_time",
                         "transit+caregiver_time",
                         "resolution_time",
                         "acceptance_time_thresholds",
                         "transit+caregiver_time_thresholds",
                         "resolution_time_thresholds"
                        )

alarms.dropDuplicates()
# alarms.show(10, truncate = False)
# alarms.printSchema()
# Filter out alarms with acceptance time less than 5 seconds (0.0833 minutes)
alarms = alarms.filter(col("acceptance_time") > 0.08333)

# Show the filtered DataFrame
# alarms.show(10, truncate = False)
# Creating a separate table for alarms that haven’t been responded to for 2+ hours.
alarms_outliers = alarms.filter(col("acceptance_time") >= 120)

# Show the filtered DataFrame
# alarms_outliers.show(10, truncate = False)
alarms_less_than_2_hr = alarms.filter(col("acceptance_time") < 120)
# alarms_less_than_2_hr.show(10, truncate = False)
# Filtering alarms that have both open and close dates; i.e. exclude 'openedat' and 'closedat' that null values
alarms = alarms.filter( (col("openedat").isNotNull()) | (col("closedat").isNotNull()))

# Show the filtered DataFrame
# alarms.show(10, truncate = False)
alarms_less_than_2_hr = alarms_less_than_2_hr.filter( (col("openedat").isNotNull()) | (col("closedat").isNotNull()))
# alarms_less_than_2_hr.show(10, truncate = False)
alarms_facility_community= alarms.join(facilities_df.select("communityid","facility"),"communityid").join(community.select("communityid","communityname"),"communityid")
alarms_facility_community.dropDuplicates()

# Filter out records that don't have "QA" or "Dev" in the facility and community columns 
alarms_facility_community = alarms_facility_community.filter(~(col("facility").like("%QA%") | col("facility").like("%Dev%")) & ~(col("communityname").like("%QA%") | col("communityname").like("%Dev%"))) 

# Show the filtered DataFrame
# alarms_facility_community.show(10, truncate = False)
# alarms_facility_community.printSchema()
alarms_1 = alarms_facility_community.select("alarmid",
                        "communityid",
                        "residentid",
                        "locationkey",
                        "dispositiontype",
                        "dispositionnote",
                        "dispositionauthorkey",
                        "openedat",
                        "createdat",
                        "updatedat",
                        "closedat",
                        "alarms_status",
                        "alarms_type",
                        "topic",
                        "domain",
                        "class",
                         "earliest_event_id",
                         "earliest_event_date",
                        "earliest_event_name",
                        "alarms_shift",
                         "closedat_date",
                         "closedat_time",
                         "closedat_day",
                         "closedat_month",
                         "closedat_year",
                         "closedat_hour",
                         "closedat_minute",
                         "closedat_second",
                         "closedat_week",
                         "closedat_quarter",
                         "acceptance_time",
                         "transit+caregiver_time",
                         "resolution_time",
                         "acceptance_time_thresholds",
                         "transit+caregiver_time_thresholds",
                         "resolution_time_thresholds"
                        )
# alarms_1.show(10, truncate = False)
# alarms_1.printSchema()
alarms_less_than_2_hr_filtered = alarms_less_than_2_hr.join(facilities_df.select("communityid","facility"),"communityid").join(community.select("communityid","communityname"),"communityid")
alarms_less_than_2_hr_filtered.dropDuplicates()

# Filter out records that don't have "QA" or "Dev" in the facility and community columns 
alarms_less_than_2_hr_filtered = alarms_less_than_2_hr_filtered.filter(~(col("facility").like("%QA%") | col("facility").like("%Dev%")) & ~(col("communityname").like("%QA%") | col("communityname").like("%Dev%"))) 

# alarms_less_than_2_hr_filtered.show(10, truncate = False)
# alarms_less_than_2_hr_filtered.printSchema()
alarms_2 = alarms_less_than_2_hr_filtered.select("alarmid",
                        "communityid",
                        "residentid",
                        "locationkey",
                        "dispositiontype",
                        "dispositionnote",
                        "dispositionauthorkey",
                        "openedat",
                        "createdat",
                        "updatedat",
                        "closedat",
                        "alarms_status",
                        "alarms_type",
                        "topic",
                        "domain",
                        "class",
                         "earliest_event_id",
                         "earliest_event_date",
                        "earliest_event_name",
                        "alarms_shift",
                         "closedat_date",
                         "closedat_time",
                         "closedat_day",
                         "closedat_month",
                         "closedat_year",
                         "closedat_hour",
                         "closedat_minute",
                         "closedat_second",
                         "closedat_week",
                         "closedat_quarter",
                         "acceptance_time",
                         "transit+caregiver_time",
                         "resolution_time",
                         "acceptance_time_thresholds",
                         "transit+caregiver_time_thresholds",
                         "resolution_time_thresholds"
                        )
# alarms_2.show(10, truncate = False)
# alarms_2.printSchema()
resident.write.parquet("s3://test.processing.for.glue.prod.output/resident/")
events.write.parquet("s3://test.processing.for.glue.prod.output/events/")
community.write.parquet("s3://test.processing.for.glue.prod.output/community/")
location.write.parquet("s3://test.processing.for.glue.prod.output/location/")
alarms_1.write.parquet("s3://test.processing.for.glue.prod.output/alarms_orignal/")
alarms_2.write.parquet("s3://test.processing.for.glue.prod.output/alarms_less_than_2hr/")
# spark.stop()
job.commit()