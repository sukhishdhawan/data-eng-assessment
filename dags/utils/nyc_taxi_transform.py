from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyAppName") \
    .getOrCreate()



# reading all tables

project_id = 'trusty-drive-434711-g9'
dataset = 'nyc_taxi'



fhv_df = spark.read.format('bigquery') \
    .option('project_id', project_id) \
    .option('dataset', dataset) \
    .option('table', 'fhv_tripdata') \
    .load()

fhv_df.createOrReplaceTempView('fhv')

fhvhv_df = spark.read.format('bigquery') \
    .option('project_id', project_id) \
    .option('dataset', dataset) \
    .option('table', 'fhvhv_tripdata') \
    .load()

fhvhv_df.createOrReplaceTempView('fhvhv')

green_df = spark.read.format('bigquery') \
    .option('project_id', project_id) \
    .option('dataset', dataset) \
    .option('table', 'green_tripdata') \
    .load()

green_df.createOrReplaceTempView('green')

yellow_df = spark.read.format('bigquery') \
    .option('project_id', project_id) \
    .option('dataset', dataset) \
    .option('table', 'yellow_tripdata') \
    .load()

yellow_df.createOrReplaceTempView('yellow')

location_lookup_df = spark.read.format('bigquery') \
    .option('project_id', project_id) \
    .option('dataset', dataset) \
    .option('table', 'taxi_zone_lookup') \
    .load()

location_lookup_df.cache() # caching the df so that it can be used anywhere multiple times

location_lookup_df.createOrReplaceTempView('location_lookup')


# creating denormalized df

transformed_df = spark.sql(
    """ 
    select 
    nvl(pickup.zone , 'Unknown') pickup_zone,
    nvl(drop.zone ,'Unknown') drop_zone,
    pickup_datetime,
    dropOff_datetime,
    'FHV TRIP' as trip_type,
    file_month
    from fhv
    left join location_lookup pickup on pickup.LocationID = fhv.PUlocationID
    left join location_lookup drop on drop.LocationID = fhv.DOlocationID
    
    union all
    
    select 
    nvl(pickup.zone , 'Unknown') pickup_zone,
    nvl(drop.zone ,'Unknown') drop_zone,
    pickup_datetime,
    dropOff_datetime,
    'FHVHV TRIP' as trip_type,
    file_month
    from fhvhv
    left join location_lookup pickup on pickup.LocationID = fhvhv.PUlocationID
    left join location_lookup drop on drop.LocationID = fhvhv.DOlocationID
    
    union all
    
    select 
    nvl(pickup.zone , 'Unknown') pickup_zone,
    nvl(drop.zone ,'Unknown') drop_zone,
    lpep_pickup_datetime,
    lpep_dropOff_datetime,
    'GREEN TRIP' as trip_type,
    file_month
    from green
    left join location_lookup pickup on pickup.LocationID = green.PUlocationID
    left join location_lookup drop on drop.LocationID = green.DOlocationID
    
    union all
    
    select 
    nvl(pickup.zone , 'Unknown') pickup_zone,
    nvl(drop.zone ,'Unknown') drop_zone,
    tpep_pickup_datetime,
    tpep_dropOff_datetime,
    'YELLOW TRIP' as trip_type,
    file_month
    from yellow
    left join location_lookup pickup on pickup.LocationID = yellow.PUlocationID
    left join location_lookup drop on drop.LocationID = yellow.DOlocationID
    
    
    """

)


transformed_df.write \
    .format("bigquery") \
    .option("table", "trusty-drive-434711-g9.nyc_taxi.trip_data_consolidated") \
    .option("partitionField", "file_month") \
    .option("clusteredFields", "trip_type") \
    .option("writeMethod", "direct") \
    .mode("overwrite") \
    .save()


