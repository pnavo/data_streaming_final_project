# schemas.py

taxi_ride_schema = """
{
  "type": "record",
  "name": "Trip",
  "fields": [
    
    {"name": "VendorID", "type": "int"},
    {"name": "tpep_pickup_datetime", "type": "string"},
    {"name": "tpep_dropoff_datetime", "type": "string"},
    {"name": "passenger_count", "type": "float"},
    {"name": "trip_distance", "type": "float"},
    {"name": "RatecodeID", "type": "float"},
    {"name": "store_and_fwd_flag", "type": "string"},
    {"name": "PULocationID", "type": "int"},
    {"name": "DOLocationID", "type": "int"},
    {"name": "payment_type", "type": "int"},
    {"name": "fare_amount", "type": "float"},
    {"name": "extra", "type": "float"},
    {"name": "mta_tax", "type": "float"},
    {"name": "tip_amount", "type": "float"},
    {"name": "tolls_amount", "type": "float"},
    {"name": "improvement_surcharge", "type": "float"},
    {"name": "total_amount", "type": "float"},
    {"name": "congestion_surcharge", "type": "float"},
    {"name": "airport_fee", "type": "float"}
 
  ]
}
"""