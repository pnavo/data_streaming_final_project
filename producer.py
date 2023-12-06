# producer.py

import boto3
import logging
import pyarrow.parquet as pq
from fastavro import reader
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import SerializingProducer
# import confluent_kafka
from confluent_kafka.serialization import StringSerializer  # Serializer for message keys
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import io
from dotenv import load_dotenv
import os
from datetime import datetime
import concurrent.futures

import random
import schemas

# Load environment variables from .env file
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

exp_num = os.getenv('EXP_NUMBER')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')
s3_bucket = os.getenv('S3_BUCKET')
s3_prefix = os.getenv('S3_PREFIX')
kafka_topic = os.getenv('TOPIC') + str(exp_num)
schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL')
schema_registry_basic_auth_user_info = os.getenv('AUTH_USER_INFO')
bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS')



def get_kafka_config():
    return {
        'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'],
        'security.protocol': os.environ['SECURITY_PROTOCOL'],
        'sasl.mechanisms': os.environ['SASL_MECHANISMS'],
        'sasl.username': os.environ['SASL_USERNAME'],
        'sasl.password': os.environ['SASL_PASSWORD']
    }


def check_and_create_topic(topic_name):
    # admin_client_conf = {'bootstrap.servers': bootstrap_servers}
    admin_client = AdminClient(get_kafka_config())

    try:
        new_topic = NewTopic(topic=kafka_topic, num_partitions=3, replication_factor=3)
        futures = admin_client.create_topics([new_topic])
        for topic, future in futures.items():
            future.result()
            logger.info(f"Created topic {topic}.")
    except Exception as e:
        logger.warning(f'An error occurred while createing topic: {e}')
    

def get_s3_parquet_files(bucket, prefix, aws_access_key, aws_secret_key, aws_region):
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]

    return files

def read_parquet_file_from_s3(bucket, key, aws_access_key, aws_secret_key, aws_region):
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)

    response = s3.get_object(Bucket=bucket, Key=key)
    parquet_data = response['Body'].read()

    return parquet_data

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def produce_parquet_data_to_kafka(parquet_data, producer, topic):
    table = pq.read_table(io.BytesIO(parquet_data))
    records = table.to_pandas().to_dict(orient='records')
    
    for i, record in enumerate(records):
        # convert timestamps to strings
        record["tpep_pickup_datetime"] = record["tpep_pickup_datetime"].strftime('%Y-%m-%d')
        record["tpep_dropoff_datetime"] = record["tpep_dropoff_datetime"].strftime('%Y-%m-%d')
        
        if record == {}:
            break

        # Asynchronously produce the messages
        producer.produce(
            topic=topic,
            value=record
            # callback=delivery_report
        )
        if i % 10_000 == 0:
            producer.flush()


def make_producer(schema_str: str) -> SerializingProducer:
    schema_registry_conf = {
        'url': schema_registry_url,
        'basic.auth.user.info': schema_registry_basic_auth_user_info
    }
    schema_reg_client = SchemaRegistryClient(schema_registry_conf)

    # print(schema_str)
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_reg_client,
        schema_str=schema_str,
        to_dict=lambda data, ctx: data if isinstance(data, dict) else data.dict(by_alias=True)
    )

    producer_conf = get_kafka_config()
    producer_conf.update({
        'acks': 'all',
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer,
    })

    return SerializingProducer(producer_conf)

def main():
    # Check and create Kafka topic if it doesn't exist
    check_and_create_topic(kafka_topic)
    # check_and_create_topic()

    s3_files = get_s3_parquet_files(s3_bucket, s3_prefix, aws_access_key_id, aws_secret_access_key, aws_region)

    if not s3_files:
        print("No Parquet files found in S3.")
        return

    producer = make_producer(schemas.taxi_ride_schema)

    for s3_file in s3_files:
        print(f"Processing: {s3_file}")
        parquet_data = read_parquet_file_from_s3(s3_bucket, s3_file, aws_access_key_id, aws_secret_access_key, aws_region)
        produce_parquet_data_to_kafka(parquet_data, producer, kafka_topic)

    producer.flush()

if __name__ == "__main__":
    main()
