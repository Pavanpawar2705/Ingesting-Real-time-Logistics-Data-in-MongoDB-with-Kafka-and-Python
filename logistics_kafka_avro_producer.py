import datetime
import threading
from decimal import *
from time import sleep
from uuid import uuid4, UUID

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'GPTQM7BVI6ISY2YA',
    'sasl.password': 'buTrxfhk6IRIBO/3wE+67ZkS1EZkgLHw0SyAwNueHOyK6h6+PepeAel0M6Jc6Ir9'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-3r6ym.ap-southeast-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('M6V25BB27G7GUXVT', 'zkWJqv5buxIhbMn8h9NEriu8R23/35Gs0hVr2CuM//I/nPnlbhioJ6fo1/do4m8L')
})

# Fetch the latest Avro schema for the value
subject_name = 'log-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
# key_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str='{"type": "string"}')
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})



# Load the CSV data into a pandas DataFrame
df = pd.read_csv('delivery_trip_truck_data.csv')
#df[['Curr_lat','Curr_lon']] = df[['Curr_lat','Curr_lon']].astype(str)
df['Minimum_kms_to_be_covered_in_a_day'].fillna(0,inplace=True)
df['Driver_MobileNo'] = df['Driver_MobileNo'].astype(str)
df = df.fillna('null')


# Iterate over DataFrame rows and produce to Kafka
for index, row in df.iterrows():
    # Create a dictionary from the row values
    value = row.to_dict()
    # Produce to Kafka
    producer.produce(topic='log', key=str(index), value=value, on_delivery=delivery_report)
    producer.flush()
    #break

print("All Data successfully published to Kafka")
