import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

from pymongo import MongoClient

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'GPTQM7BVI6ISY2YA',
    'sasl.password': 'buTrxfhk6IRIBO/3wE+67ZkS1EZkgLHw0SyAwNueHOyK6h6+PepeAel0M6Jc6Ir9',
    'group.id': 'group12',
    'auto.offset.reset': 'latest'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-3r6ym.ap-southeast-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('M6V25BB27G7GUXVT', 'zkWJqv5buxIhbMn8h9NEriu8R23/35Gs0hVr2CuM//I/nPnlbhioJ6fo1/do4m8L')
})

# Fetch the latest Avro schema for the value
subject_name = 'log-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

# Subscribe to the 'retail_data' topic
consumer.subscribe(['log'])

#Intializing a Mongo Instance
conn_string='mongodb+srv://powerpavan23:Poppytillu@cluster0.qoho0k0.mongodb.net/?retryWrites=true&w=majority'
client = MongoClient(conn_string)
db = client['gds_db']
logistics_test_collection = db['logistics_test']

#Continually read messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0) # How many seconds to wait for message

        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue
        
        insert_result = logistics_test_collection.insert_one(msg.value())
        print(f"Document inserted with id: {insert_result.inserted_id}")
        print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))

except KeyboardInterrupt:
    pass
finally:
    consumer.close()