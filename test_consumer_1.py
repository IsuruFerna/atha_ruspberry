import os
import json
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException, KafkaError

# load .env file 
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=dotenv_path)

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'streaming')

# Kafka config
conf = {
    'bootstrap.servers': KAFKA_BROKER_URL,
    'group.id': "mygroup",
    'auto.offset.reset': 'earliest',
}
consumer = Consumer(**conf)

consumer = Consumer(conf)
consumer.subscribe([KAFKA_TOPIC])


# consume incoming messages
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())
        
        # print('Received message: {}'.format(msg.value().decode('utf-8')))
        # handle movements
        try:
            data = msg.value().decode("utf-8")
            print(f"Received message: {data}")
            data_dict = json.loads(data)

        except Exception as e:
            print(f"Error processing message: {e}")


except KeyboardInterrupt:
    pass
finally:
    consumer.close()