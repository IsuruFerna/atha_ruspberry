import os
from dotenv import load_dotenv
import time
import board
from adafruit_motor import servo
from adafruit_pca9685 import PCA9685
from confluent_kafka import Consumer, KafkaException, KafkaError
import json

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

# board config
i2c = board.I2C()

try:
    pca = PCA9685(i2c)
    pca.frequency = 50

    print("PCA9685 detected! Connection successful.")
    print("I2C Address: 0x40")  # PCA9685 defaults to 0x40
    print(f"PWM Frequency: {pca.frequency}Hz")

    ############## right hand fingers ####################
    finger_thumb_R = servo.Servo(pca.channels[0])
    finger_index_R = servo.Servo(pca.channels[1])
    finger_middle_R  = servo.Servo(pca.channels[2])
    finger_ring_R = servo.Servo(pca.channels[3])
    finger_pinkey_R = servo.Servo(pca.channels[4])

    current_angle = 0
    
    finger_thumb_R.angle = current_angle
    time.sleep(0.05)

    print(f"this is initial angle: {current_angle}")

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

    for i in range(180):
        current_angle = i
        finger_thumb_R.angle = i
        finger_index_R.angle = i
        finger_middle_R.angle = i
        finger_ring_R.angle = i
        finger_pinkey_R.angle = i

        time.sleep(0.03)
        print(f"first loop: {current_angle}")
    for i in range(180):
        finger_thumb_R.angle = 180 - i
        finger_index_R.angle = 180 - i
        finger_middle_R.angle = 180 - i
        finger_ring_R.angle = 180 - i
        finger_pinkey_R.angle = 180 - i

        current_angle = 180 - i
        time.sleep(0.03)
        print(f"second loop: {current_angle}")

    print(f"ending angle: {current_angle}")

    pca.deinit()

except Exception as e:
    print(f"Error: {e}")
    print("PCA9685 not deteccted. heck wiring or I2C address.")
