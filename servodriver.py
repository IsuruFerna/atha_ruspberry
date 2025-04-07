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
consumer.subscribe([KAFKA_TOPIC])

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

    ############## arm ####################
    arm_wrist_R = servo.Servo(pca.channels[8])
    arm_elbow_R = servo.Servo(pca.channels[9])
    arm_sholder_R = servo.Servo(pca.channels[10])


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
                detected_angles = json.loads(data)
                
                # set servo angles
                hand_R = detected_angles["hand_R"]
                arm_R = detected_angles["arm_R"]

                finger_thumb_R.angle = hand_R["thumb"]
                finger_index_R.angle = hand_R["thumb"]
                finger_middle_R.angle = hand_R["thumb"]
                finger_ring_R.angle = hand_R["thumb"]
                finger_pinkey_R.angle = hand_R["thumb"]

                arm_sholder_R.angle = arm_R["wrist"]
                arm_elbow_R.angle = arm_R["elbow"]
                arm_sholder_R.angle = arm_R["sholder"]

                # FIXME: should I keep this?
                pca.deinit()

            except Exception as e:
                print(f"Error processing message: {e}")


    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()





except Exception as e:
    print(f"Error: {e}")
    print("PCA9685 not deteccted. heck wiring or I2C address.")
