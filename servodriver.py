import time
import board
from adafruit_motor import servo
from adafruit_pca9685 import PCA9685

i2c = board.I2C()

try:
    pca = PCA9685(i2c)
    pca.frequency = 50

    print("PCA9685 detected! Connection successful.")
    print("I2C Address: 0x40")  # PCA9685 defaults to 0x40
    print(f"PWM Frequency: {pca.frequency}Hz")

    servo_0 = servo.Servo(pca.channels[0])
    servo_1 = servo.Servo(pca.channels[1])
    servo_2 = servo.Servo(pca.channels[2])
    servo_3 = servo.Servo(pca.channels[3])
    servo_4 = servo.Servo(pca.channels[4])

    current_angle = 0
    
    servo_0.angle = current_angle
    time.sleep(0.05)

    print(f"this is initial angle: {current_angle}")

    for i in range(180):
        current_angle = i
        servo_0.angle = i
        servo_1.angle = i
        servo_2.angle = i
        servo_3.angle = i
        servo_4.angle = i

        time.sleep(0.03)
        print(f"first loop: {current_angle}")
    for i in range(180):
        servo_0.angle = 180 - i
        servo_1.angle = 180 - i
        servo_2.angle = 180 - i
        servo_3.angle = 180 - i
        servo_4.angle = 180 - i

        current_angle = 180 - i
        time.sleep(0.03)
        print(f"second loop: {current_angle}")

    print(f"ending angle: {current_angle}")

    pca.deinit()

except Exception as e:
    print(f"Error: {e}")
    print("PCA9685 not deteccted. heck wiring or I2C address.")
