import configparser
import json

import numpy as np
from cv2 import cv2
from kafka import KafkaConsumer, KafkaProducer

# Reading Configs
config = configparser.ConfigParser()
config.read("config.ini")

# File information
download_dir = config["Common"]["download_dir"]

# Consumer setting
event_type = "capture_event"
topic = "capturer"
group_id = "cap"

# Producer setting
producer_topic = "saver"

# Consumer setup
consumer = KafkaConsumer(
    topic,
    group_id=group_id,
    enable_auto_commit=False,
    max_poll_records=1
)

# Producer setup
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def send_save_message(name, extension):
    message = {
        "type": "save_event",
        "data": {
            "name": name,
            "ext": extension
        }
    }

    producer.send(producer_topic, value=message)
    producer.flush()
    print(f"Save message sent: {name}{extension}.")


def capture_from_video(name, extension):
    # Read video from the temporary file
    video = cv2.VideoCapture(f"{download_dir}{name}{extension}")

    # Read the first frame from the video
    success, frame = video.read()

    # Check if the frame was read successfully
    if success:
        # Convert the frame to a numpy array
        frame = np.array(frame)

        # Store the frame locally
        cv2.imwrite(f"{download_dir}{name}.jpg", frame)

    # Release the video capture object
    video.release()

    print(f"Successfully captured: {name}{extension}.")


def process_data(data):
    capture_from_video(data["name"], data["ext"])
    with open(f"{download_dir}{data['name']}.c.hook", "w"):
        pass
    send_save_message(data["name"], ".jpg")


def main():
    while True:
        msg = next(consumer)
        msg_body = msg.value

        try:
            deserialized_data = json.loads(msg_body.decode("utf-8"))
        except json.JSONDecodeError as e:
            print(f"Failed to deserialize message: {e}")
            continue

        if deserialized_data["type"] == event_type:
            try:
                process_data(deserialized_data["data"])
            except Exception as e:
                print(e)
                consumer.commit()
                continue

        consumer.commit()


main()
