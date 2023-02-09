import configparser
import json

from b2sdk.account_info import InMemoryAccountInfo
from b2sdk.api import B2Api
from kafka import KafkaConsumer

# Reading Configs
config = configparser.ConfigParser()
config.read("config.ini")

# File information
download_dir = config["Common"]["download_dir"]

# Kafka setting
event_type = "save_event"
topic = "saver"
group_id = "sav"

# Backblaze config
bucket_name = config["Backblaze"]["bucket_name"]
base_url = config["Backblaze"]["base_url"]
application_key_id = config["Backblaze"]["application_key_id"]
application_key = config["Backblaze"]["application_key"]

# Backblaze setup
info = InMemoryAccountInfo()
b2_api = B2Api(info)
b2_api.authorize_account("production", application_key_id, application_key)

# Consumer setup
consumer = KafkaConsumer(
    topic,
    group_id=group_id,
    enable_auto_commit=False,
    max_poll_records=1
)


def upload_file(name, extension):
    bucket = b2_api.get_bucket_by_name(bucket_name)
    bucket.upload_local_file(f"{download_dir}{name}{extension}",
                             file_name=f"{name}{extension}")

    print(f"Successfully uploaded: {name}{extension}")


def process_data(data):
    upload_file(data["name"], data["ext"])
    with open(f"{download_dir}{data['name']}.s.hook", "w"):
        pass


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
