import asyncio
import configparser
import json

from kafka import KafkaConsumer, KafkaProducer
from telethon import TelegramClient

# Reading Configs
config = configparser.ConfigParser()
config.read("config.ini")

# API ID and hash obtained from https://my.telegram.org
api_id = int(config["Telegram"]["api_id"])
api_hash = config["Telegram"]["api_hash"]
session_name = config["Telegram"]["session_name"]

# File information
download_dir = config["Common"]["download_dir"]
host_min_file_size = float(config["Common"]["host_min_file_size_mb"]) * 1024 * 1024

# Consumer setting
event_type = "download_event"
topic = "downloader"
group_id = "dow"

# Producer setting
save_topic = "saver"
capture_topic = "capturer"

# Initialize the client
client = TelegramClient(session_name, api_id, api_hash)

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


async def send_save_message(name, extension):
    message = {
        "type": "save_event",
        "data": {
            "name": name,
            "ext": extension
        }
    }

    producer.send(save_topic, value=message)
    producer.flush()
    print(f"Save message sent: {name}{extension}.")


async def send_capture_message(name, extension):
    message = {
        "type": "capture_event",
        "data": {
            "name": name,
            "ext": extension
        }
    }

    producer.send(capture_topic, value=message)
    producer.flush()
    print(f"Capture message sent: {name}{extension}.")


async def process_message(message, name, extension):
    if message.media and (message.photo or message.video) and not message.is_reply:
        await client.download_media(message, f"{download_dir}{name}{extension}")


async def process_data(data):
    # Get the group entity
    group_entity = None
    groups = await client.get_dialogs()
    for g in groups:
        if g.name == data["group_name"]:
            group_entity = g
            break

    # get the message by ID
    messages = await client.get_messages(group_entity, ids=[data["message_id"]])

    if len(messages) > 0:
        message = messages[0]
        await process_message(message, data["name"], data["ext"])
        if message.file.size > host_min_file_size:
            await send_capture_message(data["name"], data["ext"])
            await send_save_message(data["name"], data["ext"])
        else:
            with open(f"{download_dir}{data['name']}.d.hook", "w"):
                pass


async def main():
    await client.start()
    user = await client.get_me()
    print(f'Telegram logged in as {user.username}')

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
                await process_data(deserialized_data["data"])
            except Exception as e:
                print(e)
                consumer.commit()
                continue

        consumer.commit()


asyncio.run(main())
