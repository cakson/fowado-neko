import asyncio
import base64
import configparser
import json

from b2sdk.account_info import InMemoryAccountInfo
from b2sdk.api import B2Api
from kafka import KafkaProducer
from telethon import TelegramClient

# Reading Configs
config = configparser.ConfigParser()
config.read("config.ini")

# API ID and hash obtained from https://my.telegram.org
api_id = int(config["Telegram"]["api_id"])
api_hash = config["Telegram"]["api_hash"]
session_name = "seeker"

# Common information
download_dir = config["Common"]["download_dir"]
host_min_file_size = float(config["Common"]["host_min_file_size_mb"]) * 1024 * 1024
source_id = int(config["Common"]["source_id"])
runner_name = config["Common"]["source_id"]
target_id = int(config["Common"]["target_id"])
offset = int(config["Common"]["offset"])
limit = int(config["Common"]["limit"])
group_name = config["Telegram"]["group_name"]

# Producer config
download_topic = "downloader"
publish_topic = "publisher"

# Producer setup
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Initialize the client
client = TelegramClient(session_name, api_id, api_hash)

# Backblaze config
bucket_name = config["Backblaze"]["bucket_name"]
base_url = config["Backblaze"]["base_url"]
application_key_id = config["Backblaze"]["application_key_id"]
application_key = config["Backblaze"]["application_key"]

# Backblaze setup
info = InMemoryAccountInfo()
b2_api = B2Api(info)
b2_api.authorize_account("production", application_key_id, application_key)
bucket = b2_api.get_bucket_by_name(bucket_name)


async def send_download_message(name, extension, message_id):
    message = {
        "type": "download_event",
        "data": {
            "message_id": message_id,
            "group_name": group_name,
            "name": name,
            "ext": extension
        }
    }

    producer.send(download_topic, value=message)
    producer.flush()
    print(f"Download message sent: {name}{extension}.")


async def send_publish_message(
        name,
        extension,
        media_type,
        message_text,
        file_size,
        message_id,
        is_duplicate
):
    message = {
        "type": "publish_event",
        "data": {
            "runner": runner_name,
            "source_id": source_id,
            "target_id": target_id,
            "type": media_type,
            "name": name,
            "ext": extension,
            "message_text": message_text,
            "file_size": file_size,
            "group_name": group_name,
            "message_id": message_id,
            "is_duplicate": is_duplicate
        }
    }

    producer.send(publish_topic, value=message)
    producer.flush()
    print(f"Publish message sent: {name}{extension}.")


def get_file_in_bucket(filename):
    try:
        file_info = bucket.get_file_info_by_name(filename)
        return file_info
    except Exception:
        return None


async def process_media_message(message):
    if message.media and (message.photo or message.video) and not message.is_reply:
        print(f"Processing message: {message.id}.")

        # Determine the file extension based on the type of media
        if message.photo:
            media_type = "image"
            file_extension = '.jpg'
        else:
            media_type = "video"
            file_extension = '.mp4'

        # Construct the filename
        file_name = "{}-{}".format(group_name, message.id)
        filename_base64 = base64.urlsafe_b64encode(file_name.encode()).decode()

        file_size = message.file.size
        if file_size <= host_min_file_size:
            await send_download_message(filename_base64, file_extension, message.id)
            await send_publish_message(
                name=filename_base64,
                extension=file_extension,
                media_type=media_type,
                message_text=message.message,
                file_size=file_size / (1024 * 1024),
                message_id=message.id,
                is_duplicate=False
            )
        else:
            media_type = "video_hosted"
            file_info = get_file_in_bucket(f"{filename_base64}{file_extension}")
            if file_info:
                await send_publish_message(
                    name=filename_base64,
                    extension=file_extension,
                    media_type=media_type,
                    message_text=message.message,
                    file_size=file_size / (1024 * 1024),
                    message_id=message.id,
                    is_duplicate=True
                )
            else:
                await send_download_message(filename_base64, file_extension, message.id)
                await send_publish_message(
                    name=filename_base64,
                    extension=file_extension,
                    media_type=media_type,
                    message_text=message.message,
                    file_size=file_size / (1024 * 1024),
                    message_id=message.id,
                    is_duplicate=False
                )


async def main():
    await client.start()
    user = await client.get_me()
    print(f'Telegram logged in as {user.username}')

    # Get the group entity
    group_entity = None
    groups = await client.get_dialogs()
    for g in groups:
        if g.name == group_name:
            group_entity = g
            break

    if not group_entity:
        raise Exception("group not found")

    messages = await client.get_messages(
        group_entity, offset_id=offset, limit=limit)

    for message in messages:
        await process_media_message(message)

    _ = messages


asyncio.run(main())
