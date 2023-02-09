import asyncio
import configparser
import json
import os
import time

import discord
from kafka import KafkaConsumer

# Reading Configs
config = configparser.ConfigParser()
config.read("config.ini")

# Discord bot token
bot_token = config["Discord"]["bot_token"]

# File information
download_dir = config["Common"]["download_dir"]
base_url = config["Backblaze"]["base_url"]

source_id = 1
event_type = "publish_event"

topic = "publisher"
group_id = f"pub_{source_id}"

consumer = KafkaConsumer(
    topic,
    group_id=group_id,
    enable_auto_commit=False,
    max_poll_records=1
)

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)


async def cleanup(file_name):
    # Get the base name and extension of the file
    base, ext = os.path.splitext(file_name)

    # Create a list of extensions to check for
    extensions = [".jpg", ".mp4", ".d.hook", ".s.hook", ".c.hook"]

    # Iterate over the extensions
    for extension in extensions:
        # Construct the file path
        file_path = base + extension

        # Check if the file exists
        if os.path.exists(f"{download_dir}{file_path}"):
            # Delete the file
            os.remove(f"{download_dir}{file_path}")
            print(f"Deleted file: {file_path}")


@client.event
async def on_ready():
    print(f'{client.user} has connected to Discord!')

    try:
        await main()
    except Exception as e:
        print(f"error: {e}")
        await client.close()


async def process_data(data):
    if data["type"] == "video" or data["type"] == "image":
        max_retry = 450
        duration = 2

        for attempt in range(max_retry):
            if os.path.exists(f"{download_dir}{data['name']}.d.hook"):
                time.sleep(1)
                await process_media_post(
                    channel_id=data["target_id"],
                    file_ext=data["ext"],
                    file_name=data["name"],
                    runner_type=data["runner"],
                    group_name=data["group_name"],
                    message_id=data["message_id"],
                    message_text=data["message_text"],
                    file_size=data["file_size"]
                )
                break
            else:
                print("waiting for " + data["name"])
                if attempt == max_retry - 1:
                    raise FileNotFoundError("hook file not found")
                time.sleep(duration)

    if data["type"] == "video_hosted" and not data["is_duplicate"]:
        max_retry = 450
        duration = 2

        for attempt in range(max_retry):
            if os.path.exists(f"{download_dir}{data['name']}.c.hook"):
                time.sleep(1)
                await process_self_hosted_video_post(
                    channel_id=data["target_id"],
                    file_name=data["name"],
                    runner_type=data["runner"],
                    group_name=data["group_name"],
                    message_id=data["message_id"],
                    message_text=data["message_text"],
                    file_size=data["file_size"],
                    is_duplicate=data["is_duplicate"]
                )
                break
            else:
                print("waiting for " + data["name"])
                if attempt == max_retry - 1:
                    raise FileNotFoundError("hook file not found")
                time.sleep(duration)

    if data["type"] == "video_hosted":
        await process_self_hosted_video_post(
            channel_id=data["target_id"],
            file_name=data["name"],
            runner_type=data["runner"],
            group_name=data["group_name"],
            message_id=data["message_id"],
            message_text=data["message_text"],
            file_size=data["file_size"],
            is_duplicate=data["is_duplicate"]
        )

    await cleanup(data['name'])


async def process_media_post(
        channel_id,
        file_name,
        file_ext,
        runner_type,
        group_name,
        message_id,
        message_text,
        file_size,
):
    message = f"""🐈 1 Message Forwarded
```
runner           : {runner_type}
group_name       : {group_name}
message_id       : {message_id}
message_text     : {message_text}
file_size        : {file_size} MB
```
"""

    # Get the channel to send the media to
    channel = client.get_channel(channel_id)

    # Create a new file object
    file = discord.File(fp=f"{download_dir}{file_name}{file_ext}",
                        filename=f"{file_name}{file_ext}")

    # Send the media to the channel
    await channel.send(message, file=file)

    print(f'Sent file: {message_id} -- {file_name}')


async def process_self_hosted_video_post(
        channel_id,
        file_name,
        runner_type,
        group_name,
        message_id,
        message_text,
        file_size,
        is_duplicate,
):
    thumbnail_link = ""
    if is_duplicate:
        thumbnail_link = f"(Duplicate)\n{base_url}/{file_name}.jpg\n"

    message = f"""🐈 1 Message Forwarded
```
runner_type      : {runner_type}
group_name       : {group_name}
message_id       : {message_id}
message_text     : {message_text}
file_size        : {file_size} MB
```
{thumbnail_link}{base_url}/{file_name}.mp4
"""

    # Get the channel to send the media to
    channel = client.get_channel(channel_id)

    # Create a new file object for thumbnail
    if not is_duplicate:
        file = discord.File(fp=f"{download_dir}{file_name}.jpg",
                            filename=f"{file_name}.jpg")

        # Send the media to the channel
        await channel.send(message, file=file)

    else:
        # Send the media to the channel
        await channel.send(message)

    print(f'Sent file: {message_id} -- {file_name}')


async def main():
    while True:
        msg = next(consumer)
        msg_body = msg.value

        try:
            deserialized_data = json.loads(msg_body.decode("utf-8"))
        except json.JSONDecodeError as e:
            print(f"Failed to deserialize message: {e}")
            continue

        if deserialized_data["type"] == event_type \
                and deserialized_data["data"]["source_id"] == source_id:
            try:
                await process_data(deserialized_data["data"])
            except Exception as e:
                print(e)
                consumer.commit()
                continue

        consumer.commit()


async def start_discord_client():
    await client.start(bot_token)


async def init():
    discord_init_event = asyncio.create_task(
        start_discord_client())
    await discord_init_event


asyncio.run(init())
