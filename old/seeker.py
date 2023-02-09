import asyncio
import base64
import configparser
import io
import sys

import discord
from b2sdk.account_info import InMemoryAccountInfo
from b2sdk.api import B2Api
from telethon import TelegramClient

import backblaze_helper
import common_helper
import discord_helper
from common_helper import convert_to_mb

# Reading Configs
config = configparser.ConfigParser()
config.read("config.ini")

# API ID and hash obtained from https://my.telegram.org
api_id = int(config["Telegram"]["api_id"])
api_hash = config["Telegram"]["api_hash"]
session_name = config["Telegram"]["session_name"]

# Name of the Telegram group
group_name = config["Telegram"]["group_name"]

# Discord bot token
bot_token = config["Discord"]["bot_token"]
channel_id = int(config["Discord"]["channel_id"])

# Initialize the client
intents = discord.Intents.default()
intents.message_content = True
discord_client = discord.Client(intents=intents)
telegram_client = TelegramClient(session_name, api_id, api_hash)

# Seek past messages in the group
offset = 0
limit = 1000

# B2 setup
info = InMemoryAccountInfo()
b2_api = B2Api(info)
bucket_name = "000"
base_url = "https://999.backblazeb2.com/file/000"
application_key_id = '99950dbf514af2c0000000000'
application_key = '9994N5Ry35FjxG2PG3Bc08HmrXbl000'
b2_api.authorize_account("production", application_key_id, application_key)


# Login to Discord using the bot token
@discord_client.event
async def on_ready():
    print(f'Discord logged in as {discord_client.user}')

    await telegram_client.start()
    user = await telegram_client.get_me()
    print(f'Telegram logged in as {user.username}')

    # Get the group entity
    group_entity = None
    groups = await telegram_client.get_dialogs()
    for g in groups:
        if g.name == group_name:
            group_entity = g
            break

    messages = await telegram_client.get_messages(
        group_entity, offset_id=offset, limit=limit)

    # Print the messages
    for message in messages:
        if message.media and (message.photo or message.video) and not message.is_reply:
            # Determine the file extension based on the type of media
            if message.photo:
                file_extension = '.jpg'
            else:
                file_extension = '.mp4'

            # Construct the filename
            filename = "{}-{}".format(group_name, message.id)
            filename_base64 = "{}{}".format(base64.urlsafe_b64encode(filename.encode()).decode(),
                                            file_extension)

            # Download the file
            file = io.BytesIO()
            await telegram_client.download_media(message, file)

            file_size = sys.getsizeof(file)
            if file_size <= 49.5 * 1024 * 1024:
                await discord_helper.send_media_to_discord(
                    discord_client, file, filename_base64, channel_id,
                    group_name, message.id, "", "seeker", convert_to_mb(file_size))
            else:
                url, is_duplicate = backblaze_helper.upload_file(b2_api, bucket_name,
                                                                 filename_base64, base_url, file)

                screenshot = None
                if file_extension == ".mp4":
                    screenshot = common_helper.get_screenshot(file)

                await discord_helper.send_ext_media_to_discord(
                    discord_client,
                    url,
                    is_duplicate,
                    channel_id,
                    group_name,
                    message.id,
                    "",
                    "seeker",
                    convert_to_mb(file_size),
                    screenshot
                )


async def main():
    discord_init_event = asyncio.create_task(
        discord_helper.start_discord_client(discord_client, bot_token))
    await discord_init_event


asyncio.run(main())
