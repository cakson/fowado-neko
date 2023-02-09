import configparser
import io
import discord
from telethon import TelegramClient, events
import asyncio

from old.discord_helper import send_media_to_discord, start_discord_client

# Reading Configs
config = configparser.ConfigParser()
config.read("config.ini.temp")

# API ID and hash obtained from https://my.telegram.org
api_id = int(config["Telegram"]["api_id"])
api_hash = config["Telegram"]["api_hash"]

# Name of the Telegram group
group_name = config["Telegram"]["group_name"]

# Discord bot token
bot_token = config["Discord"]["bot_token"]
channel_id = int(config["Discord"]["channel_id"])

# Initialize the client
intents = discord.Intents.default()
intents.message_content = True
discord_client = discord.Client(intents=intents)
telegram_client = TelegramClient('listener', api_id, api_hash)


# Login to Discord using the bot token
@discord_client.event
async def on_ready():
    print(f'Logged in as {discord_client.user}')

    await telegram_client.start()

    # Get the group entity
    group_entity = None
    groups = await telegram_client.get_dialogs()
    for g in groups:
        if g.name == group_name:
            group_entity = g
            break

    # Listen for new messages in the group
    @telegram_client.on(events.NewMessage(chats=group_entity))
    async def handler(event):
        if event.message.media and (event.message.photo or event.message.video):
            file_date = event.message.date
            date_string = file_date.strftime("%Y-%m-%d_%H-%M-%S")

            # Determine the file extension based on the type of media
            if event.message.photo:
                file_extension = '.jpg'
            else:
                file_extension = '.mp4'

            # Construct the filename
            filename = date_string + file_extension

            # Download the file
            file = io.BytesIO()
            await telegram_client.download_media(event.message, file)

            await send_media_to_discord(discord_client, file, filename, channel_id,
                                        group_name, event.message.id, "", "listener")

    if group_entity is None:
        print(f'Group "{group_name}" not found.')
        exit(1)

    await telegram_client.run_until_disconnected()


async def main():
    discord_init_event = asyncio.create_task(
        start_discord_client(discord_client, bot_token))

    await discord_init_event


asyncio.run(main())
