import discord


# Start the Discord client using asyncio
async def start_discord_client(client, bot_token):
    await client.start(bot_token)


# Send the media stored in the buffer to a Discord channel
async def send_media_to_discord(discord_client, buffer, filename,
                                channel_id, group_name, message_id, message, runner_type, file_size):
    buffer.seek(0)

    message = """🐈 1 Message Forwarded
```
runner_type      : {}
group_name       : {}
message_id       : {}
caption          : {}
file_size        : {} MB
```
""".format(runner_type, group_name, message_id, message, file_size)

    # Get the channel to send the media to
    channel = discord_client.get_channel(channel_id)

    # Create a new file from the buffer
    file = discord.File(buffer, filename=filename)

    # Send the media to the channel
    await channel.send(message, file=file)

    print(f'Sent file: {message_id} -- {filename}')


# Send the media stored in the buffer to a Discord channel
async def send_ext_media_to_discord(discord_client, url, is_duplicate, channel_id, group_name, message_id, message,
                                    runner_type, file_size, screenshot):
    if is_duplicate:
        url = "(dupl) " + url

    message = """🐈 1 Message Forwarded
```
runner_type      : {}
group_name       : {}
message_id       : {}
caption          : {}
file_size        : {} MB
```
{}
""".format(runner_type, group_name, message_id, message, file_size, url)

    # Get the channel to send the media to
    channel = discord_client.get_channel(channel_id)

    if screenshot:
        screenshot.seek(0)

        # Create a new file from the buffer
        file = discord.File(screenshot, filename="preview.jpg")

        # Send the media to the channel
        await channel.send(message, file=file)
    else:
        await channel.send(message)

    print(f'Sent file: {message_id} -- {url}')
