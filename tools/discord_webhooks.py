import hashlib
import time

import httpx

timeout_url = 'https://discordapp.com/api/webhooks/1203781932815618108/7FbLFBN4ym0_sqjTJ_zMGteBh6f4x03jXukJvwbCB2YfxGChAHWhZ6kG-g4KvamSt7vP'
czr_url = 'https://discordapp.com/api/webhooks/1242189166369046678/fCP0KTt4VQ0vVR4UeEZoTTOnuzqgmeV8dNWAf2iRjmAi9zX2ZjseOCmFv53jYE_XGKc-'
dk_url = 'https://discordapp.com/api/webhooks/1242189078741647593/gav43cyJz7fQcSUffh5buPVrhH9eAiPxoueGFtLF_bxKwTDyCd9YvJNiYJNATETs9iHv'
fd_url = 'https://discordapp.com/api/webhooks/1242185001030647830/ggDmhKDZkshxiWZ4tYoCWjW6Q3m716BjnxJzL4krppuwCFRRYclOC3DCp9M7WZ8UJWS5'
fliff_url = 'https://discordapp.com/api/webhooks/1242185996997496935/qYs5zdMjW6lvzu9LtQdsf4EnR5kAGxsHbBwEPTLxexVI1YnJCRaRvmNYVnAkGv0rBrdY'
golf_url = 'https://discordapp.com/api/webhooks/1247968671176069120/R80nmlj1dRoklx6-aJ0H-NJukvv-xnrWAK2vqMHnJnRcPIq0nzyP-cV8Yos6qa4B7ag4'
test_url = 'https://discordapp.com/api/webhooks/1242191264502517870/q3zp3NvnBdOuM3NDqDAl5-mMu13bJpYlGSHVu7_EFJCGH5roOY9PI6w_k2SPhVqq1MNl'
egzee_url = 'https://discord.com/api/webhooks/1276003252109840395/TD4XdGIqGT7P4guZnttdo5cLZmk_03k3eR6SzwihXePIFMxutZdXL5os6Q5enREoBwp6'
mcdanglez_url = 'https://discord.com/api/webhooks/1276005856684539924/kBRTQqxSPmVlGjBochPayCsB1NrS7PHyfTYWodlq2KkZXYVHWXFwbakEwMEjRn2XXgBS'
webhook_mapping = {
    'timeout': timeout_url,
    'czr': czr_url,
    'dk': dk_url,
    'fd': fd_url,
    'fliff': fliff_url,
    'golf': golf_url,
    'test': test_url,
    'egzee': egzee_url,
    'mcdanglez': mcdanglez_url
}

client = httpx.AsyncClient()

# Dictionary to track the last sent time and hash of the message for each channel
last_sent_data = {}


def get_message_hash(message):
    """Generate a hash for the message content."""
    return hashlib.sha256(str(message).encode('utf-8')).hexdigest()


async def send_webhook(message, channel, type='embeds'):
    current_time = time.time()

    if type == 'embeds':
        message = message.to_dict()
        message = [message]


    url = webhook_mapping[channel]
    response = await client.post(url, json={type: message})

    if response.status_code == 204:
        # Update the last sent time and message hash if the message was sent successfully
        last_sent_data[channel] = {'time': current_time, 'message': message}
    else:
        print(response.text)
