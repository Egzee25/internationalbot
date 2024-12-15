import requests
from matplotlib.ticker import FuncFormatter
import matplotlib.pyplot as plt
import numpy as np
from tools.devig import dec_to_amer, calculate_decimal_odds
import io
import httpx
import discord
import json

session = httpx.AsyncClient()


def imp_to_american(imp):
    dec = 1 / -imp
    return dec_to_amer(dec)


def american_to_imp(american):
    return -1 / calculate_decimal_odds(american)


def graph(history, graph_title):
    fig, ax = plt.subplots()

    odds_times = []
    limit_times = []
    odds = []
    limits = []

    for change in history:
        if change['type'] == 'odds':
            odds_times.append(change['changed_at'])
            change_odds = float(change['new_value'].split(',')[0])
            # Convert odds to implied probability first:
            imp = 1 / calculate_decimal_odds(change_odds)
            odds.append(-imp)
        elif change['type'] == 'limit':
            limit_times.append(change['changed_at'])
            limit = float(change['new_value'])
            limits.append(limit)

    if len(odds_times) < 2:
        # Not enough data to plot a meaningful line
        return None

    max_time = max(odds_times + limit_times)

    if max_time not in limit_times:
        limit_times.append(max_time)
        limits.append(limits[-1])

    if max_time not in odds_times:
        odds_times.append(max_time)
        odds.append(odds[-1])

    # Format the primary y-axis for odds as American odds
    ax.yaxis.set_major_formatter(FuncFormatter(lambda val, pos: imp_to_american(val)))
    ax.plot(odds_times, odds, color='blue', label='Odds', marker='o')
    ax.set_ylabel('Odds (in American)')

    # Create a second y-axis sharing the x-axis
    if len(limits) > 0:
        ax2 = ax.twinx()
        ax2.plot(limit_times, limits, color='red', label='Limits')
        ax2.set_ylabel('Limits')  # This axis will show the raw limit values without American odds formatting
        ax2.set_ylim(min(limits) * 0.9, max(limits) * 1.1)

        # Combine legends
        lines1, labels1 = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    else:
        ax.legend(loc='upper left')

    ax.set_title(graph_title)

    # Save the figure to a BytesIO buffer as a PNG
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=300)
    plt.close(fig)
    buf.seek(0)

    # Return the PNG image as bytes
    return buf.getvalue()


def send_graph(history, embed_text, graph_title, embed_subtext, game):
    webhook_url = 'https://discordapp.com/api/webhooks/1242191264502517870/q3zp3NvnBdOuM3NDqDAl5-mMu13bJpYlGSHVu7_EFJCGH5roOY9PI6w_k2SPhVqq1MNl'
    image = graph(history, graph_title)
    if image is None:
        print("No graph generated (not enough data).")
        return

    # Create the embed object as a dictionary
    embed = {
        "title": embed_text,
        "color": 0x00FF00,
        'fields': [
            {
                'name': game,
                'value': embed_subtext
            }
        ],
        # The image will reference the attachment we’re sending
        "image": {
            "url": "attachment://graph.png"
        }
    }

    # Prepare the payload with the embed
    payload = {
        "embeds": [embed]
    }

    # Post request with multipart form-data
    files = {
        "files[0]": ("graph.png", io.BytesIO(image), "image/png")
    }
    data = {
        "payload_json": json.dumps(payload)
    }

    response = requests.post(webhook_url, data=data, files=files)
    if response.status_code == 200:
        print("Message sent successfully.")
    else:
        print(f"Failed to send message. Status: {response.status_code}, Response: {response.text}")



