import requests
import matplotlib



import numpy as np
from tools.devig import dec_to_amer, calculate_decimal_odds
import io
import httpx
import time
import discord
import json

session = httpx.AsyncClient()
matplotlib.use('Agg')

from matplotlib.ticker import FuncFormatter
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def imp_to_american(imp):
    dec = 1 / -imp
    return dec_to_amer(dec)


def american_to_imp(american):
    return -1 / calculate_decimal_odds(american)


def graph(history, graph_title, side):
    fig, ax = plt.subplots()

    odds_times = []
    limit_times = []
    odds = []
    limits = []

    for change in history:
        if change['type'] == 'odds':
            odds_times.append(change['changed_at'])
            odds_values = change['new_value'].split(',')
            if side == 'one':
                change_odds = float(odds_values[0])
            else:
                change_odds = float(odds_values[1])
            # Convert to implied probability and then to internal representation
            imp = 1 / calculate_decimal_odds(change_odds)
            odds.append(-imp)
        elif change['type'] == 'limit':
            limit_times.append(change['changed_at'])
            limit = float(change['new_value'])
            limits.append(limit)

    if len(odds_times) < 2:
        return None

    max_time = max(odds_times + limit_times)
    if max_time not in limit_times and len(limits) > 0:
        limit_times.append(max_time)
        limits.append(limits[-1])

    if max_time not in odds_times and len(odds) > 0:
        odds_times.append(max_time)
        odds.append(odds[-1])

    # Convert datetime objects to Matplotlib’s numeric date format
    odds_times = mdates.date2num(odds_times)
    limit_times = mdates.date2num(limit_times)

    # Set the Y-axis formatting to American odds using FuncFormatter
    ax.yaxis.set_major_formatter(FuncFormatter(lambda val, pos: imp_to_american(val)))

    # Plot odds
    ax.plot(odds_times, odds, color='blue', marker='o', label='Odds')
    ax.set_ylabel('Odds (in American)')

    # Plot limits on a second Y-axis if present
    if len(limits) > 0:
        ax2 = ax.twinx()
        ax2.plot(limit_times, limits, color='red', label='Limits')
        ax2.set_ylabel('Limits')
        ax2.set_ylim(min(limits) * 0.9, max(limits) * 1.1)

        lines1, labels1 = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    else:
        ax.legend(loc='upper left')

    ax.set_title(graph_title)

    # Set up the x-axis for dates
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))

    # Automatically rotate and format x-axis labels
    fig.autofmt_xdate()

    # Save the figure to a buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=300)
    plt.close(fig)
    buf.seek(0)

    return buf.getvalue()


def send_graph(history, embed_text, graph_title, embed_subtext, game, side):
    webhook_url = 'https://discordapp.com/api/webhooks/1242191264502517870/q3zp3NvnBdOuM3NDqDAl5-mMu13bJpYlGSHVu7_EFJCGH5roOY9PI6w_k2SPhVqq1MNl'
    t = time.time()
    image = graph(history, graph_title, side)
    print(f"Time taken to generate graph: {time.time() - t:.2f}s")
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
    t = time.time()
    response = requests.post(webhook_url, data=data, files=files)
    print(f"Time taken to send message: {time.time() - t:.2f}s")
    if response.status_code == 200:
        print("Message sent successfully.")
    else:
        print(f"Failed to send message. Status: {response.status_code}, Response: {response.text}")