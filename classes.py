import asyncio
import time
import httpx
import requests
import pytz
from datetime import datetime, timezone
import psycopg2

from tools.devig import dec_to_amer, calculate_vig

key = "47459b6cc5msh408997758417086p1a4c4bjsna2d524d30b8d"

class Pinnacle:
    def __init__(self, sport):
        self.name = "pin"
        self.ids = {'soccer': 1, 'tennis': 2, 'hockey': 4, 'football': 7, 'baseball': 9, 'basketball': 3}
        self.id = self.ids.get(sport)
        self.client = httpx.AsyncClient(timeout=10)
        self.sport = sport
        self.limits = {}
        self.timeouts = {}

        # Connect to PostgreSQL
        self.conn = psycopg2.connect(
            dbname="odds_data_db",     # Update these credentials as needed
            user="odds_user",          # Update user
            password="odds_password",  # Update password
            host="localhost",          # Update host if needed
            port=5432                  # Update port if needed
        )

        # Ensure the bookmaker is present
        self.bookmaker_id = self.get_bookmaker_id("pin", "http://www.pinnacle.com")

        # Optionally load initial data (prematch)
        self.data = self.get_all_events_data(live=False)

    def get_bookmaker_id(self, name, website=None):
        now = datetime.now(timezone.utc)
        with self.conn.cursor() as cur:
            cur.execute("SELECT bookmaker_id FROM bookmakers WHERE name=%s", (name,))
            row = cur.fetchone()
            if row:
                return row[0]

            cur.execute("""
                INSERT INTO bookmakers (name, website, created_at, updated_at)
                VALUES (%s, %s, %s, %s)
                RETURNING bookmaker_id
            """, (name, website, now, now))
            bookmaker_id = cur.fetchone()[0]
        self.conn.commit()
        return bookmaker_id

    def get_event_id(self, sport, league, home_team, away_team, start_time, event_id):
        event_id = int(event_id)
        now = datetime.now(timezone.utc)
        with self.conn.cursor() as cur:
            # Check if the event already exists
            cur.execute("SELECT event_id FROM events WHERE event_id=%s", (event_id,))
            row = cur.fetchone()
            if row:
                # The event is already in the DB
                return event_id

            # If not, insert it
            cur.execute("""
                INSERT INTO events (event_id, sport, league, home_team, away_team, start_time, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING event_id
            """, (event_id, sport, league, home_team, away_team, start_time, now, now))
            inserted_id = cur.fetchone()[0]
        self.conn.commit()
        return inserted_id

    def get_market_id(self, event_id, bookmaker_id, market_type, selection):
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT market_id FROM markets 
                WHERE event_id=%s AND bookmaker_id=%s AND market_type=%s AND selection=%s
            """, (event_id, bookmaker_id, market_type, selection))
            row = cur.fetchone()
            if row:
                return row[0]

            cur.execute("""
                INSERT INTO markets (event_id, bookmaker_id, market_type, selection, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING market_id
            """, (event_id, bookmaker_id, market_type, selection, now, now))
            market_id = cur.fetchone()[0]
        self.conn.commit()
        return market_id

    def update_odds(self, market_id, new_odds):
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        with self.conn.cursor() as cur:
            cur.execute("SELECT odds FROM current_odds WHERE market_id=%s", (market_id,))
            row = cur.fetchone()
            old_odds = row[0] if row else None

            if old_odds != new_odds:
                # Insert into odds_history
                cur.execute("""
                    INSERT INTO odds_history (market_id, old_odds, new_odds, changed_at)
                    VALUES (%s, %s, %s, %s)
                """, (market_id, old_odds, new_odds, now))

                if old_odds is None:
                    # Insert current odds
                    cur.execute("""
                        INSERT INTO current_odds (market_id, odds, last_updated)
                        VALUES (%s, %s, %s)
                    """, (market_id, new_odds, now))
                else:
                    # Update current odds
                    cur.execute("""
                        UPDATE current_odds SET odds=%s, last_updated=%s WHERE market_id=%s
                    """, (new_odds, now, market_id))
        self.conn.commit()

    def update_limit(self, market_id, new_limit):
        # Similar logic as update_odds, but for limits
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        with self.conn.cursor() as cur:
            cur.execute("SELECT max_limit FROM current_limits WHERE market_id=%s", (market_id,))
            row = cur.fetchone()
            old_limit = row[0] if row else None

            if old_limit != new_limit:
                # Insert into limit_history
                cur.execute("""
                    INSERT INTO limit_history (market_id, old_limit, new_limit, changed_at)
                    VALUES (%s, %s, %s, %s)
                """, (market_id, old_limit, new_limit, now))

                if old_limit is None:
                    # Insert current limit
                    cur.execute("""
                        INSERT INTO current_limits (market_id, max_limit, last_updated)
                        VALUES (%s, %s, %s)
                    """, (market_id, new_limit, now))
                else:
                    # Update current limit
                    cur.execute("""
                        UPDATE current_limits SET max_limit=%s, last_updated=%s WHERE market_id=%s
                    """, (new_limit, now, market_id))
        self.conn.commit()

    async def get_events_data(self, live=True):
        url = "https://pinnacle-odds.p.rapidapi.com/kit/v1/markets"
        current_unix_time = time.time()
        querystring = {
            "sport_id": self.id,
            "is_have_odds": "true",
            "since": str(int(current_unix_time) - 3000),
            'event_type': 'live' if live else 'prematch'
        }
        headers = {
            "X-RapidAPI-Key": key,
            "X-RapidAPI-Host": "pinnacle-odds.p.rapidapi.com"
        }

        response = await self.client.get(url, headers=headers, params=querystring)
        data = response.json()
        processed_data = self.process_data(data, live)
        if processed_data:
            self.update_database(processed_data)
        return processed_data

    def get_all_events_data(self, live=True):
        url = "https://pinnacle-odds.p.rapidapi.com/kit/v1/markets"
        querystring = {
            "sport_id": self.id,
            "is_have_odds": "true",
            'event_type': 'live' if live else 'prematch'
        }
        headers = {
            "X-RapidAPI-Key": key,
            "X-RapidAPI-Host": "pinnacle-odds.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers, params=querystring)
        try:
            data = response.json()
            processed = self.process_data(data, live)
            if processed:
                self.update_database(processed)
            return processed
        except Exception as e:
            print(f'error {e}')

    def get_game_state(self, data):
        return data.get('period_results')

    def process_data(self, data, live):
        events = data.get('events')
        if not events:
            return
        processed_data = {}

        utc_zone = pytz.utc
        est_zone = pytz.timezone('US/Eastern')

        for event in events:
            event_data = {}
            league = event.get('league_name')
            if 'NBA' in league or 'NCAA' in league:
                continue

            home = event.get('home')
            away = event.get('away')
            datetime_str = event.get('starts')
            dt_utc = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S')
            dt_utc = utc_zone.localize(dt_utc)
            dt_est = dt_utc.astimezone(est_zone)
            formatted_date = dt_est.strftime("%b %d")

            if '(Hits+Runs+Errors)' in home:
                continue

            external_event_id = str(event.get('event_id'))

            event_data['info'] = {
                'league': league,
                'start': datetime_str,
                'sql_key': external_event_id,
                'date': formatted_date,
                'is_timeout': False,
            }

            # Determine periods based on sport:
            if self.sport == 'baseball':
                fg = [event.get('periods', {}).get('num_0', {}), 'full']
                h1 = [event.get('periods', {}).get('num_1', {}), 'half']
                periods = [fg, h1]
            elif self.sport == 'tennis':
                fg = [event.get('periods', {}).get('num_0', {}), 'full']
                s1 = [event.get('periods', {}).get('num_1', {}), 'set 1']
                s2 = [event.get('periods', {}).get('num_2', {}), 'set 2']
                s3 = [event.get('periods', {}).get('num_3', {}), 'set 3']
                s4 = [event.get('periods', {}).get('num_4', {}), 'set 4']
                s5 = [event.get('periods', {}).get('num_5', {}), 'set 5']
                periods = [fg, s1, s2, s3, s4, s5]
            elif self.sport == 'basketball':
                fg = [event.get('periods', {}).get('num_0', {}), 'full']
                periods = [fg]
            elif self.sport == 'soccer':
                fg = [event.get('periods', {}).get('num_0', {}), 'full']
                h1 = [event.get('periods', {}).get('num_1', {}), 'half']
                ot = [event.get('periods', {}).get('num_3', {}), 'ot']
                qual = [event.get('periods', {}).get('num_8', {}), 'qual']
                periods = [fg, h1, ot, qual]
            elif self.sport == 'football':
                fg = [event.get('periods', {}).get('num_0', {}), 'full']
                h1 = [event.get('periods', {}).get('num_1', {}), 'half']
                h2 = [event.get('periods', {}).get('num_2', {}), '2h']
                q1 = [event.get('periods', {}).get('num_3', {}), '1q']
                q2 = [event.get('periods', {}).get('num_4', {}), '2q']
                q3 = [event.get('periods', {}).get('num_5', {}), '3q']
                q4 = [event.get('periods', {}).get('num_6', {}), '4q']
                periods = [fg, h1, h2, q1, q2, q3, q4]
            elif self.sport == 'hockey':
                fg = [event.get('periods', {}).get('num_0', {}), 'full']
                p1 = [event.get('periods', {}).get('num_1', {}), '1p']
                p2 = [event.get('periods', {}).get('num_2', {}), '2p']
                p3 = [event.get('periods', {}).get('num_3', {}), '3p']
                reg = [event.get('periods', {}).get('num_6', {}), 'reg']
                periods = [fg, p1, p2, p3, reg]
            else:
                continue

            id = f'{away} @ {home}'
            for p in periods:
                period = p[-1]
                p = p[0]
                if not p or p.get('period_status') == 2:
                    continue
                cutoff = p.get('cutoff')
                cutoff_time = datetime.fromisoformat(cutoff).replace(tzinfo=timezone.utc)
                current_time = datetime.now(tz=timezone.utc)
                if current_time > cutoff_time:
                    continue

                money_line = p.get('money_line', {}) or {}

                if self.sport != 'soccer' and (self.sport != 'hockey' or period != 'reg'):
                    event_data[period] = {
                        'Money Line': {
                            'home': dec_to_amer(money_line.get('home')),
                            'away': dec_to_amer(money_line.get('away')),
                            'max': p.get('meta', {}).get('max_money_line')
                        },
                        'spread': {},
                        'total': {}
                    }
                else:
                    event_data[period] = {
                        '3-way': {
                            'one': dec_to_amer(money_line.get('home')),
                            'two': dec_to_amer(money_line.get('away')),
                            'three': dec_to_amer(money_line.get('draw')),
                            'max': p.get('meta', {}).get('max_money_line')
                        },
                        'spread': {},
                        'total': {}
                    }

                if p.get('spreads') is not None:
                    for spread in p.get('spreads', {}).values():
                        if spread.get('hdp') == 0:
                            event_data[period]['Money Line'] = {
                                'home': dec_to_amer(spread.get('home')),
                                'away': dec_to_amer(spread.get('away')),
                                'max': spread.get('max')
                            }
                            continue
                        alt = bool(spread.get('alt_line_id'))
                        # Check if low vig triggers timeout
                        if not alt and period == 'full':
                            vig = calculate_vig([dec_to_amer(spread.get('home')), dec_to_amer(spread.get('away'))])
                            if vig < 1.053:
                                event_data['info']['is_timeout'] = True

                        event_data[period]['spread'][spread.get('hdp')] = {
                            'one': dec_to_amer(spread.get('home')),
                            'two': dec_to_amer(spread.get('away')),
                            'alt': alt,
                            'max': spread.get('max')
                        }

                if p.get('totals') is not None:
                    for total in p.get('totals', {}).values():
                        event_data[period]['total'][total.get('points')] = {
                            'one': dec_to_amer(total.get('over')),
                            'two': dec_to_amer(total.get('under')),
                            'alt': bool(total.get('alt_line_id')),
                            'max': total.get('max')
                        }

            # Check if event_data contains meaningful odds
            # (This condition is just ensuring we actually got some odds data)
            if any(key in event_data for key in ['full', 'half', 'ot', '2h']):
                processed_data[id] = event_data

        return processed_data

    def update_database(self, processed_data):
        if not processed_data:
            return

        sport = self.sport
        for event_key, event_data in processed_data.items():
            if 'info' not in event_data:
                continue

            league = event_data['info']['league']
            start_str = event_data['info']['start']
            external_event_id = event_data['info'].get('sql_key', None)
            away_team, home_team = event_key.split(" @ ")

            dt_utc = datetime.strptime(start_str, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
            event_id = self.get_event_id(sport, league, home_team, away_team, dt_utc, external_event_id)

            is_timeout = event_data['info'].get('is_timeout', False)
            with self.conn.cursor() as cur:
                cur.execute("UPDATE events SET is_timeout=%s WHERE event_id=%s", (is_timeout, event_id))
            self.conn.commit()

            # Update odds and limits for each market
            for period_name, period_data in event_data.items():
                if period_name == 'info':
                    continue

                for market_type, market_details in period_data.items():
                    if market_type in ["Money Line", "3-way"]:
                        # market_details might have keys like 'home', 'away', 'draw', 'max'
                        for selection_key, odds_value in market_details.items():
                            if selection_key in ['home', 'away', 'draw', 'one', 'two', 'three']:
                                new_odds_str = str(odds_value)
                                selection = f"{period_name}:{market_type}:{selection_key}"
                                market_id = self.get_market_id(event_id, self.bookmaker_id, market_type, selection)
                                self.update_odds(market_id, new_odds_str)
                                # Update limit if present
                                max_limit = market_details.get('max')
                                if max_limit is not None:
                                    self.update_limit(market_id, max_limit)
                    else:
                        # spread/total markets
                        for line_key, line_info in market_details.items():
                            # line_info is a dict with 'one', 'two', 'alt', 'max'
                            new_odds_str = f"{line_info['one']},{line_info['two']},{line_info['alt']}"
                            selection = f"{period_name}:{market_type}:{line_key}"
                            market_id = self.get_market_id(event_id, self.bookmaker_id, market_type, selection)
                            self.update_odds(market_id, new_odds_str)
                            # Update limit if present
                            max_limit = line_info.get('max')
                            if max_limit is not None:
                                self.update_limit(market_id, max_limit)

        print("Database updated with current odds and limits.")

    def get_odds_history(self, event_id, market_type, selection):
        """
        Retrieve the full odds and limit history for the specified event/market/selection.

        Returns a list of dictionaries, each representing a historical change (either odds or limit),
        sorted by changed_at. Each dictionary includes a 'type' field indicating whether it's an 'odds' or 'limit' change.
        """

        market_id = self.get_market_id(event_id, self.bookmaker_id, market_type, selection)

        changes = []

        with self.conn.cursor() as cur:
            # Fetch odds history
            cur.execute("""
                SELECT history_id, market_id, old_odds, new_odds, changed_at
                FROM odds_history
                WHERE market_id = %s
                ORDER BY changed_at ASC
            """, (market_id,))
            odds_rows = cur.fetchall()

            for row in odds_rows:
                changes.append({
                    'type': 'odds',
                    'history_id': row[0],
                    'market_id': row[1],
                    'old_value': row[2],
                    'new_value': row[3],
                    'changed_at': row[4]
                })

            # Fetch limit history
            cur.execute("""
                SELECT limit_history_id, market_id, old_limit, new_limit, changed_at
                FROM limit_history
                WHERE market_id = %s
                ORDER BY changed_at ASC
            """, (market_id,))
            limit_rows = cur.fetchall()

            for row in limit_rows:
                changes.append({
                    'type': 'limit',
                    'history_id': row[0],
                    'market_id': row[1],
                    'old_value': str(row[2]) if row[2] is not None else None,
                    'new_value': str(row[3]) if row[3] is not None else None,
                    'changed_at': row[4]
                })

        # Combine and sort all changes by changed_at
        changes.sort(key=lambda x: x['changed_at'], reverse=True)

        return changes


class Betonline:
    def __init__(self, sport):
        self.name = 'bol'
        self.sport = sport
        self.url = 'https://betonline-58db51404c56.herokuapp.com/data'
        self.client = httpx.AsyncClient(timeout=10)
        self.cache = {}
        self.cache_time = None

    async def get_events_data(self, live):
        current_time = time.time()
        if self.cache_time is not None and current_time - self.cache_time < 60:
            print('returning cache')
            return self.cache
        res = await self.client.get(self.url)
        try:
            res.raise_for_status()
        except Exception as e:
            print(e)
            return {}
        data = res.json()
        self.cache = data
        self.cache_time = current_time
        return data

class Fanduel:
    def __init__(self, sport):
        self.name = 'fd'
        self.sport = sport
        self.client = httpx.AsyncClient()
        self.base_url = 'vm.egzee.com/fd'

    async def get_events_data(self, live=True):
        url = f'https://{self.base_url}/{self.sport}/{live}'
        res = await self.client.get(url)
        try:
            res.raise_for_status()
        except Exception as e:
            print(e)
            return {}
        return res.json()


async def main():
    pinnacle = Pinnacle('basketball')
    data = await pinnacle.get_events_data(live=False)
    print(data)
    if data:
        for k, v in data.items():
            for period, data in v.items():
                print(k, period, data)


if __name__ == '__main__':
    asyncio.run(main())