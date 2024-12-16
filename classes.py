import asyncio
import time
import httpx
import requests
import pytz
from datetime import datetime, timezone
import psycopg2
import psycopg2.extras
import asyncpg


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

        # Ensure the bookmaker is present
        self.bookmaker_id = None

        # Optionally load initial data (prematch)
        self.data = None
        self.pool=None

    @classmethod
    async def create(cls, sport, db_user, db_pass, db_name='odds_data_db', db_host='localhost', db_port=5432):
        """
        Async factory method to create and initialize the Pinnacle instance.
        """
        self = cls(sport)
        # Create asyncpg connection pool
        self.pool = await asyncpg.create_pool(
            user=db_user,
            password=db_pass,
            database=db_name,
            host=db_host,
            port=db_port
        )
        self.bookmaker_id = await self.get_bookmaker_id("pin", "http://www.pinnacle.com")
        # Optionally load initial data
        self.data = await self.get_all_events_data(live=False)
        return self

    async def get_bookmaker_id(self, name, website=None):
        now = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT bookmaker_id FROM bookmakers WHERE name=$1", name)
            if row:
                return row['bookmaker_id']
            row = await conn.fetchrow("""
                INSERT INTO bookmakers (name, website, created_at, updated_at)
                VALUES ($1, $2, $3, $4)
                RETURNING bookmaker_id
            """, name, website, now, now)
            return row['bookmaker_id']

    async def get_all_events_data(self, live=True):
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

        resp = requests.get(url, headers=headers, params=querystring)
        try:
            data = resp.json()
            processed = self.process_data(data, live)
            if processed:
                await self.update_database(processed)
            return processed
        except Exception as e:
            print(f'Error: {e}')

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
            await self.update_database(processed_data)
        return processed_data

    async def update_database(self, processed_data):
        if not processed_data:
            return

        # Extract all events and markets
        events_info = []  # (event_id, sport, league, home_team, away_team, start_time, is_timeout)
        markets = []
        odds_temp = []

        now = datetime.now(timezone.utc)
        sport = self.sport

        for event_key, event_data in processed_data.items():
            if 'info' not in event_data:
                continue

            league = event_data['info']['league']
            start_str = event_data['info']['start']
            external_event_id = event_data['info'].get('sql_key')
            away_team, home_team = event_key.split(" @ ")
            dt_utc = datetime.strptime(start_str, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
            is_timeout = event_data['info'].get('is_timeout', False)

            event_id = int(external_event_id)
            events_info.append((event_id, sport, league, home_team.strip(), away_team.strip(), dt_utc, is_timeout))

            for period_name, period_data in event_data.items():
                if period_name == 'info':
                    continue
                for market_type, market_details in period_data.items():
                    if market_type in ["Money Line", "3-way"]:
                        # single or three selection
                        keys = ['one', 'two', 'three'] if market_type == '3-way' else ['home', 'away']
                        for k in keys:
                            if k in market_details and market_details[k] is not None:
                                sel = f"{period_name}:{market_type}:{k}"
                                markets.append((event_id, self.bookmaker_id, market_type, sel))
                                max_limit = market_details.get('max')
                                odds_temp.append(((event_id, market_type, sel), str(market_details[k]), max_limit))
                    else:
                        # spread/total
                        for line_key, line_info in market_details.items():
                            one_odds = line_info['one']
                            two_odds = line_info['two']
                            alt = line_info['alt']
                            new_odds_str = f"{one_odds},{two_odds},{alt}"
                            sel = f"{period_name}:{market_type}:{line_key}"
                            markets.append((event_id, self.bookmaker_id, market_type, sel))
                            max_limit = line_info.get('max')
                            odds_temp.append(((event_id, market_type, sel), new_odds_str, max_limit))

        # Upsert events
        await self.upsert_events(events_info)

        # Upsert markets
        market_ids = await self.upsert_markets(markets)

        # Prepare final odds data
        final_odds_data = []
        for ((e_id, mtype, sel), odds_str, max_limit) in odds_temp:
            mid = market_ids.get((e_id, mtype, sel))
            if mid:
                final_odds_data.append((mid, odds_str, max_limit))

        await self.bulk_upsert_odds(final_odds_data)
        print("Database updated with current odds and limits.")

    async def upsert_events(self, events_info):
        if not events_info:
            return
        # Build a query string with placeholders
        # columns: (event_id, sport, league, home_team, away_team, start_time, created_at, updated_at, is_timeout)
        now = datetime.now(timezone.utc)
        values = []
        for e_id, s, l, h, a, st, timeout in events_info:
            values.append((e_id, s, l, h, a, st, now, now, timeout))

        # Dynamically build placeholders
        # We have 9 columns per row
        rows = []
        args = []
        arg_index = 1
        for row in values:
            placeholders = []
            for col in row:
                placeholders.append(f'${arg_index}')
                arg_index += 1
                args.append(col)
            rows.append(f"({','.join(placeholders)})")
        rows_str = ",".join(rows)

        query = f"""
            INSERT INTO events (event_id, sport, league, home_team, away_team, start_time, created_at, updated_at, is_timeout)
            VALUES {rows_str}
            ON CONFLICT (event_id) DO UPDATE SET 
                updated_at=EXCLUDED.updated_at,
                is_timeout=EXCLUDED.is_timeout
        """

        async with self.pool.acquire() as conn:
            await conn.execute(query, *args)

    async def upsert_markets(self, markets):
        if not markets:
            return {}
        # markets: (event_id, bookmaker_id, market_type, selection)
        # We'll return a dict {(event_id, market_type, selection): market_id}
        now = datetime.now(timezone.utc)
        values = []
        for e_id, b_id, m_t, sel in markets:
            values.append((e_id, b_id, m_t, sel, now, now))

        rows = []
        args = []
        arg_index = 1
        for row in values:
            placeholders = []
            for col in row:
                placeholders.append(f'${arg_index}')
                arg_index += 1
                args.append(col)
            rows.append(f"({','.join(placeholders)})")
        rows_str = ",".join(rows)

        query = f"""
        INSERT INTO markets (event_id, bookmaker_id, market_type, selection, created_at, updated_at)
        VALUES {rows_str}
        ON CONFLICT (event_id, bookmaker_id, market_type, selection) DO UPDATE
        SET updated_at=EXCLUDED.updated_at
        RETURNING market_id, event_id, market_type, selection;
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)

        market_ids = {(r['event_id'], r['market_type'], r['selection']): r['market_id'] for r in rows}
        return market_ids


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

    async def bulk_upsert_odds(self, odds_data):
        if not odds_data:
            return

        # odds_data: (market_id, new_odds_str, max_limit)
        market_ids = [x[0] for x in odds_data]

        # Fetch current odds
        async with self.pool.acquire() as conn:
            records = await conn.fetch("SELECT market_id, odds FROM current_odds WHERE market_id = ANY($1)", market_ids)
        current_map = {r['market_id']: r['odds'] for r in records}

        history_inserts = []
        current_inserts = []
        current_updates = []

        limit_history_inserts = []
        limit_inserts = []
        limit_updates = []

        now = datetime.now(timezone.utc)

        # Fetch current limits
        async with self.pool.acquire() as conn:
            limit_records = await conn.fetch("SELECT market_id, max_limit FROM current_limits WHERE market_id = ANY($1)", market_ids)
        current_limits_map = {r['market_id']: r['max_limit'] for r in limit_records}

        for (market_id, new_odds_str, max_limit) in odds_data:
            old_odds = current_map.get(market_id)
            if old_odds != new_odds_str:
                history_inserts.append((market_id, old_odds, new_odds_str, now))
                if old_odds is None:
                    current_inserts.append((market_id, new_odds_str, now))
                else:
                    current_updates.append((new_odds_str, now, market_id))

            old_limit = current_limits_map.get(market_id)
            if max_limit is not None and max_limit != old_limit:
                limit_history_inserts.append((market_id, old_limit, max_limit, now))
                if old_limit is None:
                    limit_inserts.append((market_id, max_limit, now))
                else:
                    limit_updates.append((max_limit, now, market_id))

        async with self.pool.acquire() as conn:
            # Insert odds_history
            if history_inserts:
                # (market_id, old_odds, new_odds, changed_at)
                await self._execute_many(conn,
                    "INSERT INTO odds_history (market_id, old_odds, new_odds, changed_at) VALUES ($1, $2, $3, $4)",
                    history_inserts
                )

            # current_odds
            if current_inserts:
                await self._execute_many(conn,
                    "INSERT INTO current_odds (market_id, odds, last_updated) VALUES ($1, $2, $3)",
                    current_inserts
                )
            if current_updates:
                await self._execute_many(conn,
                    "UPDATE current_odds SET odds=$1, last_updated=$2 WHERE market_id=$3",
                    current_updates
                )

            # limits
            if limit_history_inserts:
                await self._execute_many(conn,
                    "INSERT INTO limit_history (market_id, old_limit, new_limit, changed_at) VALUES ($1, $2, $3, $4)",
                    limit_history_inserts
                )

            if limit_inserts:
                await self._execute_many(conn,
                    "INSERT INTO current_limits (market_id, max_limit, last_updated) VALUES ($1, $2, $3)",
                    limit_inserts
                )
            if limit_updates:
                await self._execute_many(conn,
                    "UPDATE current_limits SET max_limit=$1, last_updated=$2 WHERE market_id=$3",
                    limit_updates
                )

    async def _execute_many(self, conn, query, args_list):
        # Helper method to run executemany-style commands in asyncpg
        # asyncpg doesn't have executemany for DML the same way psycopg2 does,
        # but we can just run them in a transaction.
        async with conn.transaction():
            for args in args_list:
                await conn.execute(query, *args)

    async def get_odds_history(self, event_id, market_type, selection):
        """
        Retrieve the full odds and limit history for the specified event/market/selection.
        Returns a list of dictionaries, each representing a historical change (either odds or limit),
        sorted by changed_at (descending).
        """
        event_id = int(event_id)

        async with self.pool.acquire() as conn:
            # First, get the market_id
            market_id = await conn.fetchval("""
                SELECT market_id FROM markets 
                WHERE event_id=$1 AND bookmaker_id=$2 AND market_type=$3 AND selection=$4
            """, event_id, self.bookmaker_id, market_type, selection)

            if market_id is None:
                # No such market found
                return []

            # Fetch odds history
            odds_rows = await conn.fetch("""
                SELECT history_id, market_id, old_odds, new_odds, changed_at
                FROM odds_history
                WHERE market_id=$1
                ORDER BY changed_at ASC
            """, market_id)

            # Fetch limit history
            limit_rows = await conn.fetch("""
                SELECT limit_history_id, market_id, old_limit, new_limit, changed_at
                FROM limit_history
                WHERE market_id=$1
                ORDER BY changed_at ASC
            """, market_id)

        changes = []

        # Process odds history
        for row in odds_rows:
            changes.append({
                'type': 'odds',
                'history_id': row['history_id'],
                'market_id': row['market_id'],
                'old_value': row['old_odds'],
                'new_value': row['new_odds'],
                'changed_at': row['changed_at']
            })

        # Process limit history
        for row in limit_rows:
            changes.append({
                'type': 'limit',
                'history_id': row['limit_history_id'],
                'market_id': row['market_id'],
                'old_value': str(row['old_limit']) if row['old_limit'] is not None else None,
                'new_value': str(row['new_limit']) if row['new_limit'] is not None else None,
                'changed_at': row['changed_at']
            })

        # Sort all changes by changed_at descending
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
    pinnacle = await Pinnacle.create('basketball', 'odds_user', 'odds_password')
    while True:
        data = await pinnacle.get_events_data(live=False)
        print(data)
        if data:
            for k, v in data.items():
                for period, data in v.items():
                    print(k, period, data)


if __name__ == '__main__':
    asyncio.run(main())