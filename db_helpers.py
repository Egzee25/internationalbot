import psycopg2
from datetime import datetime, timezone

def get_connection():
    # Update these connection parameters with your actual database credentials
    return psycopg2.connect(
        dbname="odds_data_db",
        user="odds_user",
        password="odds_password",
        host="localhost",
        port=5432
    )

def get_bookmaker_id(conn, name, website=None):
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    with conn.cursor() as cur:
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
    conn.commit()
    return bookmaker_id

def get_event_id(conn, sport, league, home_team, away_team, start_time, external_event_id):
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    with conn.cursor() as cur:
        if external_event_id:
            cur.execute("SELECT event_id FROM events WHERE external_event_id=%s", (external_event_id,))
            row = cur.fetchone()
            if row:
                return row[0]

        cur.execute("""
            SELECT event_id FROM events
            WHERE sport=%s AND league=%s AND home_team=%s AND away_team=%s AND start_time=%s
        """, (sport, league, home_team, away_team, start_time))
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute("""
            INSERT INTO events (sport, league, home_team, away_team, start_time, external_event_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING event_id
        """, (sport, league, home_team, away_team, start_time, external_event_id, now, now))
        event_id = cur.fetchone()[0]
    conn.commit()
    return event_id

def get_market_id(conn, event_id, bookmaker_id, market_type, selection):
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT market_id FROM markets WHERE event_id=%s AND bookmaker_id=%s AND market_type=%s AND selection=%s
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
    conn.commit()
    return market_id

def update_odds(conn, market_id, new_odds):
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    with conn.cursor() as cur:
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

    conn.commit()



