import psycopg2
from psycopg2 import sql

def setup_db():
    conn = psycopg2.connect(
        dbname="odds_data_db",
        user="odds_user",
        password="odds_password",
        host="localhost",
        port=5432
    )
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS bookmakers (
        bookmaker_id SERIAL PRIMARY KEY,
        name TEXT UNIQUE,
        website TEXT,
        created_at TIMESTAMP WITH TIME ZONE,
        updated_at TIMESTAMP WITH TIME ZONE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_id SERIAL PRIMARY KEY,
        sport TEXT,
        league TEXT,
        home_team TEXT,
        away_team TEXT,
        start_time TIMESTAMP WITH TIME ZONE,
        external_event_id TEXT UNIQUE,
        is_timeout BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP WITH TIME ZONE,
        updated_at TIMESTAMP WITH TIME ZONE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS markets (
        market_id SERIAL PRIMARY KEY,
        event_id INTEGER REFERENCES events(event_id),
        bookmaker_id INTEGER REFERENCES bookmakers(bookmaker_id),
        market_type TEXT,
        selection TEXT,
        created_at TIMESTAMP WITH TIME ZONE,
        updated_at TIMESTAMP WITH TIME ZONE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS current_odds (
        market_id INTEGER PRIMARY KEY REFERENCES markets(market_id),
        odds TEXT,
        last_updated TIMESTAMP WITH TIME ZONE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS odds_history (
        history_id SERIAL PRIMARY KEY,
        market_id INTEGER REFERENCES markets(market_id),
        old_odds TEXT,
        new_odds TEXT,
        changed_at TIMESTAMP WITH TIME ZONE
    )
    """)

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    setup_db()
    print("PostgreSQL database and tables created successfully.")
