from nba_api.stats.endpoints import leaguegamefinder
import psycopg2
from psycopg2 import Error
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

def create_upcoming_games_table():
    connection = None
    cursor = None
    try:
        load_dotenv()
        
        connection = psycopg2.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            database="nba_stats"
        )
        cursor = connection.cursor()

        # Create upcoming games table
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS upcoming_games (
            id SERIAL PRIMARY KEY,
            game_id VARCHAR(20) UNIQUE,
            game_date DATE,
            game_time TIME,
            home_team_id INTEGER,
            away_team_id INTEGER,
            arena VARCHAR(100),
            tv_channel VARCHAR(50),
            game_status VARCHAR(20) DEFAULT 'Scheduled',
            FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
            FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
        );
        '''
        cursor.execute(create_table_query)
        connection.commit()
        print("upcoming_games table created successfully")

    except (Exception, Error) as error:
        print(f"Error: {error}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    create_upcoming_games_table()