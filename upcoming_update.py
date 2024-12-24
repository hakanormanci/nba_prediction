from nba_api.stats.endpoints import scoreboardv2
import psycopg2
from psycopg2 import Error
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

def update_upcoming_games():
    connection = None
    cursor = None
    try:
        # Get upcoming games from NBA API
        games = scoreboardv2.ScoreboardV2()
        games_df = games.game_header.get_data_frame()

        # Database connection
        load_dotenv()
        connection = psycopg2.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            database="nba_stats"
        )
        cursor = connection.cursor()

        # Process each game
        for _, game in games_df.iterrows():
            insert_query = '''
            INSERT INTO upcoming_games 
            (game_id, game_date, game_time, home_team_id, away_team_id, 
             arena, game_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id) DO UPDATE 
            SET game_date = EXCLUDED.game_date,
                game_time = EXCLUDED.game_time,
                game_status = EXCLUDED.game_status;
            '''
            
            game_datetime = pd.to_datetime(game['GAME_DATE_EST'])
            
            cursor.execute(insert_query, (
                game['GAME_ID'],
                game_datetime.date(),
                game_datetime.time(),
                int(game['HOME_TEAM_ID']),
                int(game['VISITOR_TEAM_ID']),
                game['ARENA_NAME'],
                game['GAME_STATUS_TEXT']
            ))
            
            connection.commit()
            print(f"Game {game['GAME_ID']} updated successfully")

    except Exception as error:
        print(f"Error: {error}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    update_upcoming_games()