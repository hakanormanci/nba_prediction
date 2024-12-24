from nba_api.stats.endpoints import leaguegamefinder, boxscoresummaryv2
import psycopg2
from psycopg2 import Error
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

def get_game_details(game_df, valid_team_ids):
    """Helper function to process game data with team ID validation"""
    game_groups = game_df.groupby('GAME_ID').agg({
        'GAME_DATE': 'first',
        'TEAM_ID': lambda x: list(x),
        'MATCHUP': lambda x: list(x),
        'PTS': lambda x: list(x)
    })
    
    game_list = []
    for game_id, row in game_groups.iterrows():
        # Skip games with invalid team IDs
        if not all(team_id in valid_team_ids for team_id in row['TEAM_ID']):
            print(f"Skipping game {game_id} - Invalid team ID")
            continue
            
        is_home_0 = '@' not in row['MATCHUP'][0]
        
        game_dict = {
            'game_id': game_id,
            'game_date': pd.to_datetime(row['GAME_DATE']).date(),
            'home_team_id': row['TEAM_ID'][0] if is_home_0 else row['TEAM_ID'][1],
            'away_team_id': row['TEAM_ID'][1] if is_home_0 else row['TEAM_ID'][0],
            'home_team_score': row['PTS'][0] if is_home_0 else row['PTS'][1],
            'away_team_score': row['PTS'][1] if is_home_0 else row['PTS'][0]
        }
        game_list.append(game_dict)
    return game_list

def create_games_table():
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
        
        # Get valid team IDs from database
        cursor.execute("SELECT team_id FROM teams")
        valid_team_ids = {row[0] for row in cursor.fetchall()}
        
        # Get games
        gamefinder = leaguegamefinder.LeagueGameFinder(
            season_nullable="2024-25",
            league_id_nullable="00"
        )
        games_df = gamefinder.get_data_frames()[0]
        
        # Create table if not exists
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS games (
            game_id VARCHAR(20) PRIMARY KEY,
            game_date DATE,
            home_team_id INTEGER,
            away_team_id INTEGER,
            home_team_score INTEGER,
            away_team_score INTEGER,
            season VARCHAR(10),
            FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
            FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
        );
        '''
        cursor.execute(create_table_query)
        
        # Process only games with valid team IDs
        games_list = get_game_details(games_df, valid_team_ids)
        
        for game in games_list:
            insert_query = '''
            INSERT INTO games 
            (game_id, game_date, home_team_id, away_team_id, 
             home_team_score, away_team_score, season)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id) DO UPDATE 
            SET home_team_score = EXCLUDED.home_team_score,
                away_team_score = EXCLUDED.away_team_score;
            '''
            cursor.execute(insert_query, (
                game['game_id'],
                game['game_date'],
                game['home_team_id'],
                game['away_team_id'],
                game['home_team_score'],
                game['away_team_score'],
                "2024-25"
            ))
            connection.commit()
            print(f"Game {game['game_id']} saved successfully")

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
    create_games_table()