from nba_api.stats.endpoints import boxscoretraditionalv2
import psycopg2
from psycopg2 import Error
import os
from dotenv import load_dotenv
import pandas as pd

def convert_minutes_to_int(minutes_str):
    """Convert minutes from various formats to integer minutes"""
    try:
        if pd.isna(minutes_str):
            return 0
        if isinstance(minutes_str, (int, float)):
            return int(minutes_str)
        if ':' in str(minutes_str):
            mins, secs = map(float, str(minutes_str).split(':'))
            return int(mins + secs/60)
        return int(float(str(minutes_str).replace('.', ':')))
    except:
        print(f"Warning: Could not parse minutes value: {minutes_str}")
        return 0
    
def safe_int_convert(value, default=0):
    """Safely convert value to integer, handling NaN and None"""
    try:
        if pd.isna(value):
            return default
        return int(float(value))
    except:
        return default

def create_player_game_stats_table():
    """Create and populate player game statistics table"""
    connection = None
    cursor = None
    try:
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

        # Create table if not exists
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS player_game_stats (
            id SERIAL PRIMARY KEY,
            game_id VARCHAR(20),
            player_id INTEGER,
            team_id INTEGER,
            minutes INTEGER,
            points INTEGER,
            assists INTEGER,
            rebounds INTEGER,
            steals INTEGER,
            blocks INTEGER,
            turnovers INTEGER,
            field_goals_made INTEGER,
            field_goals_attempted INTEGER,
            three_points_made INTEGER,
            three_points_attempted INTEGER,
            free_throws_made INTEGER,
            free_throws_attempted INTEGER,
            plus_minus INTEGER,
            FOREIGN KEY (game_id) REFERENCES games(game_id),
            FOREIGN KEY (player_id) REFERENCES players(player_id),
            FOREIGN KEY (team_id) REFERENCES teams(team_id),
            UNIQUE(game_id, player_id)
        );
        '''
        cursor.execute(create_table_query)
        connection.commit()

        # Get games from database
        cursor.execute("SELECT game_id FROM games")
        game_ids = [row[0] for row in cursor.fetchall()]
        print(f"Found {len(game_ids)} games to process")

        # Process each game
        for game_id in game_ids:
            try:
                box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
                player_stats = box_score.player_stats.get_data_frame()

                for _, player in player_stats.iterrows():
                    # Check and insert player with minimal info
                    cursor.execute("SELECT 1 FROM players WHERE player_id = %s", (int(player['PLAYER_ID']),))
                    if not cursor.fetchone():
                        cursor.execute(
                            "INSERT INTO players (player_id, full_name, is_active) VALUES (%s, %s, %s)",
                            (int(player['PLAYER_ID']), player['PLAYER_NAME'], True)
                        )

                    # Insert player stats
                    insert_query = '''
                    INSERT INTO player_game_stats 
                    (game_id, player_id, team_id, minutes, points, assists, rebounds,
                    steals, blocks, turnovers, field_goals_made, field_goals_attempted,
                    three_points_made, three_points_attempted, free_throws_made,
                    free_throws_attempted, plus_minus)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (game_id, player_id) DO UPDATE 
                    SET minutes = EXCLUDED.minutes,
                        points = EXCLUDED.points,
                        assists = EXCLUDED.assists,
                        rebounds = EXCLUDED.rebounds;
                    '''

                    cursor.execute(insert_query, (
                        game_id,
                        int(player['PLAYER_ID']),
                        int(player['TEAM_ID']),
                        convert_minutes_to_int(player['MIN']),
                        safe_int_convert(player['PTS']),
                        safe_int_convert(player['AST']),
                        safe_int_convert(player['REB']),
                        safe_int_convert(player['STL']),
                        safe_int_convert(player['BLK']),
                        safe_int_convert(player['TO']),
                        safe_int_convert(player['FGM']),
                        safe_int_convert(player['FGA']),
                        safe_int_convert(player['FG3M']),
                        safe_int_convert(player['FG3A']),
                        safe_int_convert(player['FTM']),
                        safe_int_convert(player['FTA']),
                        safe_int_convert(player['PLUS_MINUS'])
                    ))
                
                connection.commit()
                print(f"Stats for game {game_id} saved successfully")

            except Exception as e:
                print(f"Error processing game {game_id}: {e}")
                connection.rollback()
                continue

    except (Exception, Error) as error:
        print(f"Database error: {error}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    create_player_game_stats_table()