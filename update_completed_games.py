# her gun bu scripti calistir
# bu scriptin gorevi nba_api ile gunun oyunlarini cekmek ve database'e eklemek

from nba_api.stats.endpoints import scoreboardv2, boxscoresummaryv2, boxscoretraditionalv2
import psycopg2
from psycopg2 import Error
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta

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

def update_completed_games():
    """Update database with completed games from yesterday"""
    connection = None
    cursor = None
    try:
        # Get yesterday's completed games
        yesterday = datetime.now() - timedelta(days=1)
        games = scoreboardv2.ScoreboardV2(game_date=yesterday.strftime('%Y-%m-%d'))
        games_df = games.game_header.get_data_frame()
        completed_games = games_df[games_df['GAME_STATUS_TEXT'] == 'Final']

        if completed_games.empty:
            print("No completed games found for yesterday")
            return

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

        for _, game in completed_games.iterrows():
            try:
                # Get game details and scores
                box_score = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game['GAME_ID'])
                line_score = box_score.line_score.get_data_frame()
                
                # Get home and away team scores
                home_score = line_score[line_score['TEAM_ID'] == int(game['HOME_TEAM_ID'])]['PTS'].iloc[0]
                away_score = line_score[line_score['TEAM_ID'] == int(game['VISITOR_TEAM_ID'])]['PTS'].iloc[0]

                # Update games table
                update_game_query = '''
                INSERT INTO games 
                (game_id, game_date, home_team_id, away_team_id, 
                home_team_score, away_team_score, season)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id) DO UPDATE 
                SET home_team_score = EXCLUDED.home_team_score,
                    away_team_score = EXCLUDED.away_team_score;
                '''
                
                cursor.execute(update_game_query, (
                    game['GAME_ID'],
                    pd.to_datetime(game['GAME_DATE_EST']).date(),
                    int(game['HOME_TEAM_ID']),
                    int(game['VISITOR_TEAM_ID']),
                    int(home_score),
                    int(away_score),
                    str(game['SEASON'])
                ))

                # Get and update player statistics
                traditional_box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game['GAME_ID'])
                player_stats = traditional_box_score.player_stats.get_data_frame()

                for _, player in player_stats.iterrows():
                    update_player_stats_query = '''
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
                        rebounds = EXCLUDED.rebounds,
                        steals = EXCLUDED.steals,
                        blocks = EXCLUDED.blocks,
                        turnovers = EXCLUDED.turnovers,
                        field_goals_made = EXCLUDED.field_goals_made,
                        field_goals_attempted = EXCLUDED.field_goals_attempted,
                        three_points_made = EXCLUDED.three_points_made,
                        three_points_attempted = EXCLUDED.three_points_attempted,
                        free_throws_made = EXCLUDED.free_throws_made,
                        free_throws_attempted = EXCLUDED.free_throws_attempted,
                        plus_minus = EXCLUDED.plus_minus;
                    '''

                    cursor.execute(update_player_stats_query, (
                        game['GAME_ID'],
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

                # Remove from upcoming games
                cursor.execute(
                    "DELETE FROM upcoming_games WHERE game_id = %s",
                    (game['GAME_ID'],)
                )

                connection.commit()
                print(f"Game {game['GAME_ID']} and its stats updated successfully")

            except Exception as e:
                print(f"Error processing game {game['GAME_ID']}: {e}")
                connection.rollback()
                continue

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
    update_completed_games()