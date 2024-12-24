from nba_api.stats.endpoints import leaguegamefinder, teamgamelog
import psycopg2
from psycopg2 import Error
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta

def calculate_team_metrics(team_id, date):
    team_log = teamgamelog.TeamGameLog(team_id=team_id, season='2023-24')
    games_df = team_log.get_data_frames()[0]
    
    games_df['GAME_DATE'] = pd.to_datetime(games_df['GAME_DATE'], format='mixed')
    target_date = pd.to_datetime(date).date()
    past_games = games_df[games_df['GAME_DATE'].dt.date < target_date].head(10)
    
    if past_games.empty:
        return {
            'last_5_wins': 0,
            'last_10_wins': 0,
            'points_scored_avg': 0,
            'home_games': 0,
            'home_wins': 0,
            'away_games': 0,
            'away_wins': 0,
            'rest_days': 0
        }
    
    # Calculate rest days between games
    past_games = past_games.sort_values('GAME_DATE', ascending=False)  # Most recent first
    past_games['rest_days'] = (past_games['GAME_DATE'] - past_games['GAME_DATE'].shift(-1)).dt.days
    
    metrics = {
        'last_5_wins': len(past_games.head(5)[past_games.head(5)['WL'] == 'W']),
        'last_10_wins': len(past_games[past_games['WL'] == 'W']),
        'points_scored_avg': past_games['PTS'].mean(),
        'home_games': len(past_games[~past_games['MATCHUP'].str.contains('@')]),
        'home_wins': len(past_games[(~past_games['MATCHUP'].str.contains('@')) & (past_games['WL'] == 'W')]),
        'away_games': len(past_games[past_games['MATCHUP'].str.contains('@')]),
        'away_wins': len(past_games[past_games['MATCHUP'].str.contains('@') & (past_games['WL'] == 'W')]),
        'rest_days': past_games['rest_days'].iloc[0] if not past_games.empty else 0  # Get rest days since last game
    }
    
    return metrics

def update_team_metrics():
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

        cursor.execute("SELECT team_id FROM teams")
        teams = cursor.fetchall()
        today = datetime.now()
        
        for (team_id,) in teams:
            metrics = calculate_team_metrics(team_id, today)
            
            insert_query = '''
            INSERT INTO team_metrics 
            (team_id, date, last_5_wins, last_10_wins, 
            points_scored_avg, home_win_pct, away_win_pct, rest_days)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (team_id, date) DO UPDATE 
            SET last_5_wins = EXCLUDED.last_5_wins,
                last_10_wins = EXCLUDED.last_10_wins,
                points_scored_avg = EXCLUDED.points_scored_avg,
                home_win_pct = EXCLUDED.home_win_pct,
                away_win_pct = EXCLUDED.away_win_pct,
                rest_days = EXCLUDED.rest_days;
            '''
            
            cursor.execute(insert_query, (
                team_id,
                today.date(),
                metrics['last_5_wins'],
                metrics['last_10_wins'],
                round(float(metrics['points_scored_avg']), 2),
                round(metrics['home_wins'] / metrics['home_games'] if metrics['home_games'] > 0 else 0, 3),
                round(metrics['away_wins'] / metrics['away_games'] if metrics['away_games'] > 0 else 0, 3),
                round(float(metrics['rest_days']), 1)
            ))
            
            connection.commit()
            print(f"Updated metrics for team {team_id}")

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
    update_team_metrics()