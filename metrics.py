from nba_api.stats.endpoints import leaguegamefinder, teamgamelog
import psycopg2
from psycopg2 import Error
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta

def create_prediction_tables():
    """Create tables for predictions and metrics"""
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

        # Create metrics table
        create_metrics_query = '''
        CREATE TABLE IF NOT EXISTS team_metrics (
            id SERIAL PRIMARY KEY,
            team_id INTEGER REFERENCES teams(team_id),
            date DATE,
            last_5_wins INTEGER,
            last_10_wins INTEGER,
            home_win_pct FLOAT,
            away_win_pct FLOAT,
            points_scored_avg FLOAT,
            points_allowed_avg FLOAT,
            offensive_rating FLOAT,
            defensive_rating FLOAT,
            rest_days INTEGER,
            UNIQUE(team_id, date)
        );
        '''
        
        # Create predictions table
        create_predictions_query = '''
        CREATE TABLE IF NOT EXISTS game_predictions (
            id SERIAL PRIMARY KEY,
            game_id VARCHAR(20) REFERENCES games(game_id),
            predicted_winner_id INTEGER REFERENCES teams(team_id),
            predicted_home_score INTEGER,
            predicted_away_score INTEGER,
            predicted_total_points INTEGER,
            win_probability FLOAT,
            prediction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            actual_winner_id INTEGER REFERENCES teams(team_id),
            prediction_correct BOOLEAN,
            points_difference INTEGER,
            UNIQUE(game_id)
        );
        '''
        
        cursor.execute(create_metrics_query)
        cursor.execute(create_predictions_query)
        connection.commit()
        print("Prediction tables created successfully")

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
    create_prediction_tables()