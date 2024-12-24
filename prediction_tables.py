import psycopg2
from dotenv import load_dotenv
import os

def create_prediction_tables():
    connection = None
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

        # Game Predictions Table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS game_predictions (
            id SERIAL PRIMARY KEY,
            game_id VARCHAR(20) REFERENCES games(game_id),
            prediction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            predicted_winner_id INTEGER REFERENCES teams(team_id),
            win_probability FLOAT,
            predicted_home_score INTEGER,
            predicted_away_score INTEGER,
            predicted_total_points INTEGER,
            actual_winner_id INTEGER REFERENCES teams(team_id),
            prediction_accuracy BOOLEAN,
            UNIQUE(game_id)
        );
        ''')

        # Game Features Table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS game_features (
            id SERIAL PRIMARY KEY,
            game_id VARCHAR(20) REFERENCES games(game_id),
            game_date DATE,
            home_team_id INTEGER REFERENCES teams(team_id),
            away_team_id INTEGER REFERENCES teams(team_id),
            home_last_5_wins INTEGER,
            home_last_10_wins INTEGER,
            home_points_avg FLOAT,
            home_win_pct FLOAT,
            away_last_5_wins INTEGER,
            away_last_10_wins INTEGER,
            away_points_avg FLOAT,
            away_win_pct FLOAT,
            UNIQUE(game_id)
        );
        ''')

        connection.commit()
        print("Prediction tables created successfully")

    except Exception as error:
        print(f"Error: {error}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    create_prediction_tables()