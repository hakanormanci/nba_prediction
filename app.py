from flask import Flask, render_template
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import joblib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from nba_api.stats.endpoints import scoreboardv2, teamestimatedmetrics
from nba_api.stats.static import teams
from api_config import get_session

# Initialize Flask app
app = Flask(__name__)
Base = declarative_base()

# Database Model
class PredictedGame(Base):
    __tablename__ = 'predicted_games'
    
    id = Column(Integer, primary_key=True)
    game_date = Column(DateTime)
    home_team = Column(String)
    away_team = Column(String)
    predicted_winner = Column(String)
    predicted_total = Column(Float)
    actual_winner = Column(String, nullable=True)
    actual_total = Column(Float, nullable=True)
    prediction_correct = Column(Boolean, nullable=True)

# Load models
model_data = joblib.load('models/nba_prediction_models.joblib')
winner_model = model_data['winner_model']
total_model = model_data['total_model']
scaler = model_data['scaler']
feature_columns = model_data['feature_columns']

def get_team_name(team_id):
    """Get team name from team ID"""
    return next((team['full_name'] for team in teams.get_teams() 
                if team['id'] == team_id), None)

def get_upcoming_games():
    """Get next 3 days of NBA games"""
    games = []
    for days in range(3):
        date = datetime.now() + timedelta(days=days)
        scoreboard = scoreboardv2.ScoreboardV2(game_date=date.strftime('%Y-%m-%d'))
        games_on_date = scoreboard.game_header.get_data_frame()
        
        for _, game in games_on_date.iterrows():
            games.append({
                'date': pd.to_datetime(game['GAME_DATE_EST']),
                'home_team': get_team_name(game['HOME_TEAM_ID']),
                'away_team': get_team_name(game['VISITOR_TEAM_ID']),
                'home_team_id': game['HOME_TEAM_ID'],
                'away_team_id': game['VISITOR_TEAM_ID']
            })
    return games

def get_team_metrics(team_id):
    """Get team performance metrics with fallback values"""
    try:
        metrics = teamestimatedmetrics.TeamEstimatedMetrics()
        team_stats = metrics.get_data_frames()[0]
        
        # Print available columns for debugging
        print(f"Available columns: {team_stats.columns.tolist()}")
        
        team_row = team_stats[team_stats['TEAM_ID'] == team_id].iloc[0]
        
        return {
            'off_rtg': float(team_row.get('E_OFF_RATING', 110.0)),
            'pace': float(team_row.get('E_PACE', 100.0)),
            'ts_pct': float(team_row.get('E_NET_RATING', 0.0)) / 200.0 + 0.55  # Estimate TS% from net rating
        }
    except Exception as e:
        print(f"Error getting metrics for team {team_id}: {str(e)}")
        return {
            'off_rtg': 110.0,  # League average
            'pace': 100.0,     # League average
            'ts_pct': 0.550    # League average
        }



def prepare_prediction_features(home_team_id, away_team_id):
    """Prepare features for prediction"""
    home_metrics = get_team_metrics(home_team_id)
    away_metrics = get_team_metrics(away_team_id)
    
    features = {
        'off_rtg_diff': home_metrics['off_rtg'] - away_metrics['off_rtg'],
        'ts_pct_diff': home_metrics['ts_pct'] - away_metrics['ts_pct'],
        'pace_diff': home_metrics['pace'] - away_metrics['pace'],
        'away_off_rtg': away_metrics['off_rtg'],
        'home_off_rtg': home_metrics['off_rtg'],
        'h2h_home_win_pct': 0.5  # Default value if no history
    }
    
    return pd.DataFrame([features])[feature_columns]

def make_prediction(game):
    """Make predictions for a single game"""
    features = prepare_prediction_features(game['home_team_id'], game['away_team_id'])
    features_scaled = scaler.transform(features)
    
    win_prob = winner_model.predict(features_scaled)[0]
    total_points = total_model.predict(features_scaled)[0]
    
    # Clamp probability between 0 and 1
    win_prob = min(max(win_prob, 0), 1)
    
    predicted_winner = game['home_team'] if win_prob > 0.5 else game['away_team']
    win_probability = win_prob if predicted_winner == game['home_team'] else 1 - win_prob
    
    return {
        'winner': predicted_winner,
        'win_probability': min(win_probability * 100, 99.9),  # Convert to percentage and cap at 99.9%
        'total_points': total_points
    }

@app.route('/')
def index():
    games = get_upcoming_games()
    predictions = []
    
    for game in games:
        pred = make_prediction(game)
        predictions.append({
            'date': game['date'],
            'home_team': game['home_team'],
            'away_team': game['away_team'],
            'predicted_winner': pred['winner'],
            'win_probability': f"{pred['win_probability']:.1f}%",  # Format as percentage
            'predicted_total': round(pred['total_points'], 1)
        })
    
    return render_template('index.html', predictions=predictions)

def get_past_games(days=7):
    """Get completed games from the past week"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    past_games = []
    
    for date in pd.date_range(start=start_date, end=end_date):
        try:
            # Get scoreboard data
            scoreboard = scoreboardv2.ScoreboardV2(game_date=date.strftime('%Y-%m-%d'))
            games = scoreboard.game_header.get_data_frame()
            scores = scoreboard.line_score.get_data_frame()
            
            # Filter completed games
            completed_games = games[games['GAME_STATUS_ID'] == 3]
            
            for _, game in completed_games.iterrows():
                game_id = game['GAME_ID']
                
                # Get scores from line_score DataFrame
                home_score = scores[
                    (scores['GAME_ID'] == game_id) & 
                    (scores['TEAM_ID'] == game['HOME_TEAM_ID'])
                ]['PTS'].iloc[0]
                
                away_score = scores[
                    (scores['GAME_ID'] == game_id) & 
                    (scores['TEAM_ID'] == game['VISITOR_TEAM_ID'])
                ]['PTS'].iloc[0]
                
                past_games.append({
                    'date': pd.to_datetime(game['GAME_DATE_EST']),
                    'home_team': get_team_name(game['HOME_TEAM_ID']),
                    'away_team': get_team_name(game['VISITOR_TEAM_ID']),
                    'home_team_id': game['HOME_TEAM_ID'],
                    'away_team_id': game['VISITOR_TEAM_ID'],
                    'home_score': int(home_score),
                    'away_score': int(away_score)
                })
                
        except Exception as e:
            print(f"Error fetching games for {date.strftime('%Y-%m-%d')}: {str(e)}")
            continue
            
    return past_games

@app.route('/history')
def history():
    past_games = get_past_games()
    results = []
    
    # Stats counters
    total_games = 0
    correct_winners = 0
    total_points_diff = 0
    
    for game in past_games:
        pred = make_prediction(game)
        actual_winner = game['home_team'] if game['home_score'] > game['away_score'] else game['away_team']
        actual_total = game['home_score'] + game['away_score']
        prediction_correct = pred['winner'] == actual_winner
        
        # Update counters
        total_games += 1
        if prediction_correct:
            correct_winners += 1
        total_points_diff += abs(pred['total_points'] - actual_total)
        
        results.append({
            'date': game['date'],
            'home_team': game['home_team'],
            'away_team': game['away_team'],
            'predicted_winner': pred['winner'],
            'actual_winner': actual_winner,
            'prediction_correct': prediction_correct,
            'predicted_total': round(pred['total_points'], 1),
            'actual_total': actual_total,
            'total_difference': abs(round(pred['total_points'] - actual_total, 1)),
            'win_probability': f"{pred['win_probability']:.1f}%"
        })
    
    # Calculate summary stats
    summary_stats = {
        'total_games': total_games,
        'correct_predictions': correct_winners,
        'incorrect_predictions': total_games - correct_winners,
        'accuracy_percentage': (correct_winners / total_games * 100) if total_games > 0 else 0,
        'avg_points_diff': (total_points_diff / total_games) if total_games > 0 else 0
    }
    
    return render_template('history.html', results=results, stats=summary_stats)


if __name__ == '__main__':
    engine = create_engine('sqlite:///predictions.db')
    Base.metadata.create_all(engine)
    app.run(debug=True)