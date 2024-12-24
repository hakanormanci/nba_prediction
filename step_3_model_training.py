import os
import logging
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor
from sklearn.metrics import accuracy_score, mean_squared_error, r2_score
import joblib
from step_2_features_engineering import prepare_training_features

# Constants
MODEL_PARAMS = {
    'n_estimators': 1000,
    'learning_rate': 0.01,
    'max_depth': 3,
    'subsample': 0.8,
    'colsample_bytree': 0.8,
    'objective': 'reg:squarederror',
    'eval_metric': 'rmse',
    'random_state': 42
}

# Features ordered by importance
PRIMARY_FEATURES = [
    'off_rtg_diff',      # Offensive rating differential
    'ts_pct_diff',       # True shooting % differential
    'h2h_home_win_pct',  # Head-to-head home win %
    'pace_diff',         # Pace differential
    'away_off_rtg',      # Away team offensive rating
    'home_off_rtg'       # Home team offensive rating
]

def evaluate_model(model, X_test, y_test, is_winner=True):
    """Evaluate model performance and log metrics"""
    predictions = model.predict(X_test)
    metrics = {
        'r2': r2_score(y_test, predictions)
    }
    
    if is_winner:
        metrics['accuracy'] = accuracy_score(y_test, predictions.round())
        logging.info(f"Winner Model - Accuracy: {metrics['accuracy']:.3f}, R²: {metrics['r2']:.3f}")
    else:
        metrics['rmse'] = np.sqrt(mean_squared_error(y_test, predictions))
        logging.info(f"Total Points Model - RMSE: {metrics['rmse']:.1f}, R²: {metrics['r2']:.3f}")
    
    return metrics, predictions

def train_prediction_models():
    """Train and evaluate NBA prediction models"""
    try:
        logging.info("Starting model training process")
        df = prepare_training_features()
        
        X = df[PRIMARY_FEATURES]
        y_winner = df['home_win']
        y_total = df['total_points']
        
        # Split and scale data
        X_train, X_test, y_train_winner, y_test_winner = train_test_split(X, y_winner, test_size=0.2, random_state=42)
        _, _, y_train_total, y_test_total = train_test_split(X, y_total, test_size=0.2, random_state=42)
        
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Train models
        logging.info("Training winner prediction model")
        winner_model = XGBRegressor(**MODEL_PARAMS)
        winner_model.fit(X_train_scaled, y_train_winner)
        
        logging.info("Training total points model")
        total_model = XGBRegressor(**MODEL_PARAMS)
        total_model.fit(X_train_scaled, y_train_total)
        
        # Evaluate performance
        winner_metrics, winner_preds = evaluate_model(winner_model, X_test_scaled, y_test_winner, True)
        total_metrics, total_preds = evaluate_model(total_model, X_test_scaled, y_test_total, False)
        
        # Get feature importance
        winner_importance = dict(zip(PRIMARY_FEATURES, winner_model.feature_importances_))
        total_importance = dict(zip(PRIMARY_FEATURES, total_model.feature_importances_))
        
        # Save artifacts
        models_dir = os.path.join(os.path.dirname(__file__), 'models')
        if not os.path.exists(models_dir):
            os.makedirs(models_dir)
            
        model_data = {
            'winner_model': winner_model,
            'total_model': total_model,
            'scaler': scaler,
            'feature_columns': PRIMARY_FEATURES,
            'winner_metrics': winner_metrics,
            'total_metrics': total_metrics,
            'feature_importance': {
                'winner': winner_importance,
                'total': total_importance
            },
            'params': MODEL_PARAMS,
            'training_date': pd.Timestamp.now()
        }
        
        model_path = os.path.join(models_dir, 'nba_prediction_models.joblib')
        joblib.dump(model_data, model_path)
        logging.info(f"Models saved to {model_path}")
        
        return winner_model, total_model
        
    except Exception as e:
        logging.error(f"Error training models: {e}", exc_info=True)
        return None, None

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('model_training.log'),
            logging.StreamHandler()
        ]
    )
    winner_model, total_model = train_prediction_models()