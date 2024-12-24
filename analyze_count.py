from step_2_features_engineering import prepare_training_features
import pandas as pd

# Load training data
df = prepare_training_features()

# Analyze dataset
print("\nTraining Data Analysis:")
print(f"Total games in dataset: {len(df)}")
print(f"Date range: {df['game_date'].min()} to {df['game_date'].max()}")
print("\nHome wins: {:.1%}".format(df['home_win'].mean()))
print("Average total points: {:.1f}".format(df['total_points'].mean()))