import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def get_db_engine():
    """Create SQLAlchemy database engine"""
    load_dotenv()
    db_params = {
        'user': os.getenv("DB_USER"),
        'password': os.getenv("DB_PASSWORD"),
        'host': os.getenv("DB_HOST", "localhost"),
        'port': os.getenv("DB_PORT", "5432"),
        'database': "nba_stats"
    }
    return create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')

def prepare_training_features():
    engine = get_db_engine()
    
    features_query = '''
    WITH league_metrics AS (
        SELECT 
            AVG(points) as avg_points,
            AVG(field_goals_attempted + 0.44 * free_throws_attempted - total_rebounds * 0.3 + turnovers) as avg_pace,
            AVG(CAST(points AS float) / (2 * NULLIF(field_goals_attempted + 0.44 * free_throws_attempted, 0))) as avg_ts
        FROM team_game_stats
    ),
    advanced_stats AS (
        SELECT 
            tgs.*,
            (field_goals_attempted + 0.44 * free_throws_attempted - total_rebounds * 0.3 + turnovers) as possessions,
            100.0 * points / NULLIF(field_goals_attempted + 0.44 * free_throws_attempted - total_rebounds * 0.3 + turnovers, 0) as off_rtg,
            points::float / (2 * NULLIF(field_goals_attempted + 0.44 * free_throws_attempted, 0)) as true_shooting,
            lm.avg_points, lm.avg_pace, lm.avg_ts
        FROM team_game_stats tgs
        CROSS JOIN league_metrics lm
    ),
    matchup_stats AS (
        SELECT 
            home_team_id,
            away_team_id,
            COUNT(*) as games_played,
            SUM(CASE WHEN home_team_score > away_team_score THEN 1 ELSE 0 END) as home_team_wins
        FROM games 
        GROUP BY home_team_id, away_team_id
    )
    SELECT 
        g.game_id, g.game_date,
        g.home_team_id, g.away_team_id,
        g.home_team_score, g.away_team_score,
        -- Team metrics
        tm_home.last_5_wins as home_l5_wins,
        tm_home.last_10_wins as home_l10_wins,
        tm_home.points_scored_avg as home_pts_avg,
        tm_home.home_win_pct as home_win_pct,
        tm_away.last_5_wins as away_l5_wins,
        tm_away.last_10_wins as away_l10_wins,
        tm_away.points_scored_avg as away_pts_avg,
        tm_away.away_win_pct as away_win_pct,
        -- Advanced metrics
        h.off_rtg as home_off_rtg,
        h.possessions as home_pace,
        h.true_shooting as home_ts_pct,
        a.off_rtg as away_off_rtg,
        a.possessions as away_pace,
        a.true_shooting as away_ts_pct,
        -- League averages
        h.avg_pace as league_avg_pace,
        h.avg_points as league_avg_points,
        h.avg_ts as league_avg_ts,
        -- Matchup history
        ms.games_played as h2h_games,
        ms.home_team_wins as h2h_home_wins,
        CAST(ms.home_team_wins AS FLOAT) / NULLIF(ms.games_played, 0) as h2h_home_win_pct
    FROM games g
    JOIN advanced_stats h ON g.game_id = h.game_id AND g.home_team_id = h.team_id
    JOIN advanced_stats a ON g.game_id = a.game_id AND g.away_team_id = a.team_id
    JOIN team_metrics tm_home ON g.home_team_id = tm_home.team_id 
    JOIN team_metrics tm_away ON g.away_team_id = tm_away.team_id
    LEFT JOIN matchup_stats ms ON g.home_team_id = ms.home_team_id 
        AND g.away_team_id = ms.away_team_id
    WHERE g.home_team_score IS NOT NULL
    ORDER BY g.game_date DESC;
    '''
    
    df = pd.read_sql_query(features_query, engine)
    
    # Calculate target variables and differentials
    df['home_win'] = (df['home_team_score'] > df['away_team_score']).astype(int)
    df['total_points'] = df['home_team_score'] + df['away_team_score']
    df['pace_diff'] = df['home_pace'] - df['away_pace']
    df['off_rtg_diff'] = df['home_off_rtg'] - df['away_off_rtg']
    df['ts_pct_diff'] = df['home_ts_pct'] - df['away_ts_pct']
    df['home_pace_vs_avg'] = df['home_pace'] / df['league_avg_pace']
    df['away_pace_vs_avg'] = df['away_pace'] / df['league_avg_pace']
    
    return df

if __name__ == "__main__":
    df = prepare_training_features()
    print(f"\nGenerated features for {len(df)} games")
    print("\nFeature columns:")
    print(df.columns.tolist())