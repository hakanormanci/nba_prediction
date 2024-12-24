from nba_api.stats.endpoints import commonallplayers
import psycopg2
from psycopg2 import Error
import os
from dotenv import load_dotenv
import pandas as pd

def update_player_teams():
    connection = None
    cursor = None
    try:
        load_dotenv()
        
        # NBA API'den verileri al
        all_players = commonallplayers.CommonAllPlayers()
        players_data = all_players.get_data_frames()[0]
        
        # Aktif oyuncuları filtrele
        active_players = players_data[players_data['ROSTERSTATUS'] == 1]

        connection = psycopg2.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            database="nba_stats"
        )
        
        cursor = connection.cursor()

        # Her aktif oyuncunun takım bilgisini güncelle
        update_query = '''
        UPDATE players 
        SET team_id = %s 
        WHERE player_id = %s AND team_id IS NULL;
        '''

        updates = 0
        for _, player in active_players.iterrows():
            if player['TEAM_ID'] != 0:  # 0 = free agent
                cursor.execute(update_query, 
                             (int(player['TEAM_ID']), 
                              int(player['PERSON_ID'])))
                updates += cursor.rowcount

        connection.commit()
        print(f"{updates} oyuncunun takım bilgisi güncellendi")

        # Doğrulama sorgusu
        cursor.execute("""
            SELECT p.full_name, t.full_name as team_name 
            FROM players p 
            LEFT JOIN teams t ON p.team_id = t.team_id 
            WHERE p.is_active = true 
            ORDER BY t.full_name;
        """)
        
        print("\nGüncel Takım Kadroları:")
        for player in cursor.fetchall():
            print(f"{player[0]} - {player[1] or 'Takım yok'}")

    except (Exception, Error) as error:
        if connection:
            connection.rollback()
        print(f"Hata: {error}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    update_player_teams()