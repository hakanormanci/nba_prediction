from nba_api.stats.static import players
import psycopg2
from psycopg2 import Error
import os
from dotenv import load_dotenv

def create_players_table():
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

        # Oyuncular tablosunu oluştur
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY,
            full_name VARCHAR(100),
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            is_active BOOLEAN,
            team_id INTEGER,
            FOREIGN KEY (team_id) REFERENCES teams(team_id)
        );
        '''
        cursor.execute(create_table_query)

        # NBA API'den aktif oyuncuları al
        nba_players = players.get_active_players()

        # Verileri tabloya ekle
        for player in nba_players:
            insert_query = '''
            INSERT INTO players (player_id, full_name, first_name, last_name, is_active)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (player_id) DO UPDATE 
            SET full_name = EXCLUDED.full_name,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                is_active = EXCLUDED.is_active;
            '''
            record_to_insert = (
                player['id'],
                player['full_name'],
                player['first_name'],
                player['last_name'],
                player['is_active']
            )
            cursor.execute(insert_query, record_to_insert)

        connection.commit()
        print("Oyuncu verileri başarıyla kaydedildi")

    except (Exception, Error) as error:
        print("PostgreSQL'de hata oluştu:", error)
    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    create_players_table()