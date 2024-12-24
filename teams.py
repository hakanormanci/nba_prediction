from nba_api.stats.static import teams
import psycopg2
from psycopg2 import Error

def create_teams_table():
    try:
        # PostgreSQL bağlantısı
        connection = psycopg2.connect(
            user="postgres",
            password="164051",
            host="localhost",
            port="5432",
            database="nba_stats"
        )
        
        cursor = connection.cursor()

        # Takımlar tablosunu oluştur
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS teams (
            team_id INTEGER PRIMARY KEY,
            full_name VARCHAR(100),
            abbreviation VARCHAR(10),
            nickname VARCHAR(50),
            city VARCHAR(50),
            state VARCHAR(50),
            year_founded INTEGER
        );
        '''
        cursor.execute(create_table_query)

        # NBA API'den takım verilerini al
        nba_teams = teams.get_teams()

        # Verileri tabloya ekle
        for team in nba_teams:
            insert_query = '''
            INSERT INTO teams (team_id, full_name, abbreviation, nickname, city, state, year_founded)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (team_id) DO UPDATE 
            SET full_name = EXCLUDED.full_name,
                abbreviation = EXCLUDED.abbreviation,
                nickname = EXCLUDED.nickname,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                year_founded = EXCLUDED.year_founded;
            '''
            record_to_insert = (
                team['id'],
                team['full_name'],
                team['abbreviation'],
                team['nickname'],
                team['city'],
                team['state'],
                team['year_founded']
            )
            cursor.execute(insert_query, record_to_insert)

        connection.commit()
        print("Takım verileri başarıyla kaydedildi")

    except (Exception, Error) as error:
        print("PostgreSQL'de hata oluştu:", error)
    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    create_teams_table()