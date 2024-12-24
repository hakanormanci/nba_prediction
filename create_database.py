import psycopg2
from psycopg2 import Error
import os
from dotenv import load_dotenv

def create_database():
    try:
        # .env dosyasından veritabanı bağlantı bilgilerini al
        load_dotenv()
        
        # Önce postgres default veritabanına bağlan
        connection = psycopg2.connect(
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            database="postgres"
        )
        connection.autocommit = True
        cursor = connection.cursor()
        
        # Veritabanının var olup olmadığını kontrol et
        cursor.execute("SELECT 1 FROM pg_database WHERE datname='nba_stats'")
        exists = cursor.fetchone()
        
        if not exists:
            # Veritabanını oluştur
            cursor.execute('CREATE DATABASE nba_stats')
            print("nba_stats veritabanı başarıyla oluşturuldu.")
        else:
            print("nba_stats veritabanı zaten mevcut.")
            
    except (Exception, Error) as error:
        print("Hata:", error)
    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    create_database()