import os

# Database Configuration - ưu tiên environment variables cho Docker
DB_HOST = os.getenv("DB_HOST", "163.223.8.12")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "cdudu")
DB_PASSWORD = os.getenv("DB_PASSWORD", "cdudu.com")
DB_NAME = os.getenv("DB_NAME", "google-map-data")
DB_SSL_MODE = os.getenv("DB_SSL_MODE", "disable")
DB_MAX_OPEN_CONNS = int(os.getenv("DB_MAX_OPEN_CONNS", "100"))
DB_MAX_IDLE_CONNS = int(os.getenv("DB_MAX_IDLE_CONNS", "10"))
DB_CONN_MAX_LIFETIME = int(os.getenv("DB_CONN_MAX_LIFETIME", "3600"))

# Database connection string
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={DB_SSL_MODE}"

# Database config dict for psycopg2
DB_CONFIG = {
    "host": DB_HOST,
    "port": DB_PORT,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "dbname": DB_NAME,
    "sslmode": DB_SSL_MODE
}
