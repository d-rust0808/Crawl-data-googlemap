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

# Proxy Configuration
PROXY_HOST = os.getenv("PROXY_HOST", "42.114.125.153")
PROXY_PORT = os.getenv("PROXY_PORT", "34441")
PROXY_USERNAME = os.getenv("PROXY_USERNAME", "khljtiNj3Kd")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD", "fdkm3nbjg45d")

# Threading Configuration
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "1"))  # Số luồng tối đa (giảm xuống 1 để tránh trùng lặp)
THREAD_DELAY = float(os.getenv("THREAD_DELAY", "3.0"))  # Delay giữa các request (giây) - tăng lên
PROXY_RETRY_COUNT = int(os.getenv("PROXY_RETRY_COUNT", "3"))  # Số lần retry khi proxy fail