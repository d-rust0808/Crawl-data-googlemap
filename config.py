import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Load environment variables from .env file
load_dotenv()

# Database Configuration - lấy từ environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_SSL_MODE = os.getenv("DB_SSL_MODE")
DB_MAX_OPEN_CONNS = int(os.getenv("DB_MAX_OPEN_CONNS", "100"))
DB_MAX_IDLE_CONNS = int(os.getenv("DB_MAX_IDLE_CONNS", "10"))
DB_CONN_MAX_LIFETIME = int(os.getenv("DB_CONN_MAX_LIFETIME", "3600"))

# Validate required database variables
if not all([DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME]):
    raise ValueError("❌ Thiếu thông tin database trong file .env. Vui lòng kiểm tra DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME")

# Database connection string - URL encode password để xử lý ký tự đặc biệt
encoded_user = quote_plus(DB_USER)
encoded_password = quote_plus(DB_PASSWORD)
DATABASE_URL = f"postgresql://{encoded_user}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={DB_SSL_MODE or 'disable'}"

# Database config dict for psycopg2
DB_CONFIG = {
    "host": DB_HOST,
    "port": DB_PORT,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "dbname": DB_NAME,
    "sslmode": DB_SSL_MODE or "disable"
}

# Proxy Configuration - lấy từ environment variables
PROXY_HOST = os.getenv("PROXY_HOST")
PROXY_PORT = os.getenv("PROXY_PORT")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

# Validate required proxy variables
if not all([PROXY_HOST, PROXY_PORT, PROXY_USERNAME, PROXY_PASSWORD]):
    raise ValueError("❌ Thiếu thông tin proxy trong file .env. Vui lòng kiểm tra PROXY_HOST, PROXY_PORT, PROXY_USERNAME, PROXY_PASSWORD")

# Threading Configuration
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "1"))  # Số luồng tối đa
THREAD_DELAY = float(os.getenv("THREAD_DELAY", "3.0"))  # Delay giữa các request (giây)
PROXY_RETRY_COUNT = int(os.getenv("PROXY_RETRY_COUNT", "3"))  # Số lần retry khi proxy fail