#!/usr/bin/env python3
"""
Database handler cho Google Maps Crawler
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import threading
from config import DATABASE_URL, DB_MAX_OPEN_CONNS, DB_MAX_IDLE_CONNS, DB_CONN_MAX_LIFETIME

logger = logging.getLogger(__name__)

class DatabaseHandler:
    """Handler để kết nối và thao tác với PostgreSQL database - Thread Safe"""
    
    def __init__(self):
        self.connection = None
        self.lock = threading.Lock()  # Thread lock cho thread safety
        self.connect()
        self.create_tables()
    
    def connect(self):
        """Kết nối đến database"""
        try:
            self.connection = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            self.connection.autocommit = False  # Đảm bảo autocommit = False
            logger.info("✅ Kết nối database thành công")
        except Exception as e:
            logger.error(f"❌ Lỗi kết nối database: {e}")
            raise
    
    def create_tables(self):
        """Tạo bảng nếu chưa tồn tại"""
        try:
            cursor = self.connection.cursor()
            
            # Tạo bảng stores
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS stores (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(500) NOT NULL,
                rating VARCHAR(50),
                link TEXT,
                phone VARCHAR(100),
                address TEXT,
                website TEXT,
                plus_code VARCHAR(100),
                search_keyword TEXT,
                search_location VARCHAR(255),
                crawl_session VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            cursor.execute(create_table_sql)
            
            # Kiểm tra và thêm cột crawl_session nếu chưa có
            try:
                cursor.execute("ALTER TABLE stores ADD COLUMN IF NOT EXISTS crawl_session VARCHAR(100);")
                self.connection.commit()
                logger.info("✅ Đã thêm cột crawl_session nếu chưa có")
            except Exception as e:
                logger.info(f"ℹ️ Cột crawl_session đã tồn tại hoặc lỗi: {e}")
            
            cursor.close()
            
            logger.info("✅ Bảng stores đã được tạo/kiểm tra")
            
        except Exception as e:
            logger.error(f"❌ Lỗi tạo bảng: {e}")
            raise
    
    def store_exists(self, store_id):
        """Kiểm tra cửa hàng đã tồn tại chưa - Thread Safe"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT id FROM stores WHERE id = %s", (store_id,))
            exists = cursor.fetchone() is not None
            cursor.close()
            return exists
        except Exception as e:
            logger.error(f"❌ Lỗi kiểm tra cửa hàng: {e}")
            return False
    
    def phone_exists(self, phone):
        """Kiểm tra số điện thoại đã tồn tại chưa - Thread Safe với timeout"""
        if not phone or phone in ['Not Found', 'Error', '']:
            return False
            
        # Không cần lock riêng cho read operation đơn giản
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT id FROM stores WHERE phone = %s", (phone,))
            exists = cursor.fetchone() is not None
            cursor.close()
            return exists
        except Exception as e:
            logger.error(f"❌ Lỗi kiểm tra số điện thoại: {e}")
            return False
    
    def get_store_by_phone(self, phone):
        """Lấy thông tin cửa hàng theo số điện thoại"""
        try:
            if not phone or phone in ['Not Found', 'Error', '']:
                return None
                
            cursor = self.connection.cursor()
            cursor.execute("SELECT * FROM stores WHERE phone = %s", (phone,))
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                return {
                    'id': result[0],
                    'name': result[1],
                    'rating': result[2],
                    'link': result[3],
                    'phone': result[4],
                    'address': result[5],
                    'website': result[6],
                    'plus_code': result[7],
                    'search_keyword': result[8],
                    'search_location': result[9],
                    'crawl_session': result[10]
                }
            return None
        except Exception as e:
            logger.error(f"❌ Lỗi lấy thông tin cửa hàng theo phone: {e}")
            return None
    
    def insert_store(self, store_data):
        """Thêm cửa hàng vào database (chỉ lọc theo số điện thoại) - Thread Safe với timeout"""
        import time
        
        # Thử acquire lock với timeout để tránh deadlock
        lock_acquired = self.lock.acquire(timeout=10)  # 10 giây timeout
        
        if not lock_acquired:
            logger.error("❌ Không thể acquire database lock sau 10s - có thể deadlock!")
            return False
            
        try:
            logger.info(f"🔒 Đã acquire database lock cho: {store_data.get('nama', 'Unknown')}")
            
            # Kiểm tra connection
            if self.connection.closed:
                logger.warning("🔄 Database connection bị đóng, reconnect...")
                self.connect()
            
            # Chỉ lưu cửa hàng có số điện thoại hợp lệ
            phone = store_data.get('phone', '')
            if not phone or phone in ['Not Found', 'Error', '']:
                logger.info(f"⏭️ Bỏ qua cửa hàng không có số điện thoại: {store_data.get('nama', 'Unknown')}")
                return False  # Không lưu cửa hàng không có số điện thoại
            
            # Kiểm tra trùng lặp theo số điện thoại
            logger.info(f"🔍 Kiểm tra phone exists: {phone}")
            if self.phone_exists(phone):
                existing_store = self.get_store_by_phone(phone)
                logger.info(f"📞 Số điện thoại đã tồn tại: {phone} - {existing_store['name'] if existing_store else 'Unknown'}")
                return False  # Không lưu để tránh trùng lặp
            
            # Tạo ID mới để tránh conflict
            import hashlib
            import random
            original_id = store_data['id']
            unique_id = f"{original_id}_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
            store_data['id'] = unique_id
            
            logger.info(f"🔄 Chuẩn bị insert với ID: {unique_id}")
            
            cursor = self.connection.cursor()
            
            # Sử dụng UPSERT để tránh lỗi duplicate key
            insert_sql = """
            INSERT INTO stores (id, name, rating, link, phone, address, website, plus_code, search_keyword, search_location, crawl_session)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                rating = EXCLUDED.rating,
                link = EXCLUDED.link,
                phone = EXCLUDED.phone,
                address = EXCLUDED.address,
                website = EXCLUDED.website,
                plus_code = EXCLUDED.plus_code,
                search_keyword = EXCLUDED.search_keyword,
                search_location = EXCLUDED.search_location,
                crawl_session = EXCLUDED.crawl_session,
                updated_at = CURRENT_TIMESTAMP
            """
            
            cursor.execute(insert_sql, (
                store_data['id'],
                store_data['nama'],
                store_data['rating'],
                store_data['link'],
                store_data['phone'],
                store_data['address'],
                store_data['website'],
                store_data['plus_code'],
                store_data.get('search_keyword', ''),
                store_data.get('search_location', ''),
                store_data.get('crawl_session', '')
            ))
            
            logger.info(f"🔄 Đã execute SQL insert cho: {store_data['nama']}")
            
            self.connection.commit()
            logger.info(f"🔄 Đã commit transaction")
            
            cursor.close()
            
            logger.info(f"✅ Đã lưu cửa hàng mới: {store_data['nama'][:30]}... (ID: {unique_id})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Lỗi lưu cửa hàng: {e}")
            logger.error(f"   Store data: {store_data}")
            try:
                self.connection.rollback()  # Rollback transaction khi có lỗi
            except:
                pass
            try:
                cursor.close()
            except:
                pass
            return False
        finally:
            # Luôn release lock trong finally
            try:
                self.lock.release()
                logger.info(f"🔓 Đã release database lock cho: {store_data.get('nama', 'Unknown')}")
            except:
                pass
    
    def insert_stores_batch(self, stores_data, search_keyword="", search_location=""):
        """Thêm nhiều cửa hàng cùng lúc"""
        try:
            cursor = self.connection.cursor()
            
            insert_sql = """
            INSERT INTO stores (id, name, rating, link, phone, address, website, plus_code, search_keyword, search_location)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                rating = EXCLUDED.rating,
                link = EXCLUDED.link,
                phone = EXCLUDED.phone,
                address = EXCLUDED.address,
                website = EXCLUDED.website,
                plus_code = EXCLUDED.plus_code,
                search_keyword = EXCLUDED.search_keyword,
                search_location = EXCLUDED.search_location,
                updated_at = CURRENT_TIMESTAMP
            """
            
            # Chuẩn bị dữ liệu
            data_to_insert = []
            for store in stores_data:
                data_to_insert.append((
                    store['id'],
                    store['nama'],
                    store['rating'],
                    store['link'],
                    store['phone'],
                    store['address'],
                    store['website'],
                    store['plus_code'],
                    search_keyword,
                    search_location
                ))
            
            cursor.executemany(insert_sql, data_to_insert)
            self.connection.commit()
            cursor.close()
            
            logger.info(f"✅ Đã lưu {len(stores_data)} cửa hàng vào database")
            return True
            
        except Exception as e:
            logger.error(f"❌ Lỗi lưu batch cửa hàng: {e}")
            return False
    
    def get_stores_by_search(self, search_keyword="", search_location=""):
        """Lấy danh sách cửa hàng theo từ khóa tìm kiếm"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            if search_keyword and search_location:
                sql = """
                SELECT * FROM stores 
                WHERE search_keyword ILIKE %s AND search_location ILIKE %s
                ORDER BY created_at DESC
                """
                cursor.execute(sql, (f"%{search_keyword}%", f"%{search_location}%"))
            else:
                sql = "SELECT * FROM stores ORDER BY created_at DESC LIMIT 100"
                cursor.execute(sql)
            
            results = cursor.fetchall()
            cursor.close()
            
            logger.info(f"✅ Lấy được {len(results)} cửa hàng từ database")
            return results
            
        except Exception as e:
            logger.error(f"❌ Lỗi lấy dữ liệu: {e}")
            return []
    
    def get_store_count(self):
        """Đếm tổng số cửa hàng trong database"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM stores")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            logger.error(f"❌ Lỗi đếm cửa hàng: {e}")
            return 0
    
    def close(self):
        """Đóng kết nối database"""
        if self.connection:
            self.connection.close()
            logger.info("🔚 Đã đóng kết nối database")
