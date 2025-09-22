import pandas as pd
import time
import logging
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from run_program import get_user_input, build_search_url, scrape_store_details
from function import Scrap_data, opened_link_chroome
from database import DatabaseHandler
from config import MAX_WORKERS, THREAD_DELAY

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchCrawler:
    """Crawler batch cho nhiều từ khóa và địa điểm - Hỗ trợ đa luồng"""
    
    def __init__(self):
        self.db = DatabaseHandler()
        self.stats = {
            'total_jobs': 0,
            'completed_jobs': 0,
            'total_stores': 0,
            'new_stores': 0,
            'duplicate_stores': 0,
            'cached_stores': 0,  # Thêm thống kê cache
            'start_time': None,
            'end_time': None
        }
        self.stats_lock = threading.Lock()  # Thread lock cho stats
        
        # Cache RAM để tránh scrape lại cửa hàng đã tìm thấy
        self.store_cache = {}  # {store_link: store_data}
        self.cache_lock = threading.Lock()  # Thread lock cho cache
    
    def get_cached_store(self, store_link):
        """Lấy cửa hàng từ cache nếu có"""
        with self.cache_lock:
            return self.store_cache.get(store_link)
    
    def cache_store(self, store_link, store_data):
        """Lưu cửa hàng vào cache"""
        with self.cache_lock:
            self.store_cache[store_link] = store_data
    
    def load_jobs_from_txt(self, file_path):
        """Load danh sách job từ file TXT - format: keyword|location|max_stores"""
        try:
            jobs = []
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            for i, line in enumerate(lines):
                line = line.strip()
                if not line or line.startswith('#'):  # Bỏ qua dòng trống và comment
                    continue
                
                parts = line.split('|')
                if len(parts) >= 2:
                    job = {
                        'id': len(jobs) + 1,
                        'keyword': parts[0].strip(),
                        'location': parts[1].strip(),
                        'max_stores': int(parts[2].strip()) if len(parts) > 2 and parts[2].strip().isdigit() else 50,
                        'status': 'pending'
                    }
                    jobs.append(job)
            
            logger.info(f"✅ Đã load {len(jobs)} jobs từ file TXT")
            return jobs
            
        except Exception as e:
            logger.error(f"❌ Lỗi load file TXT: {e}")
            return []
    
    def process_single_job(self, job, batch_session):
        """Xử lý một job đơn lẻ - Thread Safe"""
        try:
            logger.info(f"📋 === JOB {job['id']}: '{job['keyword']}' tại '{job['location']}' ===")
            
            # Tạo URL
            search_url = build_search_url(job['keyword'], job['location'])
            logger.info(f"🌐 URL: {search_url}")
            
            # Khởi tạo driver - thử không proxy trước
            try:
                logger.info("🔄 Thử khởi tạo driver không proxy trước...")
                driver = opened_link_chroome(search_url, use_proxy=False)
            except Exception as driver_error:
                logger.warning(f"⚠️ Lỗi khởi tạo driver không proxy: {driver_error}")
                logger.info("🔄 Thử khởi tạo driver với proxy...")
                try:
                    driver = opened_link_chroome(search_url, use_proxy=True)
                except Exception as proxy_error:
                    logger.error(f"❌ Lỗi khởi tạo driver với proxy: {proxy_error}")
                    raise
            
            try:
                # Scrape danh sách cửa hàng
                logger.info("📋 Đang scrape danh sách cửa hàng...")
                df = Scrap_data(driver)
                
                if df.empty:
                    logger.warning(f"⚠️ Không tìm thấy cửa hàng nào cho '{job['keyword']}' tại '{job['location']}'")
                    job['status'] = 'no_results'
                    return job
                
                logger.info(f"✅ Tìm thấy {len(df)} cửa hàng")
                
                # Giới hạn số lượng
                if job['max_stores'] > 0 and len(df) > job['max_stores']:
                    df = df.head(job['max_stores'])
                    logger.info(f"🔢 Giới hạn: {job['max_stores']} cửa hàng")
                
                # Xử lý từng cửa hàng
                job_new_stores = 0
                job_duplicate_stores = 0
                
                for index, row in df.iterrows():
                    try:
                        logger.info(f"📝 Đang xử lý cửa hàng {index+1}/{len(df)}: {row['nama'][:30]}...")
                        
                        # Kiểm tra cache trước
                        store_link = row['link']
                        cached_store = self.get_cached_store(store_link)
                        
                        if cached_store:
                            logger.info(f"💾 Sử dụng cache cho: {row['nama'][:30]}...")
                            details = {
                                'phone': cached_store.get('phone', 'Not Found'),
                                'address': cached_store.get('address', 'Not Found'),
                                'website': cached_store.get('website', 'Not Found'),
                                'plus_code': cached_store.get('plus_code', 'Not Found')
                            }
                            # Cập nhật thống kê cache
                            with self.stats_lock:
                                self.stats['cached_stores'] += 1
                        else:
                            # Scrape chi tiết nếu chưa có trong cache
                            try:
                                details = scrape_store_details(driver, row['link'])
                            except Exception as scrape_error:
                                logger.warning(f"⚠️ Lỗi scrape chi tiết: {scrape_error}")
                                details = {
                                    'phone': 'Error',
                                    'address': 'Error',
                                    'website': 'Error',
                                    'plus_code': 'Error'
                                }
                        
                        # Tạo dữ liệu cửa hàng
                        store_data = {
                            'id': row['id'],
                            'nama': row['nama'],
                            'rating': row['rating'],
                            'link': row['link'],
                            'phone': details['phone'],
                            'address': details['address'],
                            'website': details['website'],
                            'plus_code': details['plus_code'],
                            'search_keyword': job['keyword'],
                            'search_location': job['location'],
                            'crawl_session': batch_session
                        }
                        
                        # Lưu vào cache nếu chưa có
                        if not cached_store:
                            self.cache_store(store_link, {
                                'phone': details['phone'],
                                'address': details['address'],
                                'website': details['website'],
                                'plus_code': details['plus_code']
                            })
                        
                        # Lưu vào database
                        try:
                            logger.info(f"💾 Đang lưu cửa hàng vào database: {row['nama'][:30]}...")
                            logger.info(f"🔍 DEBUG store_data keys: {list(store_data.keys())}")
                            logger.info(f"🔍 DEBUG store_data phone: '{store_data.get('phone', 'N/A')}'")
                            logger.info(f"🔍 DEBUG store_data nama: '{store_data.get('nama', 'N/A')}'")
                            
                            success = self.db.insert_store(store_data)
                            
                            logger.info(f"🔍 DEBUG insert_store returned: {success}")
                            
                            if success:
                                job_new_stores += 1
                                with self.stats_lock:
                                    self.stats['new_stores'] += 1
                                logger.info(f"✅ Cửa hàng mới: {row['nama'][:30]}...")
                            else:
                                job_duplicate_stores += 1
                                with self.stats_lock:
                                    self.stats['duplicate_stores'] += 1
                                logger.info(f"⏭️ Cửa hàng bị skip (trùng số điện thoại hoặc không có số điện thoại): {row['nama'][:30]}...")
                        except Exception as db_error:
                            logger.error(f"❌ Lỗi lưu database: {db_error}")
                            logger.error(f"   Store data: {store_data}")
                            import traceback
                            logger.error(f"   Traceback: {traceback.format_exc()}")
                        
                        # Nghỉ một chút giữa các cửa hàng để tránh bị chặn
                        time.sleep(1.0)  # Tăng lên 1s để tránh bị chặn
                        
                    except Exception as e:
                        logger.warning(f"⚠️ Lỗi xử lý cửa hàng: {e}")
                        continue
                
                # Cập nhật kết quả job
                job['status'] = 'completed'
                job['stores_found'] = len(df)
                job['new_stores'] = job_new_stores
                job['duplicate_stores'] = job_duplicate_stores
                
                with self.stats_lock:
                    self.stats['completed_jobs'] += 1
                    self.stats['total_stores'] += len(df)
                
                logger.info(f"✅ Hoàn thành job {job['id']}: {job_new_stores} mới, {job_duplicate_stores} trùng lặp")
                
            finally:
                # Đóng driver
                try:
                    driver.quit()
                    logger.info(f"🔚 Đã đóng driver cho job {job['id']}")
                except:
                    pass
            
            return job
            
        except Exception as job_error:
            logger.error(f"❌ Lỗi job {job['id']}: {job_error}")
            job['status'] = 'error'
            job['error'] = str(job_error)
            return job
    
    def run_batch_crawl(self, jobs):
        """Chạy batch crawl cho tất cả jobs - Hỗ trợ đa luồng"""
        self.stats['total_jobs'] = len(jobs)
        self.stats['start_time'] = datetime.now()
        
        logger.info(f"🚀 Bắt đầu batch crawl {len(jobs)} jobs với {MAX_WORKERS} luồng...")
        
        # Tạo session ID duy nhất cho batch này
        batch_session = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            # Sử dụng ThreadPoolExecutor để chạy đa luồng
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit tất cả jobs
                future_to_job = {
                    executor.submit(self.process_single_job, job, batch_session): job 
                    for job in jobs
                }
                
                # Xử lý kết quả khi hoàn thành
                for future in as_completed(future_to_job):
                    job = future_to_job[future]
                    try:
                        result = future.result()
                        logger.info(f"✅ Job {result['id']} hoàn thành: {result['status']}")
                        
                        # Thêm delay giữa các job để tránh bị chặn
                        if MAX_WORKERS == 1:  # Chỉ delay khi chạy 1 luồng
                            logger.info(f"⏳ Chờ {THREAD_DELAY}s trước job tiếp theo...")
                            time.sleep(THREAD_DELAY)
                            
                    except Exception as exc:
                        logger.error(f"❌ Job {job['id']} lỗi: {exc}")
                        job['status'] = 'error'
                        job['error'] = str(exc)
        
        except KeyboardInterrupt:
            logger.info("⏹️ Người dùng dừng chương trình")
            return jobs
        except Exception as e:
            logger.error(f"❌ Lỗi nghiêm trọng trong batch crawl: {e}")
        
        # Kết thúc
        self.stats['end_time'] = datetime.now()
        self._print_final_stats()
        
        return jobs
    
    def _print_final_stats(self):
        """In thống kê cuối cùng"""
        duration = self.stats['end_time'] - self.stats['start_time']
        
        print(f"\n🎉 === KẾT QUẢ BATCH CRAWL ===")
        print(f"⏱️ Thời gian: {duration}")
        print(f"📋 Jobs hoàn thành: {self.stats['completed_jobs']}/{self.stats['total_jobs']}")
        print(f"🏪 Tổng cửa hàng tìm thấy: {self.stats['total_stores']}")
        print(f"🆕 Cửa hàng mới: {self.stats['new_stores']}")
        print(f"🔄 Cửa hàng trùng lặp: {self.stats['duplicate_stores']}")
        print(f"💾 Cửa hàng từ cache: {self.stats['cached_stores']}")
        print(f"📊 Cache size: {len(self.store_cache)} cửa hàng")
        
        # Thống kê database
        total_in_db = self.db.get_store_count()
        print(f"🗄️ Tổng cửa hàng trong database: {total_in_db}")

def main():
    """Hàm main cho batch crawler"""
    crawler = BatchCrawler()
    
    print("🔍 === BATCH CRAWLER ===")
    print("🚀 Tự động chạy với list_jobs.txt...")
    
    # Load jobs từ file mặc định
    file_path = "list_jobs.txt"
    jobs = crawler.load_jobs_from_txt(file_path)
    
    if not jobs:
        print("❌ Không có job nào để crawl")
        return
    
    # Hiển thị danh sách jobs
    print(f"\n📋 Danh sách {len(jobs)} jobs:")
    for job in jobs:
        print(f"  {job['id']}. '{job['keyword']}' tại '{job['location']}' (tối đa {job['max_stores']})")
    
    print(f"\n🚀 Bắt đầu crawl {len(jobs)} jobs...")
    
    # Chạy batch crawl
    results = crawler.run_batch_crawl(jobs)
    
    print(f"\n🎉 Hoàn thành batch crawl!")

if __name__ == "__main__":
    main()