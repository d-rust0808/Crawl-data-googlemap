import pandas as pd
import time
import logging
from datetime import datetime
from run_program import get_user_input, build_search_url, scrape_store_details
from function import Scrap_data, opened_link_chroome
from database import DatabaseHandler

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchCrawler:
    """Crawler batch cho nhiều từ khóa và địa điểm"""
    
    def __init__(self):
        self.db = DatabaseHandler()
        self.stats = {
            'total_jobs': 0,
            'completed_jobs': 0,
            'total_stores': 0,
            'new_stores': 0,
            'duplicate_stores': 0,
            'start_time': None,
            'end_time': None
        }
    
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
    
    def run_batch_crawl(self, jobs):
        """Chạy batch crawl cho tất cả jobs"""
        self.stats['total_jobs'] = len(jobs)
        self.stats['start_time'] = datetime.now()
        
        logger.info(f"🚀 Bắt đầu batch crawl {len(jobs)} jobs...")
        
        # Tạo session ID duy nhất cho batch này
        batch_session = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        driver = None
        try:
            for i, job in enumerate(jobs):
                try:
                    logger.info(f"\n📋 === JOB {i+1}/{len(jobs)}: '{job['keyword']}' tại '{job['location']}' ===")
                    
                    # Tạo URL
                    search_url = build_search_url(job['keyword'], job['location'])
                    logger.info(f"🌐 URL: {search_url}")
                    
                    # Khởi tạo driver nếu chưa có
                    if not driver:
                        driver = opened_link_chroome(search_url)
                    else:
                        driver.get(search_url)
                    
                    # Scrape danh sách cửa hàng
                    logger.info("📋 Đang scrape danh sách cửa hàng...")
                    df = Scrap_data(driver)
                    
                    if df.empty:
                        logger.warning(f"⚠️ Không tìm thấy cửa hàng nào cho '{job['keyword']}' tại '{job['location']}'")
                        job['status'] = 'no_results'
                        continue
                    
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
                            
                            # Scrape chi tiết
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
                            
                            # Lưu vào database
                            try:
                                success = self.db.insert_store(store_data)
                                if success:
                                    job_new_stores += 1
                                    self.stats['new_stores'] += 1
                                    logger.info(f"✅ Cửa hàng mới: {row['nama'][:30]}...")
                                else:
                                    job_duplicate_stores += 1
                                    self.stats['duplicate_stores'] += 1
                                    logger.info(f"⏭️ Cửa hàng trùng lặp: {row['nama'][:30]}...")
                            except Exception as db_error:
                                logger.warning(f"⚠️ Lỗi lưu database: {db_error}")
                            
                            # Nghỉ một chút
                            time.sleep(1)
                            
                        except KeyboardInterrupt:
                            logger.info("⏹️ Người dùng dừng chương trình")
                            return
                        except Exception as e:
                            logger.warning(f"⚠️ Lỗi xử lý cửa hàng: {e}")
                            continue
                    
                    # Cập nhật kết quả job
                    job['status'] = 'completed'
                    job['stores_found'] = len(df)
                    job['new_stores'] = job_new_stores
                    job['duplicate_stores'] = job_duplicate_stores
                    
                    self.stats['completed_jobs'] += 1
                    self.stats['total_stores'] += len(df)
                    
                    logger.info(f"✅ Hoàn thành job {i+1}: {job_new_stores} mới, {job_duplicate_stores} trùng lặp")
                    
                    # Nghỉ giữa các jobs
                    if i < len(jobs) - 1:
                        logger.info("⏸️ Nghỉ 3 giây trước job tiếp theo...")
                        time.sleep(3)
                    
                except Exception as job_error:
                    logger.error(f"❌ Lỗi job {i+1}: {job_error}")
                    job['status'] = 'error'
                    job['error'] = str(job_error)
                    continue
        
        except Exception as e:
            logger.error(f"❌ Lỗi nghiêm trọng trong batch crawl: {e}")
        
        finally:
            if driver:
                try:
                    driver.quit()
                    logger.info("🔚 Đã đóng driver")
                except:
                    pass
        
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
        
        # Thống kê database
        total_in_db = self.db.get_store_count()
        print(f"🗄️ Tổng cửa hàng trong database: {total_in_db}")

def main():
    """Hàm main cho batch crawler"""
    crawler = BatchCrawler()
    
    print("🔍 === BATCH CRAWLER ===")
    print("Chọn file jobs:")
    print("1. list_jobs.txt (mặc định)")
    print("2. Nhập đường dẫn file khác")
    
    choice = input("Lựa chọn (1/2): ").strip()
    
    if choice == "1":
        file_path = "list_jobs.txt"
    elif choice == "2":
        file_path = input("📁 Đường dẫn file TXT: ").strip()
    else:
        print("❌ Lựa chọn không hợp lệ")
        return
    
    # Load jobs
    jobs = crawler.load_jobs_from_txt(file_path)
    
    if not jobs:
        print("❌ Không có job nào để crawl")
        return
    
    # Hiển thị danh sách jobs
    print(f"\n📋 Danh sách {len(jobs)} jobs:")
    for job in jobs:
        print(f"  {job['id']}. '{job['keyword']}' tại '{job['location']}' (tối đa {job['max_stores']})")
    
    # Xác nhận
    confirm = input(f"\n❓ Bắt đầu crawl {len(jobs)} jobs? (y/n): ").strip().lower()
    if confirm != 'y':
        print("👋 Hủy bỏ!")
        return
    
    # Chạy batch crawl
    results = crawler.run_batch_crawl(jobs)
    
    print(f"\n🎉 Hoàn thành batch crawl!")

if __name__ == "__main__":
    main()