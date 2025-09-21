import pandas as pd
from bs4 import BeautifulSoup
from function import Scrap_data, opened_link_chroome
from database import DatabaseHandler
import logging
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def scrape_store_details(driver, store_link):
    """Scrape chi tiết cửa hàng từ link"""
    try:
        logger.info(f"🔍 Đang scrape chi tiết: {store_link[:50]}...")
        
        driver.get(store_link)
        time.sleep(2)  # Giảm thời gian chờ
        
        data = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Tìm thông tin chi tiết với selectors mới
        details = {
            'phone': 'Not Found',
            'address': 'Not Found', 
            'website': 'Not Found',
            'plus_code': 'Not Found'
        }
        
        # Debug: In ra tất cả text có thể
        all_text_elements = data.find_all(['div', 'span', 'a'], class_=lambda x: x and any(cls in str(x) for cls in ['Io6YTe', 'fontBodyMedium', 'fontBodySmall']))
        logger.info(f"🔍 Tìm thấy {len(all_text_elements)} text elements")
        
        # Tìm phone - tìm text có số điện thoại
        phone_patterns = [
            r'\+\d{1,3}[\s\-]?\d{1,4}[\s\-]?\d{1,4}[\s\-]?\d{1,4}',  # +62 123 456 789
            r'\d{3,4}[\s\-]?\d{3,4}[\s\-]?\d{3,4}',  # 123 456 789
            r'\(\d{3,4}\)[\s\-]?\d{3,4}[\s\-]?\d{3,4}',  # (123) 456 789
        ]
        
        import re
        for elem in all_text_elements:
            text = elem.get_text().strip()
            if text and len(text) > 5:
                for pattern in phone_patterns:
                    if re.search(pattern, text):
                        details['phone'] = text
                        logger.info(f"📞 Tìm thấy phone: {text}")
                        break
                if details['phone'] != 'Not Found':
                    break
        
        # Tìm address - tìm text dài có vẻ là địa chỉ
        for elem in all_text_elements:
            text = elem.get_text().strip()
            if (text and len(text) > 20 and len(text) < 200 and 
                not text.startswith(('Phone', 'Website', 'Hours', 'Reviews', 'Rating')) and
                not any(char.isdigit() for char in text[:5]) and
                ('Street' in text or 'Road' in text or 'Avenue' in text or 'Jl.' in text or 'Đường' in text)):
                details['address'] = text
                logger.info(f"📍 Tìm thấy address: {text}")
                break
        
        # Tìm website
        website_links = data.find_all('a', href=True)
        for link in website_links:
            href = link.get('href', '')
            if (href.startswith('http') and 
                'google.com' not in href and 
                'maps.google.com' not in href and
                not href.startswith('https://www.google.com/maps')):
                details['website'] = href
                logger.info(f"🌐 Tìm thấy website: {href}")
                break
        
        # Tìm plus code
        for elem in all_text_elements:
            text = elem.get_text().strip()
            if text and '+' in text and len(text) > 8 and len(text) < 20:
                details['plus_code'] = text
                logger.info(f"📍 Tìm thấy plus code: {text}")
                break
        
        logger.info(f"✅ Hoàn thành scrape chi tiết: {details}")
        return details
        
    except Exception as e:
        logger.warning(f"⚠️ Lỗi khi scrape chi tiết: {e}")
        return {
            'phone': 'Error',
            'address': 'Error',
            'website': 'Error', 
            'plus_code': 'Error'
        }

def get_user_input():
    """Lấy input từ người dùng"""
    print("🔍 === Google Maps Crawler ===")
    print("Nhập thông tin tìm kiếm:")
    
    # Từ khóa tìm kiếm
    search_keyword = input("📝 Từ khóa tìm kiếm (ví dụ: 'kursus stir mobil', 'bánh kem', 'nhà hàng'): ").strip()
    if not search_keyword:
        search_keyword = "kursus stir mobil"  # Default
        print(f"⚠️ Sử dụng từ khóa mặc định: {search_keyword}")
    
    # Vị trí tìm kiếm
    location = input("📍 Vị trí tìm kiếm (ví dụ: 'Jakarta', 'Ho Chi Minh City', 'Hanoi'): ").strip()
    if not location:
        location = "Jakarta"  # Default
        print(f"⚠️ Sử dụng vị trí mặc định: {location}")
    
    # Số lượng cửa hàng tối đa
    try:
        max_stores = input("🔢 Số lượng cửa hàng tối đa (Enter = không giới hạn): ").strip()
        max_stores = int(max_stores) if max_stores else 0
    except ValueError:
        max_stores = 0
        print("⚠️ Sử dụng không giới hạn số lượng")
    
    return search_keyword, location, max_stores

def build_search_url(keyword, location):
    """Tạo URL tìm kiếm Google Maps"""
    # Encode keyword và location
    import urllib.parse
    encoded_keyword = urllib.parse.quote(keyword)
    encoded_location = urllib.parse.quote(location)
    
    # Tạo URL tìm kiếm
    search_url = f"https://www.google.com/maps/search/{encoded_keyword}+in+{encoded_location}"
    return search_url

def main():
    logger.info("🚀 Bắt đầu Google Maps Crawler...")
    
    # Lấy input từ người dùng
    keyword, location, max_stores = get_user_input()
    
    # Tạo URL tìm kiếm
    search_url = build_search_url(keyword, location)
    logger.info(f"🔍 Tìm kiếm: '{keyword}' tại '{location}'")
    logger.info(f"🌐 URL: {search_url}")
    
    # Khởi tạo database
    db = DatabaseHandler()
    
    # Tạo driver
    driver = opened_link_chroome(search_url)
    
    try:
        # Scrape danh sách cửa hàng
        logger.info("📋 Đang scrape danh sách cửa hàng...")
        df = Scrap_data(driver)
        
        if df.empty:
            logger.warning("⚠️ Không tìm thấy cửa hàng nào!")
            return
        
        logger.info(f"✅ Tìm thấy {len(df)} cửa hàng")
        print(f"\n📊 Danh sách cửa hàng:")
        print(df[['nama', 'rating', 'link']].head())
        
        # Giới hạn số lượng cửa hàng nếu cần
        if max_stores > 0 and len(df) > max_stores:
            df = df.head(max_stores)
            logger.info(f"🔢 Giới hạn số lượng cửa hàng: {max_stores}")
        
        # Test với ít cửa hàng trước
        test_limit = min(5, len(df))  # Chỉ test 5 cửa hàng đầu tiên
        df_test = df.head(test_limit)
        logger.info(f"🧪 Test với {test_limit} cửa hàng đầu tiên")
        
        # Scrape chi tiết từng cửa hàng
        logger.info("🔍 Bắt đầu scrape chi tiết từng cửa hàng...")
        results = []
        new_stores = 0
        existing_stores = 0
        
        for index, row in df_test.iterrows():
            try:
                logger.info(f"📝 Đang xử lý cửa hàng {index+1}/{len(df_test)}: {row['nama'][:30]}...")
                
                # Scrape chi tiết với timeout
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
                
                # Tạo kết quả
                result = {
                    'id': row['id'],
                    'nama': row['nama'],
                    'rating': row['rating'],
                    'link': row['link'],
                    'phone': details['phone'],
                    'address': details['address'],
                    'website': details['website'],
                    'plus_code': details['plus_code'],
                    'search_keyword': keyword,
                    'search_location': location
                }
                
                results.append(result)
                
                # Lưu vào database ngay lập tức
                try:
                    success = db.insert_store(result)
                    if success:
                        new_stores += 1
                        logger.info(f"✅ Đã lưu cửa hàng mới: {result['nama'][:30]}...")
                    else:
                        existing_stores += 1
                        logger.info(f"⏭️ Cửa hàng đã tồn tại: {result['nama'][:30]}...")
                except Exception as db_error:
                    logger.warning(f"⚠️ Lỗi lưu database: {db_error}")
                
                logger.info(f"✅ Hoàn thành cửa hàng {index+1}")
                
                # Nghỉ một chút để tránh bị block
                time.sleep(1)
                
            except KeyboardInterrupt:
                logger.info("⏹️ Người dùng dừng chương trình")
            break
        # Hiển thị kết quả
        if results:
            kf = pd.DataFrame(results)
            
            # Tạo tên file output dựa trên keyword và location
            import re
            safe_keyword = re.sub(r'[^\w\s-]', '', keyword).strip()
            safe_location = re.sub(r'[^\w\s-]', '', location).strip()
            output_file = f"google_maps_{safe_keyword}_{safe_location}.xlsx"
            
            # Lưu vào Excel
            kf.to_excel(output_file, index=False)
            
            # Hiển thị thống kê database
            total_stores = db.get_store_count()
            
            logger.info(f"🎉 Hoàn thành! Đã xử lý {len(results)} cửa hàng")
            logger.info(f"📊 Cửa hàng mới: {new_stores}")
            logger.info(f"📊 Cửa hàng đã tồn tại: {existing_stores}")
            logger.info(f"📊 Tổng số cửa hàng trong database: {total_stores}")
            
            print(f"\n📊 Kết quả cuối cùng:")
            print(kf[['nama', 'rating', 'phone', 'address']].head())
            print(f"\n💾 File Excel: {output_file}")
            print(f"🗄️ Database: {total_stores} cửa hàng tổng cộng")
            print(f"🆕 Cửa hàng mới: {new_stores}")
            print(f"🔄 Cửa hàng đã tồn tại: {existing_stores}")
            
        else:
            logger.warning("⚠️ Không có kết quả nào được lưu!")
            
    except Exception as e:
        logger.error(f"❌ Lỗi nghiêm trọng: {e}")
        
    finally:
        # Đóng driver và database
        try:
            driver.quit()
            logger.info("🔚 Đã đóng driver")
        except:
            pass
    
        try:
            db.close()
        except:
            pass
    
if __name__ == "__main__":
    main()