from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import datetime
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def opened_link_chroome(url_search):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_window_size(1920, 1080)
    
    logger.info(f"🌐 Đang mở URL: {url_search}")
    driver.get(url_search)
    
    # Chờ trang load
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    time.sleep(3)
    
    return driver
def Scrap_data(driver):
    logger.info("🔍 Bắt đầu scraping data từ Google Maps...")
    
    # Các selectors mới cho Google Maps hiện tại
    store_selectors = [
        "a.hfpxzc",  # Link cửa hàng chính
        "a[aria-label*='·']",  # Cửa hàng có dấu ·
        "div[role='main'] a[jsaction]",  # Link trong main area
        "div[data-value] a[href*='/place/']",  # Link đến place
        "a[jslog*='track:click']",  # Có jslog track click
        "div[class*='Nv2PK'] a",  # Link trong container Nv2PK
        "div[class*='Q2HXcd'] a",  # Link trong container Q2HXcd
    ]
    
    # Tìm elements để scroll
    scroll_elements = []
    for selector in store_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                scroll_elements.extend(elements)
                logger.info(f"✅ Tìm thấy {len(elements)} elements với selector: {selector}")
        except Exception as e:
            logger.warning(f"⚠️ Lỗi với selector {selector}: {e}")
    
    if not scroll_elements:
        logger.warning("⚠️ Không tìm thấy elements để scroll, thử cách khác...")
        # Fallback: tìm tất cả thẻ a
        scroll_elements = driver.find_elements(By.TAG_NAME, "a")
        logger.info(f"📋 Tìm thấy {len(scroll_elements)} thẻ <a>")
    
    action = ActionChains(driver)
    scroll_count = 0
    max_scrolls = 10
    
    while scroll_count < max_scrolls:
        try:
            logger.info(f"📜 Scroll lần {scroll_count + 1}/{max_scrolls}")
            
            # Scroll xuống
            if scroll_elements:
                last_element = scroll_elements[-1]
                Scroll_origin = ScrollOrigin.from_element(last_element)
                action.scroll_from_origin(Scroll_origin, 0, 1000).perform()
            else:
                # Fallback scroll
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            time.sleep(2)
            
            # Kiểm tra xem có thêm elements mới không
            new_elements = []
            for selector in store_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    new_elements.extend(elements)
                except:
                    continue
            
            if len(new_elements) > len(scroll_elements):
                scroll_elements = new_elements
                logger.info(f"🔄 Tìm thấy thêm elements, tổng: {len(scroll_elements)}")
            else:
                logger.info("✅ Không có thêm elements mới, dừng scroll")
                break
                
            scroll_count += 1
            
        except Exception as e:
            logger.warning(f"⚠️ Lỗi khi scroll: {e}")
            break
    
    # Parse HTML và extract data
    content = driver.page_source
    data = BeautifulSoup(content, 'html.parser')
    
    logger.info("📋 Bắt đầu parse data...")
    res = []
    
    # Các selectors để tìm thông tin cửa hàng
    store_containers = [
        "div[class*='Nv2PK']",
        "div[class*='Q2HXcd']", 
        "div[class*='THOPZb']",
        "div[role='main'] > div",
        "div[data-value]"
    ]
    
    for container_selector in store_containers:
        try:
            containers = data.find_all('div', class_=lambda x: x and any(cls in x for cls in ['Nv2PK', 'Q2HXcd', 'THOPZb']))
            logger.info(f"🔍 Tìm thấy {len(containers)} containers với selector: {container_selector}")
            
            for i, area in enumerate(containers):
                try:
                    # Khởi tạo biến link trước
                    link = "Link Not Found"
                    
                    # Tìm link trước
                    link_selectors = [
                        "a[href*='/maps/place/']",
                        "a[href*='google.com/maps']",
                        "a[data-value]",
                        "a[jsaction*='pane']"
                    ]
                    
                    for link_selector in link_selectors:
                        try:
                            link_elem = area.select_one(link_selector)
                            if link_elem and link_elem.get('href'):
                                link = link_elem.get('href')
                                break
                        except:
                            continue
                    
                    # Tạo ID duy nhất dựa trên link thay vì timestamp
                    import hashlib
                    if link != "Link Not Found":
                        store_id = hashlib.md5(link.encode()).hexdigest()[:16]
                    else:
                        # Fallback nếu không có link
                        current_datetime = datetime.datetime.now()
                        merge_date = current_datetime.strftime("%Y%m%d%H%M%S%f")
                        store_id = f"{merge_date}{i+1}"
                    
                    # Tìm tên cửa hàng
                    nama = "Nama Not Found"
                    name_selectors = [
                        "div[class*='qBF1Pd']",
                        "div[class*='fontHeadlineSmall']", 
                        "h1", "h2", "h3",
                        "span[class*='fontHeadlineSmall']",
                        "div[class*='fontBodyMedium']"
                    ]
                    
                    for name_selector in name_selectors:
                        try:
                            name_elem = area.select_one(name_selector)
                            if name_elem and name_elem.get_text().strip():
                                nama = name_elem.get_text().strip()
                                break
                        except:
                            continue
                    
                    # Tìm rating
                    rating = "Rating Not Found"
                    rating_selectors = [
                        "span[class*='MW4etd']",
                        "span[class*='fontBodyMedium']",
                        "div[class*='fontBodyMedium']",
                        "span[class*='rating']"
                    ]
                    
                    for rating_selector in rating_selectors:
                        try:
                            rating_elem = area.select_one(rating_selector)
                            if rating_elem and rating_elem.get_text().strip():
                                rating_text = rating_elem.get_text().strip()
                                if any(char.isdigit() for char in rating_text):
                                    rating = rating_text
                                    break
                        except:
                            continue
                    
                    
                    # Chỉ thêm nếu có ít nhất tên hoặc link
                    if nama != "Nama Not Found" or link != "Link Not Found":
                        res.append({
                            'id': store_id, 
                            'nama': nama, 
                            'rating': rating, 
                            'link': link
                        })
                        logger.info(f"✅ Tìm thấy cửa hàng: {nama[:50]}...")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Lỗi khi parse cửa hàng {i+1}: {e}")
                    continue
                    
        except Exception as e:
            logger.warning(f"⚠️ Lỗi với container selector {container_selector}: {e}")
            continue
    
    # Loại bỏ duplicate
    unique_res = []
    seen_links = set()
    
    for item in res:
        if item['link'] not in seen_links:
            unique_res.append(item)
            seen_links.add(item['link'])
    
    logger.info(f"🎉 Hoàn thành scraping! Tìm thấy {len(unique_res)} cửa hàng")
    
    df = pd.DataFrame(unique_res)
    return df