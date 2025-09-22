from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.proxy import Proxy, ProxyType
import time
import datetime
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import logging
import threading
import os
import zipfile
import tempfile
from config import PROXY_HOST, PROXY_PORT, PROXY_USERNAME, PROXY_PASSWORD, PROXY_RETRY_COUNT
from proxy_manager import proxy_manager

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_proxy_auth_extension(proxy_auth):
    """Tạo Chrome extension để xử lý proxy authentication"""
    if not proxy_auth:
        return None
    
    username, password = proxy_auth.split(':')
    
    # Tạo manifest.json
    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Chrome Proxy Auth",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"]
        },
        "minimum_chrome_version":"22.0.0"
    }
    """
    
    # Tạo background.js
    background_js = f"""
    var config = {{
        mode: "fixed_servers",
        rules: {{
            singleProxy: {{
                scheme: "http",
                host: "{PROXY_HOST}",
                port: parseInt({PROXY_PORT})
            }},
            bypassList: ["localhost"]
        }}
    }};

    chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});

    function callbackFn(details) {{
        return {{
            authCredentials: {{
                username: "{username}",
                password: "{password}"
            }}
        }};
    }}

    chrome.webRequest.onAuthRequired.addListener(
        callbackFn,
        {{urls: ["<all_urls>"]}},
        ['blocking']
    );
    """
    
    # Tạo extension zip file
    pluginfile = os.path.join(tempfile.gettempdir(), 'proxy_auth_plugin.zip')
    
    with zipfile.ZipFile(pluginfile, 'w') as zp:
        zp.writestr("manifest.json", manifest_json)
        zp.writestr("background.js", background_js)
    
    return pluginfile


def opened_link_chroome(url_search, use_proxy=True, retry_count=0):
    """
    Mở Chrome driver với proxy support và rotation
    """
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-plugins')
    options.add_argument('--disable-images')
    options.add_argument('--disable-web-security')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--disable-features=VizDisplayCompositor')
    options.add_argument('--disable-ipc-flooding-protection')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    # Thêm stealth options
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Thêm proxy nếu được yêu cầu
    current_proxy = None
    if use_proxy:
        current_proxy = proxy_manager.get_current_proxy()
        if current_proxy:
            proxy_string = proxy_manager.get_proxy_string(current_proxy)
            proxy_auth = proxy_manager.get_proxy_auth(current_proxy)
            
            # Sử dụng format đúng cho proxy authentication
            options.add_argument(f'--proxy-server=http://{proxy_string}')
            
            # Thêm proxy authentication extension
            try:
                extension_path = create_proxy_auth_extension(proxy_auth)
                options.add_extension(extension_path)
                logger.info(f"🔒 Sử dụng proxy: {proxy_string}")
            except Exception as ext_error:
                logger.error(f"❌ Lỗi tạo proxy extension: {ext_error}")
                raise Exception(f"❌ Không thể tạo proxy extension: {ext_error}")
        else:
            raise Exception("❌ BẮT BUỘC phải có proxy!")
    
    try:
        # Tải ChromeDriver trước (không qua proxy)
        logger.info("📥 Đang tải ChromeDriver...")
        service = Service(ChromeDriverManager().install())
        
        # Tạo driver
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_window_size(1920, 1080)
        
        # Thêm stealth JavaScript
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
        driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
        driver.execute_script("window.chrome = { runtime: {} }")
        
        logger.info(f"🌐 Đang mở URL: {url_search}")
        driver.get(url_search)
        
        # Chờ trang load
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(5)  # Tăng thời gian chờ
        
        # Debug: Kiểm tra title và URL
        try:
            title = driver.title
            current_url = driver.current_url
            logger.info(f"📄 Page title: {title}")
            logger.info(f"🔗 Current URL: {current_url}")
            
            # Kiểm tra xem có bị chặn không
            if "blocked" in title.lower() or "access denied" in title.lower() or "captcha" in title.lower():
                logger.warning("⚠️ Có thể bị chặn bởi Google Maps")
        except Exception as debug_error:
            logger.warning(f"⚠️ Lỗi debug: {debug_error}")
        
        # Reset retry count nếu thành công
        proxy_manager.reset_retry()
        return driver
        
    except Exception as e:
        logger.warning(f"⚠️ Lỗi khởi tạo driver: {e}")
        
        # Đánh dấu proxy fail nếu có
        if current_proxy:
            proxy_manager.mark_proxy_failed(current_proxy)
        
        # Retry logic - thử không proxy nếu proxy fail
        if proxy_manager.should_retry():
            proxy_manager.increment_retry()
            delay = proxy_manager.get_retry_delay()
            logger.info(f"🔄 Retry {proxy_manager.retry_count}/{PROXY_RETRY_COUNT} sau {delay:.1f}s...")
            time.sleep(delay)
            
            # Thử lại với proxy nếu còn retry
            if proxy_manager.retry_count < PROXY_RETRY_COUNT:
                return opened_link_chroome(url_search, use_proxy=use_proxy, retry_count=retry_count + 1)
            else:
                # Không cho phép chạy không proxy
                logger.error(f"❌ Proxy fail sau {PROXY_RETRY_COUNT} lần thử - BẮT BUỘC phải dùng proxy!")
                raise Exception("❌ BẮT BUỘC phải dùng proxy!")
        else:
            # Không cho phép chạy không proxy
            logger.error("❌ Proxy fail - BẮT BUỘC phải dùng proxy!")
            raise Exception("❌ BẮT BUỘC phải dùng proxy!")
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
        "a[href*='/maps/place/']",  # Link trực tiếp đến place
        "div[class*='THOPZb'] a",  # Link trong container THOPZb
        "div[class*='VkpGBb'] a",  # Link trong container VkpGBb
        "a[data-value]",  # Link có data-value
        "div[jsaction] a",  # Link trong div có jsaction
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
    max_scrolls = 10  # Giảm số lần scroll để tăng tốc độ
    
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
            
            time.sleep(1)  # Giảm thời gian chờ giữa các lần scroll
            
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
                    logger.debug(f"   Container HTML: {str(area)[:200]}...")
                    continue
                    
        except Exception as e:
            logger.warning(f"⚠️ Lỗi với container selector {container_selector}: {e}")
            continue
    
    # Loại bỏ duplicate dựa trên tên cửa hàng
    unique_res = []
    seen_names = set()
    duplicate_count = 0
    
    for item in res:
        # Chuẩn hóa tên để so sánh (bỏ dấu, chuyển thành chữ thường)
        import re
        normalized_name = re.sub(r'[^\w\s]', '', item['nama'].lower().strip())
        
        if normalized_name not in seen_names:
            unique_res.append(item)
            seen_names.add(normalized_name)
        else:
            duplicate_count += 1
            logger.debug(f"🔄 Bỏ qua duplicate: {item['nama'][:30]}... (tên đã có)")
    
    logger.info(f"🎉 Hoàn thành scraping! Tìm thấy {len(res)} cửa hàng, {duplicate_count} duplicate, {len(unique_res)} unique")
    
    df = pd.DataFrame(unique_res)
    return df