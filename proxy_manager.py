#!/usr/bin/env python3
"""
Proxy Manager cho Google Maps Crawler
Quản lý xoay proxy và retry logic
"""

import random
import time
import logging
from config import PROXY_HOST, PROXY_PORT, PROXY_USERNAME, PROXY_PASSWORD, PROXY_RETRY_COUNT

logger = logging.getLogger(__name__)

class ProxyManager:
    """Quản lý proxy rotation và retry logic"""
    
    def __init__(self):
        self.proxy_list = [
            {
                'host': PROXY_HOST,
                'port': PROXY_PORT,
                'username': PROXY_USERNAME,
                'password': PROXY_PASSWORD
            }
        ]
        self.current_proxy_index = 0
        self.failed_proxies = set()
        self.retry_count = 0
    
    def get_current_proxy(self):
        """Lấy proxy hiện tại"""
        if not self.proxy_list:
            return None
        
        # Lọc bỏ các proxy đã fail
        available_proxies = [p for p in self.proxy_list if f"{p['host']}:{p['port']}" not in self.failed_proxies]
        
        if not available_proxies:
            logger.warning("⚠️ Tất cả proxy đã fail, reset danh sách")
            self.failed_proxies.clear()
            available_proxies = self.proxy_list
        
        # Chọn proxy ngẫu nhiên từ danh sách available
        proxy = random.choice(available_proxies)
        logger.info(f"🔒 Sử dụng proxy: {proxy['host']}:{proxy['port']}")
        return proxy
    
    def has_working_proxy(self):
        """Kiểm tra xem còn proxy nào hoạt động không"""
        available_proxies = [p for p in self.proxy_list if f"{p['host']}:{p['port']}" not in self.failed_proxies]
        return len(available_proxies) > 0
    
    def mark_proxy_failed(self, proxy):
        """Đánh dấu proxy đã fail"""
        proxy_key = f"{proxy['host']}:{proxy['port']}"
        self.failed_proxies.add(proxy_key)
        logger.warning(f"❌ Proxy {proxy_key} đã fail")
    
    def add_proxy(self, host, port, username, password):
        """Thêm proxy mới vào danh sách"""
        proxy = {
            'host': host,
            'port': port,
            'username': username,
            'password': password
        }
        self.proxy_list.append(proxy)
        logger.info(f"➕ Đã thêm proxy: {host}:{port}")
    
    def get_proxy_string(self, proxy):
        """Tạo proxy string cho Chrome"""
        if not proxy:
            return None
        return f"{proxy['host']}:{proxy['port']}"
    
    def get_proxy_auth(self, proxy):
        """Tạo proxy auth string cho Chrome"""
        if not proxy:
            return None
        return f"{proxy['username']}:{proxy['password']}"
    
    def should_retry(self):
        """Kiểm tra có nên retry không"""
        return self.retry_count < PROXY_RETRY_COUNT
    
    def increment_retry(self):
        """Tăng retry count"""
        self.retry_count += 1
    
    def reset_retry(self):
        """Reset retry count"""
        self.retry_count = 0
    
    def get_retry_delay(self):
        """Tính delay cho retry (exponential backoff)"""
        base_delay = 2
        max_delay = 30
        delay = min(base_delay * (2 ** self.retry_count), max_delay)
        return delay + random.uniform(0, 1)  # Thêm random để tránh thundering herd

# Global proxy manager instance
proxy_manager = ProxyManager()

