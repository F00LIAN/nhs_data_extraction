"""
Stage 2 HTTP Client Module
Handles proxy requests and browser impersonation for URL fetching
"""

import asyncio
import curl_cffi
import numpy as np
import logging
from typing import Tuple


class HTTPClient:
    """Async HTTP client with proxy support and browser rotation"""
    
    def __init__(self, max_concurrent=10, delay_between_requests=0.5, max_retries=3):
        """
        Input: max_concurrent (int), delay_between_requests (float), max_retries (int)
        Output: None
        Description: Initialize HTTP client with concurrency limits and retry settings
        """
        self.max_concurrent = max_concurrent
        self.delay_between_requests = delay_between_requests
        self.max_retries = max_retries
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Browser rotation for resilience
        self.impersonations = ["chrome_android", "safari_ios", "chrome", "safari", "firefox"]
        self.current_working_browser = "chrome"
        self.browser_lock = asyncio.Lock()
        
        # California rotating proxy
        # TEMPORARILY COMMENTED OUT
        # self.proxy = {
        #     'https': 'http://35d146gk4kq9otn-country-us-state-california:z51vlpkz84emlb9@rp.scrapegw.com:6060'
        # }
        self.proxy = None  # Proxy disabled
    
    async def fetch_url(self, url: str, impersonation: str = "auto") -> Tuple[int, str]:
        """
        Fetch URL with proxy and browser impersonation.
        
        Input: url (str), impersonation (str) - "auto" uses current working browser
        Output: Tuple[status_code (int), html_content (str)]
        Description: Fetches URL with automatic browser rotation on failures and exponential backoff
        """
        async with self.semaphore:
            last_exception = None
            
            # Determine browser to use
            if impersonation == "auto":
                async with self.browser_lock:
                    current_impersonation = self.current_working_browser
            else:
                current_impersonation = impersonation
            
            browsers_tried = []
            impersonation_idx = self._get_browser_index(current_impersonation)
            
            for attempt in range(self.max_retries + 1):
                try:
                    # Apply delays and backoff
                    if attempt > 0:
                        backoff_delay = min(2 ** attempt, 30)  # Cap at 30 seconds
                        logging.info(f"🔄 Retry {attempt} for {url}, waiting {backoff_delay}s...")
                        await asyncio.sleep(backoff_delay)
                    else:
                        await asyncio.sleep(self.delay_between_requests)
                    
                    browsers_tried.append(current_impersonation)
                    
                    # Make HTTP request
                    # PROXY TEMPORARILY DISABLED
                    response = await asyncio.to_thread(
                        curl_cffi.get,
                        url,
                        impersonate=current_impersonation,
                        # proxies=self.proxy,  # PROXY DISABLED
                        timeout=30,
                        verify=False
                    )
                    
                    logging.info(f"🌐 Fetched {url} → Status: {response.status_code} (using {current_impersonation})")
                    
                    # Handle successful responses
                    if 200 <= response.status_code < 300:
                        await self._update_working_browser(current_impersonation, impersonation == "auto")
                        if attempt > 0:
                            logging.info(f"✅ Request succeeded on attempt {attempt + 1}")
                        return response.status_code, response.text
                    
                    # Handle retryable errors
                    elif response.status_code in [403, 429, 500, 502, 503, 504]:
                        logging.warning(f"⚠️ HTTP {response.status_code} for {url} (attempt {attempt + 1})")
                        if attempt < self.max_retries:
                            current_impersonation = self._rotate_browser(impersonation_idx)
                            impersonation_idx = (impersonation_idx + 1) % len(self.impersonations)
                        else:
                            return response.status_code, ""
                        continue
                    else:
                        # Don't retry for other 4xx errors
                        logging.error(f"❌ HTTP {response.status_code} error - not retrying")
                        return response.status_code, ""
                        
                except Exception as e:
                    last_exception = e
                    logging.warning(f"⚠️ Request exception (attempt {attempt + 1}): {e}")
                    if attempt < self.max_retries:
                        current_impersonation = self._rotate_browser(impersonation_idx)
                        impersonation_idx = (impersonation_idx + 1) % len(self.impersonations)
            
            # All retries failed
            logging.error(f"❌ All {self.max_retries + 1} attempts failed for {url}")
            if last_exception:
                logging.error(f"❌ Last exception: {last_exception}")
            return 0, ""
    
    def _get_browser_index(self, browser: str) -> int:
        """Get index of browser in impersonations list"""
        return self.impersonations.index(browser) if browser in self.impersonations else 0
    
    def _rotate_browser(self, current_idx: int) -> str:
        """Rotate to next browser impersonation"""
        next_idx = (current_idx + 1) % len(self.impersonations)
        next_browser = self.impersonations[next_idx]
        logging.info(f"🔄 Switching to {next_browser}")
        return next_browser
    
    async def _update_working_browser(self, browser: str, is_auto: bool):
        """Update the current working browser if successful"""
        if is_auto and browser != self.current_working_browser:
            async with self.browser_lock:
                self.current_working_browser = browser
                logging.info(f"🎯 Updated working browser to {browser}")
