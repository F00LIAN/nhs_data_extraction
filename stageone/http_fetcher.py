"""
HTTP Fetcher Module
Handles HTTP requests with retry logic and proxy support.

Input: URLs, request settings
Output: HTTP response content
Description: Async HTTP fetching with browser impersonation, proxy rotation, and comprehensive retry mechanisms.
"""

import asyncio
import curl_cffi
import logging
import time
from datetime import datetime
from typing import Tuple, Dict, List

class HttpFetcher:
    def __init__(self, max_concurrent: int = 5, delay_between_requests: float = 5.0):
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.delay_between_requests = delay_between_requests
        self.max_retries_per_url = 3
        self.retry_attempts = {}
        self.retry_queue = asyncio.Queue()
        self.failure_queue = []
        self.failed_urls = []
        
        # Browser impersonation options
        self.impersonations = ["chrome", "safari", "firefox", "chrome_android", "safari_ios"]
        self.impersonation_index = 0
        
        # California rotating proxy
        self.california_proxy = {
            'https': 'http://35d146gk4kq9otn-country-us-state-california:z51vlpkz84emlb9@rp.scrapegw.com:6060'
        }

    async def fetch_url(self, url: str, impersonation: str = "chrome", max_retries: int = 3) -> Tuple[int, str]:
        """
        Input: URL string, browser impersonation type, max retry attempts
        Output: Tuple of (status_code, html_content)
        Description: Fetch single URL with retry logic and browser rotation on failure
        """
        async with self.semaphore:
            last_exception = None
            current_impersonation = impersonation
            impersonation_idx = self.impersonations.index(impersonation) if impersonation in self.impersonations else 0
            
            for attempt in range(max_retries + 1):
                try:
                    # Add delay between requests
                    if attempt > 0:
                        backoff_delay = min(2 ** attempt, 30)  # Exponential backoff, max 30s
                        logging.info(f"🔄 Retry {attempt} for {url}, waiting {backoff_delay}s...")
                        await asyncio.sleep(backoff_delay)
                    elif hasattr(self, 'delay_between_requests'):
                        await asyncio.sleep(self.delay_between_requests)
                    
                    # Make HTTP request
                    def make_request():
                        with curl_cffi.Session(impersonate=current_impersonation) as session:
                            return session.get(url, proxies=self.california_proxy, timeout=15, verify=False)
                    
                    response = await asyncio.to_thread(make_request)
                    
                    logging.info(f"🌐 Fetched {url} → Status: {response.status_code} (using {current_impersonation})")
                    
                    # Success for any 2xx status
                    if 200 <= response.status_code < 300:
                        if attempt > 0:
                            logging.info(f"✅ Request succeeded on attempt {attempt + 1} for {url}")
                        return response.status_code, response.text
                    
                    # Handle retryable errors - rotate browser
                    elif response.status_code in [403, 429, 500, 502, 503, 504]:    
                        logging.warning(f"⚠️ HTTP {response.status_code} for {url} with {current_impersonation} (attempt {attempt + 1})")
                        if attempt < max_retries:
                            impersonation_idx = (impersonation_idx + 1) % len(self.impersonations)
                            current_impersonation = self.impersonations[impersonation_idx]
                            logging.info(f"🔄 Rotating to {current_impersonation} for next attempt")
                        else:
                            return response.status_code, ""
                        continue
                    else:
                        # Don't retry for other 4xx errors
                        logging.error(f"❌ HTTP {response.status_code} error for {url} - not retrying")
                        return response.status_code, ""
                    
                except Exception as e:
                    last_exception = e
                    logging.warning(f"⚠️ Request exception for {url} with {current_impersonation} (attempt {attempt + 1}): {e}")
                    if attempt < max_retries:
                        impersonation_idx = (impersonation_idx + 1) % len(self.impersonations)
                        current_impersonation = self.impersonations[impersonation_idx]
                        logging.info(f"🔄 Exception triggered rotation to {current_impersonation}")
                    elif attempt == max_retries:
                        break
            
            # All retries failed
            logging.error(f"❌ All {max_retries + 1} attempts failed for {url}")
            if last_exception:
                logging.error(f"❌ Last exception: {last_exception}")
            return 0, ""

    async def process_url_with_retry(self, url: str, impersonation: str = "chrome", location_info: Dict = None) -> Tuple[bool, str]:
        """
        Input: URL, browser impersonation, optional location metadata
        Output: Tuple of (success_boolean, html_content)
        Description: Process single URL with comprehensive retry handling
        """
        try:
            status_code, html = await self.fetch_url(url, impersonation)
            
            if status_code != 200 or not html:
                logging.warning(f"⚠️ Failed to fetch {url} (status: {status_code})")
                await self.retry_queue.put((url, impersonation, location_info))
                return False, ""
                
            return True, html
            
        except Exception as e:
            logging.error(f"❌ Error processing {url}: {e}")
            await self.retry_queue.put((url, impersonation, location_info))
            return False, ""

    async def process_retry_queue(self):
        """
        Input: None
        Output: None
        Description: Process failed requests with different browser impersonations
        """
        processed_urls = set()
        
        while not self.retry_queue.empty():
            try:
                url, last_impersonation, location_info = await asyncio.wait_for(self.retry_queue.get(), timeout=1.0)
                
                if url in processed_urls:
                    continue
                    
                if url not in self.retry_attempts:
                    self.retry_attempts[url] = 0
                    
                if self.retry_attempts[url] >= self.max_retries_per_url:
                    logging.warning(f"⚠️ Queue retries exceeded for {url}, moving to failure queue")
                    self.failure_queue.append((url, location_info))
                    processed_urls.add(url)
                    continue
                
                # Try next impersonation
                current_idx = self.impersonations.index(last_impersonation) if last_impersonation in self.impersonations else 0
                next_impersonation = self.impersonations[(current_idx + 1) % len(self.impersonations)]
                
                self.retry_attempts[url] += 1
                logging.info(f"🔄 Queue retry {self.retry_attempts[url]}/{self.max_retries_per_url} for {url} with {next_impersonation}")
                
                success, html = await self.process_url_with_retry(url, next_impersonation, location_info)
                
                if success:
                    logging.info(f"✅ Queue retry successful for {url}")
                    processed_urls.add(url)
                    if url in self.retry_attempts:
                        del self.retry_attempts[url]
                    return html
                else:
                    if self.retry_attempts[url] < self.max_retries_per_url:
                        await self.retry_queue.put((url, next_impersonation, location_info))
                    await asyncio.sleep(2)
                    
            except asyncio.TimeoutError:
                break
            except Exception as e:
                logging.error(f"❌ Error processing retry queue: {e}")

    async def persistent_failure_retry(self, max_duration_minutes: int = 30):
        """
        Input: Maximum duration for persistent retry attempts in minutes
        Output: None
        Description: 30-minute persistent retry with exponential backoff for failure queue
        """
        if not self.failure_queue:
            return
            
        start_time = time.time()
        max_duration = max_duration_minutes * 60
        remaining_failures = self.failure_queue.copy()
        attempt = 0
        
        logging.info(f"🔄 Starting {max_duration_minutes}-minute persistent retry for {len(remaining_failures)} failed URLs")
        
        while remaining_failures and (time.time() - start_time) < max_duration:
            attempt += 1
            backoff_delay = min(30 * (2 ** (attempt - 1)), 600)  # Cap at 10 minutes
            current_impersonation = self.impersonations[(attempt - 1) % len(self.impersonations)]
            
            logging.info(f"🔄 Persistent attempt {attempt} using {current_impersonation} (backoff: {backoff_delay}s)")
            
            if attempt > 1:
                await asyncio.sleep(backoff_delay)
            
            if (time.time() - start_time) >= max_duration:
                logging.warning(f"⏰ {max_duration_minutes}-minute timeout reached")
                break
            
            # Try all remaining failures
            new_remaining = []
            for url, location_info in remaining_failures:
                success, html = await self.process_url_with_retry(url, current_impersonation, location_info)
                
                if not success:
                    new_remaining.append((url, location_info))
                else:
                    logging.info(f"✅ Persistent retry success: {url}")
            
            success_count = len(remaining_failures) - len(new_remaining)
            logging.info(f"📊 Persistent attempt {attempt}: {success_count}/{len(remaining_failures)} succeeded")
            remaining_failures = new_remaining
            
            if not remaining_failures:
                logging.info(f"🎉 All failure queue URLs recovered!")
                break
        
        # Final summary
        elapsed = time.time() - start_time
        total_failed = len(self.failure_queue)
        final_remaining = len(remaining_failures)
        total_recovered = total_failed - final_remaining
        
        logging.info(f"📊 Persistent retry summary ({elapsed:.1f}s):")
        logging.info(f"   ✅ Recovered: {total_recovered}/{total_failed}")
        logging.info(f"   ❌ Still failed: {final_remaining}/{total_failed}")
        
        if remaining_failures:
            self.failed_urls.extend([url for url, _ in remaining_failures])
            logging.warning(f"⚠️ {len(remaining_failures)} URLs permanently failed")
        
        self.failure_queue.clear()

    def get_next_impersonation(self) -> str:
        """
        Input: None
        Output: Next browser impersonation string
        Description: Rotate through available browser impersonations
        """
        current_impersonation = self.impersonations[self.impersonation_index % len(self.impersonations)]
        self.impersonation_index += 1
        return current_impersonation

    def get_failure_stats(self) -> Dict:
        """
        Input: None
        Output: Dictionary with failure statistics
        Description: Return current failure queue and retry statistics
        """
        return {
            "failed_urls": len(self.failed_urls),
            "failure_queue_size": len(self.failure_queue),
            "retry_queue_size": self.retry_queue.qsize(),
            "retry_attempts": dict(self.retry_attempts)
        }
