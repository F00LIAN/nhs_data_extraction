import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

california_rotating_proxy = {
    'https': 'http://35d146gk4kq9otn-country-us-state-california:z51vlpkz84emlb9@rp.scrapegw.com:6060'
}

#url = "https://httpbin.org/ip"
target_url = "https://www.newhomesource.com/community/ca/perris/rockridge-by-kb-home/202198"
target_url_2 = "https://www.newhomesource.com/community/ca/winchester/oliva-at-siena-by-taylor-morrison/200717"
#resp = httpx.get(url, proxy=us_rotating_proxy, timeout=10)
#print(resp.json())

#r = curl_cffi.get(target_url_2, impersonate="safari", proxies=us_rotating_proxy)
#print(r.status_code)
#print(r.text)
