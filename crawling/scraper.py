import asyncio
import codecs
import re
import sqlite3
import sys
from datetime import datetime

import aiohttp
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tqdm.asyncio import tqdm

sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "replace")



# **隨機 User-Agent**
def get_headers():
    ua = UserAgent()
    return {"User-Agent": ua.random}


# **獲取最後一頁頁碼**
async def get_last_page(session, region, url1, url2):
    try:
        async with session.get(url1 + "1" + f"&region={region}" + url2, headers=get_headers()) as res:
            html = await res.text()
            soup = BeautifulSoup(html, "html.parser")
            elements = soup.find_all('li', attrs={"data-v-779297d8": ""})
            last_number = max([int(el.text.strip()) for el in elements if el.text.strip().isdigit()], default=1)
            return last_number
    except Exception as e:
        print(f"[ERROR] 獲取 {region} 的最後一頁數失敗: {e}")
        return 1


# **非同步爬取 post_id**
async def fetch_post_ids(session, region, page, url1, url2):
    try:
        async with session.get(url1 + str(page) + f"&region={region}" + url2, headers=get_headers()) as res:
            soup = BeautifulSoup(await res.text(), "html.parser")
            links = soup.find_all('a', class_="link v-middle")
            return [link['href'].split('/')[-1] for link in links if link.has_attr('href')]
    except Exception as e:
        print(f"[ERROR] 爬取 {region} 頁碼 {page} 的 post_id 失敗: {e}")
        return []


# **批量爬取所有 post_id**
async def crawl_all_post_ids(region, url1, url2):
    async with aiohttp.ClientSession() as session:
        last_page = await get_last_page(session, region, url1, url2)
        tasks = [fetch_post_ids(session, region, page, url1, url2) for page in range(1, last_page + 1)]
        results = await tqdm.gather(*tasks)
        return [post_id for sublist in results for post_id in sublist]


# **爬取詳細資訊**
async def fetch_post_details(session, post_id):
    url = f"https://business.591.com.tw/rent/{post_id}"
    try:
        async with session.get(url, headers=get_headers()) as res:
            soup = BeautifulSoup(await res.text(), "html.parser")

            # **抓取基本資訊**
            span_tags = soup.find_all('span', attrs={'data-v-588d0396': ''})
            extracted_texts = [tag.text.strip() for tag in span_tags]

            patterns = {
                'owner': re.compile(r'(屋主|代理人|仲介): .+'),
                'price': re.compile(r'\d{1,3}(,\d{3})*元/月'),
                'phone': re.compile(r'\d{4}-\d{3}-\d{3}|\d{2}-\d{8}|\d{2}-\d{7}')
            }

            results = {
                'id': post_id, 'owner': None, 'price': None, 'phone': None,
                'location': None, 'place': None, 'squaremeter': None, 'floor': None, 'type': None,
                'latitude': None, 'longitude': None
            }

            for text in extracted_texts:
                for key, pattern in patterns.items():
                    if pattern.match(text):
                        results[key] = text

            # **抓取地址**
            address_tag = soup.find('div', class_="address")
            if address_tag:
                results['location'] = address_tag.text.strip()

            # **抓取區域**
            place_tag = soup.find('div', class_="place")
            if place_tag:
                results['place'] = place_tag.text.strip()

            # **抓取坪數、樓層、類型**
            info_tags = soup.find_all('div', class_="info")
            info_values = [tag.text.strip() for tag in info_tags]

            if len(info_values) >= 3:
                results['squaremeter'], results['floor'], results['type'] = info_values[:3]

            # **抓取經緯度**
            script_tags = soup.find_all("script")
            for script in script_tags:
                match = re.search(r'latitude\s*:\s*([\d.]+),\s*longitude\s*:\s*([\d.]+)', script.text)
                if match:
                    results['latitude'], results['longitude'] = match.groups()
                    break

            return results
    except Exception as e:
        print(f"[ERROR] 爬取 post_id {post_id} 詳細資訊失敗: {e}")
        return results


# **批量爬取所有 post 詳細資訊**
async def crawl_all_post_details(post_ids):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_post_details(session, post_id) for post_id in post_ids]
        results = await tqdm.gather(*tasks)
        return results


# **儲存到 SQLite**
def save_to_db(data_list):
    conn = sqlite3.connect("scraped_data.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scraped_data (
            id TEXT PRIMARY KEY,
            owner TEXT,
            price TEXT,
            location TEXT,
            place TEXT,
            phone TEXT,
            squaremeter TEXT,
            floor TEXT,
            type TEXT,
            latitude TEXT,
            longitude TEXT,
            timestamp TEXT
        )
    """)

    for data in data_list:
        cursor.execute("""
            INSERT OR REPLACE INTO scraped_data 
            (id, owner, price, location, place, phone, squaremeter, floor, type, latitude, longitude, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data['id'], data['owner'], data['price'], data['location'], data['place'],
            data['phone'], data['squaremeter'], data['floor'], data['type'],
            data['latitude'], data['longitude'], datetime.now()
        ))

    conn.commit()
    conn.close()


# **主程式**
async def main():
    try:
        print("開始執行主程式...")
        
        area_dict = {
            "台北市": (1, "https://business.591.com.tw/list?type=1&kind=6&region=1&page=", "")
        }


        all_results = []
        for region_name, (area_list, url_all, url_condition) in area_dict.items():
            for region in area_list:
                print(f"\n🚀 正在爬取 {region_name} (區域ID: {region}) 的資料...")
                post_ids = await crawl_all_post_ids(region, url_all, url_condition)
                print(f"✅ 獲取 {len(post_ids)} 筆 post_id，開始爬取詳細資訊...")

                post_details = await crawl_all_post_details(post_ids)
                all_results.extend(post_details)

        save_to_db(all_results)
        print("✅ 爬取完成，數據已存入 SQLite。")
        print(all_results)

    except Exception as e:
        print(f"❌ 發生錯誤: {e}")


if __name__ == "__main__":
    asyncio.run(main())
