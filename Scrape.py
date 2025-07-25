#!/usr/bin/env python3
"""
Dynamic JPX Derivatives Statistics Scraper
------------------------------------------
This scraper dynamically discovers archive URLs from the website's dropdown menu,
making it robust against future changes. It can run in 'full' mode to get all
reports or 'targeted' mode for a specific report defined in config.ini.
"""

import time, random, json, csv, os, requests, configparser, re
from datetime import datetime
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
import undetected_chromedriver as uc

# --- Simplified Configuration ---
BASE_URL = "https://www.jpx.co.jp"
MAIN_PAGE_URL = "https://www.jpx.co.jp/english/markets/statistics-derivatives/sector/index.html"
PAGE_LOAD_INDICATOR = 'div#main-area.-is-fix'
IMPLICIT_WAIT = 10
EXPLICIT_WAIT = 20
DOWNLOAD_TIMEOUT = 30
TEMP_STORAGE_DIR = "jpx_temp_storage"
HEADLESS = True
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"]

def load_config():
    """Loads settings from config.ini with verbose error handling."""
    config_filename = 'config.ini'
    config = configparser.ConfigParser()
    if not os.path.exists(config_filename):
        print(f"[WARN] '{config_filename}' not found. Defaulting to 'full' scrape mode.")
        return {'mode': 'full'}
    try:
        config.read(config_filename, encoding='utf-8')
        if 'ScraperSettings' in config and 'mode' in config['ScraperSettings']:
            settings = config['ScraperSettings']
            print(f"[OK] Successfully loaded '{config_filename}'. Mode set to: '{settings.get('mode')}'")
            return settings
    except Exception as e:
        print(f"[WARN] Could not parse '{config_filename}'. Error: {e}")
    print("    -> Defaulting to 'full' scrape mode.")
    return {'mode': 'full'}

# +++ THIS IS THE NEW, CORRECTED PARSER +++
def parse_report_date(date_text: str):
    """
    Parses complex date strings like 'Jul 2025, Week2（7/7 - 7/11）' or 'Dec. 8, 2023 (Week 2)'
    using regular expressions for robustness.
    """
    # Pattern explanation:
    # ([A-Za-z]{3})  - Capture group 1: Exactly 3 letters for the month (e.g., "Jul")
    # \.?\s+         - Optional period, followed by one or more spaces
    # (\d{4})        - Capture group 2: Exactly 4 digits for the year (e.g., "2025")
    # .*?            - Any characters, non-greedy (to handle the comma and space)
    # Week(\d+)       - The literal word "Week" followed by Capture group 3: one or more digits for the week number
    pattern = re.compile(r"([A-Za-z]{3})\.?\s+(\d{4}).*?Week(\d+)", re.IGNORECASE)
    
    match = pattern.search(date_text)
    
    if match:
        month, year, week = match.groups()
        return {'year': int(year), 'month': month, 'week': int(week)}
    
    # Fallback for the other format, just in case
    pattern2 = re.compile(r"([A-Za-z]{3})\.?\s+\d{1,2},?\s+(\d{4}).*?Week\s?(\d+)", re.IGNORECASE)
    match2 = pattern2.search(date_text)
    if match2:
        month, year, week = match2.groups()
        return {'year': int(year), 'month': month, 'week': int(week)}

    print(f"[WARN] Could not parse date format: '{date_text}'")
    return None


class JPXCompleteScraper:
    def __init__(self):
        self.driver, self.session, self.all_data, self.failed_downloads = None, None, [], []
        self.archive_urls = {} # Will be populated dynamically
        self.config = load_config()
        self.setup_storage()
        self.setup_session()

    def setup_storage(self):
        if not os.path.exists(TEMP_STORAGE_DIR): os.makedirs(TEMP_STORAGE_DIR)
        print(f"[OK] Main storage directory ensured: {TEMP_STORAGE_DIR}")

    def setup_session(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': random.choice(USER_AGENTS)})

    def initialize_driver(self):
        print("... Initializing web driver...")
        try:
            options = uc.ChromeOptions()
            options.add_argument(f'--user-agent={random.choice(USER_AGENTS)}')
            options.add_argument('--no-sandbox')
            if HEADLESS: options.add_argument('--headless')
            self.driver = uc.Chrome(options=options)
            self.driver.implicitly_wait(IMPLICIT_WAIT)
            print("[OK] Driver initialized.")
            return True
        except Exception as e:
            print(f"[ERROR] Critical Error initializing driver: {e}")
            return False

    def wait_for_page_load(self):
        try:
            WebDriverWait(self.driver, EXPLICIT_WAIT).until(EC.presence_of_element_located((By.CSS_SELECTOR, PAGE_LOAD_INDICATOR)))
            return True
        except TimeoutException: return False

    def discover_archive_urls(self):
        """Dynamically finds all archive URLs from the dropdown menu."""
        print(f"\n--> Discovering archive URLs from: {MAIN_PAGE_URL}")
        try:
            self.driver.get(MAIN_PAGE_URL)
            if not self.wait_for_page_load():
                print("[ERROR] Could not load main page to discover URLs.")
                return False

            dropdown = self.driver.find_element(By.CSS_SELECTOR, "select.backnumber")
            options = dropdown.find_elements(By.TAG_NAME, "option")
            
            discovered_urls = {}
            for option in options:
                year_text = option.text.strip()
                url_path = option.get_attribute('value')
                key = "Current" if "current" in year_text.lower() else year_text
                discovered_urls[key] = url_path
            
            if not discovered_urls:
                print("[ERROR] Failed to discover any URLs from the dropdown.")
                return False
            
            self.archive_urls = discovered_urls
            print(f"[OK] Discovered {len(self.archive_urls)} archive pages.")
            return True

        except (NoSuchElementException, WebDriverException) as e:
            print(f"[ERROR] Error during URL discovery: {e}")
            return False

    def extract_table_data(self):
        try:
            table = self.driver.find_element(By.CSS_SELECTOR, "table.overtable.fixedhead")
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            is_targeted = self.config.get('mode') == 'targeted'
            
            if is_targeted:
                target_year, target_month, target_week = self.config.getint('year'), self.config.get('month'), self.config.getint('week')
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) < 3: continue
                    date_text = cells[0].text.strip()
                    parsed_date = parse_report_date(date_text)
                    if parsed_date and (parsed_date['year'] == target_year and parsed_date['month'].lower() == target_month.lower() and parsed_date['week'] == target_week):
                        print(f"  (TARGET) Found: {date_text}")
                        # Using a list comprehension to build the list with one item cleanly
                        return [{"report_year": str(p['year']), "date": dt, 
                                 "pdf_url": c[1].find_element(By.TAG_NAME, "a").get_attribute("href"),
                                 "pdf_filename": self.generate_filename(dt, "pdf")} for c, dt, p in [(cells, date_text, parsed_date)]]
                return []
            else: # Full mode
                all_reports = []
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) < 3: continue
                    date_text = cells[0].text.strip()
                    parsed_date = parse_report_date(date_text)
                    if parsed_date:
                        print(f"  (INFO) Found: {date_text}")
                        all_reports.append({"report_year": str(parsed_date['year']), "date": date_text,
                                            "pdf_url": cells[1].find_element(By.TAG_NAME, "a").get_attribute("href"),
                                            "pdf_filename": self.generate_filename(date_text, "pdf")})
                return all_reports
        except Exception as e:
            print(f"[ERROR] Error extracting table data: {e}")
            return []

    def generate_filename(self, date_text, file_type):
        clean_date = date_text.replace("（", "_").replace("）", "").replace("/", "-").replace(" ", "_").replace(",", "")
        return "".join(c for c in clean_date if c.isalnum() or c in "._-") + f".{file_type}"

    def download_and_store_reports(self, reports):
        for entry in reports:
            year_dir = os.path.join(TEMP_STORAGE_DIR, entry["report_year"])
            if not os.path.exists(year_dir):
                print(f"--> Creating download folder: {year_dir}")
                os.makedirs(year_dir)
            
            local_path = os.path.join(year_dir, entry["pdf_filename"])
            if os.path.exists(local_path):
                print(f"  (SKIP) Skipping existing file: {entry['pdf_filename']}")
                continue
            
            print(f"  ... Downloading {entry['pdf_filename']}")
            try:
                time.sleep(random.uniform(1, 2))
                response = self.session.get(entry["pdf_url"], timeout=DOWNLOAD_TIMEOUT, stream=True)
                response.raise_for_status()
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
                print(f"  [OK] Downloaded: {os.path.basename(local_path)}")
                self.all_data.append(entry)
            except Exception as e:
                print(f"  [ERROR] Download failed: {entry['pdf_url']} - {e}")
                self.failed_downloads.append(entry)

    def scrape_page(self, page_key, url):
        full_url = urljoin(BASE_URL, url)
        print(f"\n--> Visiting page for '{page_key}': {full_url}")
        try:
            self.driver.get(full_url)
            if not self.wait_for_page_load(): return
            time.sleep(random.uniform(2, 4))
            found_data = self.extract_table_data()
            if found_data:
                self.download_and_store_reports(found_data)
        except Exception as e: print(f"[ERROR] Error scraping page '{page_key}': {e}")

    def run_complete_scrape(self):
        start_time = time.time()
        if not self.initialize_driver(): return
        
        try:
            if not self.discover_archive_urls(): return
            
            urls_to_visit = {}
            mode = self.config.get('mode')
            
            if mode == 'targeted':
                target_year = self.config.get('year')
                print(f"\n[INFO] Starting scrape in TARGETED mode for year {target_year}.")
                if 'Current' in self.archive_urls:
                    urls_to_visit['Current'] = self.archive_urls['Current']
                if target_year in self.archive_urls:
                    urls_to_visit[target_year] = self.archive_urls[target_year]
                if not urls_to_visit:
                    print(f"[ERROR] Could not find a page for target year '{target_year}'.")
                    return
            else:
                print("\n[INFO] Starting scrape in FULL mode.")
                urls_to_visit = self.archive_urls

            print(f"[OK] Will visit {len(urls_to_visit)} page(s) to find reports.")
            for page_key, url in urls_to_visit.items():
                self.scrape_page(page_key, url)
                if mode == 'targeted' and self.all_data:
                    print("[OK] Targeted report found. Halting search.")
                    break
            
            print(f"\n[SUCCESS] SCRAPING COMPLETE! ({(time.time() - start_time)/60:.1f} minutes)")
            if self.all_data: print(f"  > Downloaded {len(self.all_data)} reports.")
            if self.failed_downloads: print(f"  > Failed to download {len(self.failed_downloads)} reports.")
            if not self.all_data and mode == 'targeted': print("[WARN] NOTE: No reports matching your criteria were found.")
        
        finally:
            if self.driver: self.driver.quit()

def main():
    print("=" * 60); print("Dynamic JPX Derivatives Statistics Scraper"); print("=" * 60)
    scraper = JPXCompleteScraper()
    scraper.run_complete_scrape()

if __name__ == "__main__":
    main()