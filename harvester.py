#!/usr/bin/env python3
"""
DMAI API Key Harvester - Continuous 24/7 Scraper
"""
import os
import sys
import time
import logging
import signal
import redis
import requests
import traceback
from datetime import datetime
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/harvester.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('harvester')

class Harvester:
    def __init__(self):
        self.running = True
        self.cycle_count = 0
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)
        
        # Initialize components
        self.init_components()
        
    def init_components(self):
        """Initialize all harvester components"""
        try:
            from config import API_PATTERNS, REDIS_URL, DATABASE_URL
            from storage.db_manager import DatabaseManager
            from scrapers.github_scraper import GitHubScraper
            
            # Redis for deduplication
            self.redis = redis.from_url(REDIS_URL)
            logger.info("Redis connected successfully")
            
            # Database
            self.db = DatabaseManager(DATABASE_URL)
            logger.info("Database connected successfully")
            
            # Scrapers
            github_token = os.getenv('GITHUB_TOKEN')
            if github_token:
                self.github_scraper = GitHubScraper(
                    token=github_token,
                    db_manager=self.db,
                    redis_client=self.redis,
                    patterns=API_PATTERNS
                )
                logger.info("GitHub scraper initialized")
            else:
                logger.warning("No GITHUB_TOKEN found, GitHub scraper disabled")
                self.github_scraper = None
            
            logger.info("All components initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize components: {e}")
            logger.error(traceback.format_exc())
            raise
    
    def handle_shutdown(self, signum, frame):
        """Graceful shutdown"""
        logger.info(f"Shutdown signal received: {signum}")
        logger.info("Shutting down harvester...")
        self.running = False
    
    def check_database_connection(self):
        """Verify database connection is alive"""
        try:
            # Simple query to check connection
            self.db.execute_query("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Database connection lost: {e}")
            # Try to reconnect
            try:
                self.db.connect()
                logger.info("Database reconnected successfully")
                return True
            except:
                return False
    
    def scrape_apis_guru(self):
        """Scrape APIs from APIS.Guru directory"""
        try:
            logger.info("Scraping APIS.Guru...")
            response = requests.get("https://api.apis.guru/v2/list.json", timeout=15)
            if response.status_code == 200:
                data = response.json()
                api_count = len(data)
                logger.info(f"Found {api_count} total APIs from APIS.Guru")
                
                # Process and save to database
                saved_count = 0
                for api_name, api_info in list(data.items())[:200]:  # Limit to first 200
                    try:
                        # Check if we should continue running
                        if not self.running:
                            break
                            
                        # Extract API details
                        api_data = {
                            'name': api_name,
                            'title': api_info.get('info', {}).get('title', ''),
                            'description': api_info.get('info', {}).get('description', ''),
                            'version': api_info.get('info', {}).get('version', ''),
                            'url': f"https://api.apis.guru/v2/{api_name}.json",
                            'source': 'apis.guru',
                            'harvested_at': datetime.now().isoformat()
                        }
                        
                        # Save to database
                        self.db.save_api(api_data)
                        saved_count += 1
                        if saved_count % 50 == 0:
                            logger.info(f"Saved {saved_count} APIs from APIS.Guru so far")
                            
                    except Exception as e:
                        logger.error(f"Error saving API {api_name}: {e}")
                
                logger.info(f"Successfully saved {saved_count} APIs from APIS.Guru")
                return saved_count
        except Exception as e:
            logger.error(f"APIS.Guru scraping error: {e}")
            logger.error(traceback.format_exc())
        return 0
    
    def scrape_public_apis_github(self):
        """Scrape APIs from public-apis GitHub repo"""
        try:
            logger.info("Scraping public-apis GitHub repo...")
            response = requests.get(
                "https://api.github.com/repos/public-apis/public-apis/contents/README.md",
                headers={"Accept": "application/vnd.github.v3.raw"},
                timeout=15
            )
            if response.status_code == 200:
                content = response.text
                logger.info(f"Fetched public-apis README ({len(content)} bytes)")
                
                # Simple parsing for API entries (look for markdown table rows)
                lines = content.split('\n')
                api_count = 0
                in_table = False
                
                for line in lines:
                    if not self.running:
                        break
                        
                    if '| API |' in line and '| Description |' in line:
                        in_table = True
                        continue
                    
                    if in_table and line.startswith('| ---'):
                        continue
                    
                    if in_table and '|' in line and not line.startswith('| ['):
                        parts = line.split('|')
                        if len(parts) >= 4:
                            api_name = parts[1].strip()
                            api_desc = parts[2].strip()
                            api_url = parts[3].strip() if len(parts) > 3 else ''
                            
                            if api_name and api_name != 'API' and not api_name.startswith('['):
                                api_data = {
                                    'name': api_name,
                                    'description': api_desc,
                                    'url': api_url,
                                    'source': 'public-apis-github',
                                    'harvested_at': datetime.now().isoformat()
                                }
                                
                                # Save to database
                                self.db.save_api(api_data)
                                api_count += 1
                    
                    if in_table and not line.startswith('|'):
                        in_table = False
                
                logger.info(f"Found and saved {api_count} APIs from public-apis repo")
                return api_count
        except Exception as e:
            logger.error(f"Public APIs scraping error: {e}")
            logger.error(traceback.format_exc())
        return 0
    
    def scrape_programmable_web(self):
        """Scrape APIs from ProgrammableWeb (if accessible)"""
        try:
            logger.info("Scraping ProgrammableWeb...")
            response = requests.get(
                "https://www.programmableweb.com/apis/directory",
                timeout=15,
                headers={'User-Agent': 'Mozilla/5.0 (compatible; DMAI-Harvester/1.0)'}
            )
            if response.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for API entries (adjust selectors based on actual HTML)
                api_entries = soup.select('.api-entry, .views-row, .api-item, tr')
                api_count = 0
                
                for entry in api_entries[:100]:  # Limit to first 100
                    if not self.running:
                        break
                        
                    try:
                        title_elem = entry.select_one('.api-title a, h3 a, td:nth-child(1) a')
                        desc_elem = entry.select_one('.api-description, .description, td:nth-child(2)')
                        
                        if title_elem:
                            api_data = {
                                'name': title_elem.text.strip(),
                                'url': title_elem.get('href', ''),
                                'description': desc_elem.text.strip() if desc_elem else '',
                                'source': 'programmableweb',
                                'harvested_at': datetime.now().isoformat()
                            }
                            
                            self.db.save_api(api_data)
                            api_count += 1
                    except Exception as e:
                        logger.error(f"Error parsing ProgrammableWeb entry: {e}")
                
                logger.info(f"Found and saved {api_count} APIs from ProgrammableWeb")
                return api_count
        except Exception as e:
            logger.error(f"ProgrammableWeb scraping error: {e}")
            logger.error(traceback.format_exc())
        return 0
    
    def run_cycle(self):
        """Run one harvesting cycle"""
        self.cycle_count += 1
        logger.info(f"=== Starting harvesting cycle #{self.cycle_count} ===")
        
        # Check database connection
        if not self.check_database_connection():
            logger.error("Cannot proceed without database connection")
            return
        
        total_apis = 0
        
        # GitHub scraping
        if self.github_scraper:
            try:
                logger.info("Starting GitHub scraping...")
                github_count = self.github_scraper.search_code()
                total_apis += github_count
                logger.info(f"GitHub scraping found {github_count} APIs")
            except Exception as e:
                logger.error(f"GitHub scraping error: {e}")
                logger.error(traceback.format_exc())
        
        # APIS.Guru scraping
        try:
            apis_guru_count = self.scrape_apis_guru()
            total_apis += apis_guru_count
        except Exception as e:
            logger.error(f"APIS.Guru scraping error: {e}")
        
        # Public APIs GitHub repo
        try:
            public_apis_count = self.scrape_public_apis_github()
            total_apis += public_apis_count
        except Exception as e:
            logger.error(f"Public APIs scraping error: {e}")
        
        # ProgrammableWeb scraping (commented out by default - uncomment if needed)
        # try:
        #     programmable_count = self.scrape_programmable_web()
        #     total_apis += programmable_count
        # except Exception as e:
        #     logger.error(f"ProgrammableWeb scraping error: {e}")
        
        logger.info(f"=== Cycle #{self.cycle_count} complete. Total APIs found: {total_apis} ===")
        
        # Update metrics in Redis
        try:
            self.redis.set('harvester:last_run', datetime.now().isoformat())
            self.redis.set('harvester:total_apis', total_apis)
            self.redis.set('harvester:cycle_count', self.cycle_count)
            self.redis.incr('harvester:cycles_completed')
            logger.debug("Redis metrics updated")
        except Exception as e:
            logger.error(f"Failed to update Redis metrics: {e}")
    
    def run(self):
        """Main loop"""
        logger.info("=" * 50)
        logger.info("DMAI API Harvester started - Continuous Mode")
        logger.info("=" * 50)
        logger.info(f"PID: {os.getpid()}")
        
        while self.running:
            try:
                cycle_start = time.time()
                self.run_cycle()
                cycle_duration = time.time() - cycle_start
                
                logger.info(f"Cycle took {cycle_duration:.2f} seconds")
                
                # Sleep between cycles (with check for shutdown)
                sleep_interval = 60  # 60 seconds between cycles
                logger.info(f"Sleeping for {sleep_interval} seconds until next cycle")
                
                for i in range(sleep_interval):
                    if not self.running:
                        logger.info("Shutdown detected during sleep")
                        break
                    if i % 10 == 0 and i > 0:
                        logger.debug(f"Sleeping... {sleep_interval - i} seconds remaining")
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"Fatal error in main loop: {e}")
                logger.error(traceback.format_exc())
                logger.info("Waiting 30 seconds before restarting cycle...")
                time.sleep(30)
        
        logger.info("=" * 50)
        logger.info(f"Harvester stopped after {self.cycle_count} cycles")
        logger.info("=" * 50)

if __name__ == "__main__":
    try:
        harvester = Harvester()
        harvester.run()
    except Exception as e:
        logger.error(f"Failed to start harvester: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
