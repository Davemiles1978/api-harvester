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
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)
        
        # Initialize components
        self.init_components()
        
    def init_components(self):
        """Initialize all harvester components"""
        from config import API_PATTERNS, REDIS_URL, DATABASE_URL
        from storage.db_manager import DatabaseManager
        from scrapers.github_scraper import GitHubScraper
        
        # Redis for deduplication
        self.redis = redis.from_url(REDIS_URL)
        
        # Database
        self.db = DatabaseManager(DATABASE_URL)
        
        # Scrapers
        github_token = os.getenv('GITHUB_TOKEN')
        if github_token:
            self.github_scraper = GitHubScraper(
                token=github_token,
                db_manager=self.db,
                redis_client=self.redis,
                patterns=API_PATTERNS
            )
        else:
            logger.warning("No GITHUB_TOKEN found, GitHub scraper disabled")
            self.github_scraper = None
        
        logger.info("All components initialized")
    
    def handle_shutdown(self, signum, frame):
        """Graceful shutdown"""
        logger.info("Shutting down harvester...")
        self.running = False
    
    def run_cycle(self):
        """Run one harvesting cycle"""
        logger.info("Starting harvesting cycle")
        
        # GitHub scraping
        if self.github_scraper:
            try:
                self.github_scraper.search_code()
            except Exception as e:
                logger.error(f"GitHub scraping error: {e}")
        
        # Pastebin scraping (TODO)
        # Public code scraping (TODO)
        
        logger.info("Harvesting cycle complete")
    
    def run(self):
        """Main loop"""
        logger.info("DMAI API Harvester started")
        
        while self.running:
            try:
                self.run_cycle()
                
                # Sleep between cycles (with check for shutdown)
                for _ in range(60):  # Check every second for shutdown
                    if not self.running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"Cycle error: {e}")
                time.sleep(30)
        
        logger.info("Harvester stopped")

if __name__ == "__main__":
    harvester = Harvester()
    harvester.run()
