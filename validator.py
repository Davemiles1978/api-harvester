#!/usr/bin/env python3
"""
DMAI API Key Validator - Hourly validation service
"""
import os
import sys
import time
import requests
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger('validator')

class KeyValidator:
    def __init__(self):
        from config import VALIDATION_URLS, DATABASE_URL
        from storage.db_manager import DatabaseManager
        from storage.encrypted_store import EncryptedKeyStore
        
        self.validation_urls = VALIDATION_URLS
        self.db = DatabaseManager(DATABASE_URL)
        self.store = EncryptedKeyStore(os.getenv('ENCRYPTION_KEY'))
        
    def validate_key(self, service: str, key: str) -> tuple[bool, float, str]:
        """Validate a single API key"""
        url = self.validation_urls.get(service)
        if not url:
            return False, 0, "No validation URL"
        
        headers = self._get_headers(service, key)
        start = time.time()
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response_time = (time.time() - start) * 1000
            
            if response.status_code == 200:
                return True, response_time, ""
            else:
                return False, response_time, f"HTTP {response.status_code}"
        except Exception as e:
            response_time = (time.time() - start) * 1000
            return False, response_time, str(e)
    
    def _get_headers(self, service: str, key: str) -> dict:
        """Get appropriate headers for each service"""
        if service == 'openai':
            return {'Authorization': f'Bearer {key}'}
        elif service == 'anthropic':
            return {'x-api-key': key, 'anthropic-version': '2023-06-01'}
        elif service == 'gemini':
            return {}  # Key in URL
        elif service == 'groq':
            return {'Authorization': f'Bearer {key}'}
        else:
            return {'Authorization': f'Bearer {key}'}
    
    def run_validation_cycle(self):
        """Validate all pending keys"""
        keys = self.db.get_keys_for_validation(limit=50)
        logger.info(f"Validating {len(keys)} keys")
        
        for key_record in keys:
            # Get actual key (this would need to retrieve from encrypted store)
            # For now, we'll just mark as validated
            success, response_time, error = self.validate_key(
                key_record['service'], 
                "dummy_key"  # TODO: retrieve actual key
            )
            
            self.db.update_validation(
                key_id=key_record['id'],
                is_valid=success,
                response_time_ms=int(response_time),
                error=error
            )
            
            if success:
                logger.info(f"✓ Key {key_record['id']} is valid")
            else:
                logger.debug(f"✗ Key {key_record['id']} invalid: {error}")
            
            time.sleep(1)  # Rate limiting
    
    def run(self):
        """Main loop (runs once per cron invocation)"""
        logger.info("Starting validation cycle")
        self.run_validation_cycle()
        logger.info("Validation complete")

if __name__ == "__main__":
    validator = KeyValidator()
    validator.run()
