import re
import time
import hashlib
import logging
from github import Github, GithubException
from datetime import datetime
from typing import Set, List, Dict

logger = logging.getLogger(__name__)

class GitHubScraper:
    def __init__(self, token: str, db_manager, redis_client, patterns: Dict):
        self.gh = Github(token)
        self.db = db_manager
        self.redis = redis_client
        self.patterns = patterns
        self.rate_limit_remaining = 60
        self.rate_limit_reset = 0
    
    def search_code(self, queries: List[str] = None):
        """Search GitHub for API keys"""
        if queries is None:
            queries = [
                'api.key',
                'sk-',
                'API_KEY',
                'OPENAI_API_KEY',
                'extension:env',
                'filename:.env',
                'password',
                'secret'
            ]
        
        for query in queries:
            try:
                self._search_with_query(query)
            except GithubException as e:
                if e.status == 403:  # Rate limited
                    logger.warning(f"GitHub rate limit hit, waiting...")
                    time.sleep(60)
                else:
                    logger.error(f"GitHub error: {e}")
    
    def _search_with_query(self, query: str):
        """Execute a single search query"""
        try:
            results = self.gh.search_code(query)
            logger.info(f"Searching GitHub for: {query} (total: {results.totalCount})")
            
            for result in results:
                if self._check_rate_limit():
                    return
                
                # Get file content
                try:
                    content = result.decoded_content.decode('utf-8', errors='ignore')
                    self._scan_content(content, result.html_url)
                except Exception as e:
                    logger.debug(f"Could not decode {result.html_url}: {e}")
                
        except GithubException as e:
            logger.error(f"Search failed for {query}: {e}")
    
    def _scan_content(self, content: str, source_url: str):
        """Scan content for API keys"""
        for service, pattern in self.patterns.items():
            matches = pattern.findall(content)
            
            for match in matches:
                # Create hash for deduplication
                key_hash = hashlib.sha256(match.encode()).hexdigest()
                
                # Check Redis for recent duplicates
                if self.redis.get(f"github:{key_hash}"):
                    continue
                
                # Record in database
                if self.db.record_key(
                    key_hash=key_hash,
                    service=service,
                    source_url=source_url,
                    source_type='github',
                    metadata={'match': match[:20] + '...'}
                ):
                    logger.info(f"Found {service} key at {source_url}")
                    self.redis.setex(f"github:{key_hash}", 86400, "1")
        
        # Record source
        self.db.record_source(source_url, 'github', len(matches))
    
    def _check_rate_limit(self) -> bool:
        """Check if we're near rate limit and should pause"""
        rate = self.gh.get_rate_limit()
        if rate.core.remaining < 10:
            sleep_time = (rate.core.reset - datetime.now()).seconds + 10
            logger.warning(f"Rate limit low, sleeping {sleep_time}s")
            time.sleep(sleep_time)
            return True
        return False
