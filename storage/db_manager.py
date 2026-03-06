import psycopg2
from psycopg2.extras import Json, DictCursor
import json
from datetime import datetime
from typing import Optional, Dict, List

class DatabaseManager:
    def __init__(self, database_url: str):
        self.conn = psycopg2.connect(database_url)
        self._init_tables()
    
    def _init_tables(self):
        """Initialize database schema"""
        with self.conn.cursor() as cur:
            # Keys table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS discovered_keys (
                    id SERIAL PRIMARY KEY,
                    key_hash VARCHAR(64) UNIQUE NOT NULL,
                    service VARCHAR(50) NOT NULL,
                    source_url TEXT,
                    source_type VARCHAR(50),
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    metadata JSONB DEFAULT '{}',
                    is_valid BOOLEAN DEFAULT FALSE,
                    last_validated TIMESTAMP,
                    validation_attempts INT DEFAULT 0
                )
            """)
            
            # Sources table (to track what we've already scraped)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scraped_sources (
                    id SERIAL PRIMARY KEY,
                    source_url VARCHAR(512) UNIQUE NOT NULL,
                    source_type VARCHAR(50) NOT NULL,
                    last_scraped TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    items_found INT DEFAULT 0
                )
            """)
            
            # Validation logs
            cur.execute("""
                CREATE TABLE IF NOT EXISTS validation_logs (
                    id SERIAL PRIMARY KEY,
                    key_id INTEGER REFERENCES discovered_keys(id),
                    validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    success BOOLEAN NOT NULL,
                    response_time_ms INTEGER,
                    error_message TEXT
                )
            """)
            
            self.conn.commit()
    
    def record_key(self, key_hash: str, service: str, source_url: str, 
                   source_type: str, metadata: Dict = None) -> bool:
        """Record a discovered key (returns False if duplicate)"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO discovered_keys 
                    (key_hash, service, source_url, source_type, metadata)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (key_hash) DO NOTHING
                    RETURNING id
                """, (key_hash, service, source_url, source_type, Json(metadata or {})))
                
                result = cur.fetchone()
                self.conn.commit()
                return result is not None
        except Exception as e:
            self.conn.rollback()
            print(f"Error recording key: {e}")
            return False
    
    def record_source(self, source_url: str, source_type: str, items_found: int):
        """Record that we've scraped a source"""
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO scraped_sources (source_url, source_type, items_found)
                VALUES (%s, %s, %s)
                ON CONFLICT (source_url) DO UPDATE SET
                    last_scraped = CURRENT_TIMESTAMP,
                    items_found = EXCLUDED.items_found
            """, (source_url, source_type, items_found))
            self.conn.commit()
    
    def get_keys_for_validation(self, limit: int = 100) -> List[Dict]:
        """Get keys that need validation"""
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
                SELECT id, key_hash, service, metadata
                FROM discovered_keys
                WHERE is_valid = FALSE 
                AND validation_attempts < 3
                AND (last_validated IS NULL OR last_validated < NOW() - INTERVAL '1 hour')
                ORDER BY discovered_at DESC
                LIMIT %s
            """, (limit,))
            
            return [dict(row) for row in cur.fetchall()]
    
    def update_validation(self, key_id: int, is_valid: bool, 
                         response_time_ms: int = None, error: str = None):
        """Update validation result"""
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE discovered_keys
                SET is_valid = %s,
                    last_validated = CURRENT_TIMESTAMP,
                    validation_attempts = validation_attempts + 1
                WHERE id = %s
            """, (is_valid, key_id))
            
            cur.execute("""
                INSERT INTO validation_logs 
                (key_id, success, response_time_ms, error_message)
                VALUES (%s, %s, %s, %s)
            """, (key_id, is_valid, response_time_ms, error))
            
            self.conn.commit()
