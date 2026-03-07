import psycopg2
from psycopg2 import sql
import os
import logging

logger = logging.getLogger('db_manager')

class DatabaseManager:
    def __init__(self, database_url=None):
        # Use provided URL or get from environment
        self.database_url = database_url or os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL must be provided or set in environment")
        
        # Render uses internal connection strings that may need SSL
        self.conn = None
        self.connect()
    
    def connect(self):
        """Establish database connection"""
        try:
            # For Render PostgreSQL, we need SSL
            self.conn = psycopg2.connect(
                self.database_url,
                sslmode='require'
            )
            logger.info("Successfully connected to database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def execute_query(self, query, params=None):
        """Execute a query and return results"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                if query.strip().upper().startswith('SELECT'):
                    return cur.fetchall()
                self.conn.commit()
                return cur.rowcount
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Query failed: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    def __del__(self):
        self.close()
