import re
import os
from typing import Dict, Pattern

# API Key Patterns
API_PATTERNS: Dict[str, Pattern] = {
    'openai': re.compile(r'sk-[a-zA-Z0-9]{48,}'),
    'anthropic': re.compile(r'sk-ant-[a-zA-Z0-9]{48,}'),
    'gemini': re.compile(r'AIza[a-zA-Z0-9\-_]{35}'),
    'groq': re.compile(r'gsk_[a-zA-Z0-9]{48,}'),
    'deepseek': re.compile(r'sk-[a-zA-Z0-9]{32,}'),
    'mistral': re.compile(r'[a-zA-Z0-9]{32,}'),
    'cohere': re.compile(r'[a-zA-Z0-9]{40,}'),
    'huggingface': re.compile(r'hf_[a-zA-Z0-9]{34,}'),
    'replicate': re.compile(r'r8_[a-zA-Z0-9]{40,}'),
}

# Validation URLs
VALIDATION_URLS = {
    'openai': 'https://api.openai.com/v1/models',
    'anthropic': 'https://api.anthropic.com/v1/models',
    'gemini': 'https://generativelanguage.googleapis.com/v1/models',
    'groq': 'https://api.groq.com/openai/v1/models',
    'deepseek': 'https://api.deepseek.com/v1/models',
    'mistral': 'https://api.mistral.ai/v1/models',
    'cohere': 'https://api.cohere.ai/v1/models',
    'huggingface': 'https://huggingface.co/api/models',
}

# Rate limiting (requests per minute)
RATE_LIMITS = {
    'github': 60,  # GitHub API limit
    'pastebin': 10,
    'public_code': 30,
}

# Redis config
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
REDIS_KEY_PREFIX = 'harvester:dedup:'
REDIS_TTL = 86400  # 24 hours dedup cache

# PostgreSQL config
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://localhost:5432/harvester')

# Encryption
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY')  # Must be set in environment
