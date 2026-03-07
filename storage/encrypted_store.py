import os
import json
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

class EncryptedKeyStore:
    def __init__(self, key_file='keys/encrypted_keys.json', salt_file='keys/salt.key'):
        self.key_file = key_file
        self.salt_file = salt_file
        self.backend = default_backend()
        self.salt = self._load_or_create_salt()
        self.cipher = self._create_cipher()
        self.keys = self._load_keys()
    
    def _load_or_create_salt(self):
        """Load existing salt or create a new one"""
        os.makedirs('keys', exist_ok=True)
        if os.path.exists(self.salt_file):
            with open(self.salt_file, 'rb') as f:
                return f.read()
        else:
            salt = os.urandom(16)
            with open(self.salt_file, 'wb') as f:
                f.write(salt)
            return salt
    
    def _create_cipher(self, password=None):
        """Create Fernet cipher from environment password"""
        if password is None:
            password = os.getenv('ENCRYPTION_KEY', 'default-key-change-me').encode()
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=self.salt,
            iterations=100000,
            backend=self.backend
        )
        key = base64.urlsafe_b64encode(kdf.derive(password))
        return Fernet(key)
    
    def _load_keys(self):
        """Load encrypted keys from file"""
        if os.path.exists(self.key_file):
            with open(self.key_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_keys(self):
        """Save encrypted keys to file"""
        os.makedirs(os.path.dirname(self.key_file), exist_ok=True)
        with open(self.key_file, 'w') as f:
            json.dump(self.keys, f, indent=2)
    
    def add_key(self, service, api_key):
        """Encrypt and store an API key"""
        encrypted = self.cipher.encrypt(api_key.encode()).decode()
        self.keys[service] = encrypted
        self._save_keys()
    
    def get_key(self, service):
        """Retrieve and decrypt an API key"""
        if service not in self.keys:
            return None
        encrypted = self.keys[service].encode()
        return self.cipher.decrypt(encrypted).decode()
    
    def list_services(self):
        """List all services with stored keys"""
        return list(self.keys.keys())
    
    def delete_key(self, service):
        """Delete a key from storage"""
        if service in self.keys:
            del self.keys[service]
            self._save_keys()
            return True
        return False
