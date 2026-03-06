from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2
import base64
import os
import json
from typing import Dict, Optional

class EncryptedKeyStore:
    def __init__(self, encryption_key: str):
        """Initialize encrypted store with master key"""
        if not encryption_key:
            raise ValueError("ENCRYPTION_KEY must be set")
        
        # Derive Fernet key from master key
        kdf = PBKDF2(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b'dmai-salt-2026',  # Fixed salt for consistency
            iterations=480000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(encryption_key.encode()))
        self.cipher = Fernet(key)
        self.storage_file = "storage/encrypted_keys.json"
        self._ensure_storage()
    
    def _ensure_storage(self):
        """Ensure storage file exists"""
        if not os.path.exists(self.storage_file):
            with open(self.storage_file, 'w') as f:
                json.dump({}, f)
    
    def _load(self) -> Dict:
        """Load and decrypt all keys"""
        with open(self.storage_file, 'r') as f:
            encrypted_data = json.load(f)
        
        decrypted = {}
        for key_id, encrypted_value in encrypted_data.items():
            try:
                decrypted[key_id] = self.cipher.decrypt(
                    encrypted_value.encode()
                ).decode()
            except:
                continue  # Skip corrupted entries
        return decrypted
    
    def _save(self, data: Dict):
        """Encrypt and save all keys"""
        encrypted = {}
        for key_id, value in data.items():
            encrypted[key_id] = self.cipher.encrypt(
                value.encode()
            ).decode()
        
        with open(self.storage_file, 'w') as f:
            json.dump(encrypted, f, indent=2)
    
    def store_key(self, key_id: str, api_key: str, service: str, metadata: Dict = None):
        """Store an encrypted API key"""
        data = self._load()
        data[key_id] = api_key
        
        # Also store metadata separately
        metadata_file = "storage/key_metadata.json"
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                all_metadata = json.load(f)
        else:
            all_metadata = {}
        
        all_metadata[key_id] = {
            'service': service,
            'discovered_at': metadata.get('discovered_at', ''),
            'source': metadata.get('source', ''),
            'validated': metadata.get('validated', False),
            'last_validated': metadata.get('last_validated', ''),
        }
        
        with open(metadata_file, 'w') as f:
            json.dump(all_metadata, f, indent=2)
        
        self._save(data)
    
    def get_key(self, key_id: str) -> Optional[str]:
        """Retrieve a decrypted API key"""
        data = self._load()
        return data.get(key_id)
    
    def list_keys(self, service: Optional[str] = None) -> Dict:
        """List all keys (metadata only, not actual keys)"""
        metadata_file = "storage/key_metadata.json"
        if not os.path.exists(metadata_file):
            return {}
        
        with open(metadata_file, 'r') as f:
            all_metadata = json.load(f)
        
        if service:
            return {k: v for k, v in all_metadata.items() 
                   if v.get('service') == service}
        return all_metadata
