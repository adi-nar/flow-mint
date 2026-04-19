import yaml
from pathlib import Path

class ConfigRegistry:
    "takes care of config info from .yaml files"
    def __init__(self, config_file):
        config_path = Path(config_file)
        if not config_path.exists():
            raise FileNotFoundError(f'config files not found: {config_file}')
        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)

    def get_bank_paths(self, bank_name: str) -> str:
        return self.config["banks"][bank_name]['path']
    
    def get_bank_ftype(self, bank_name: str) -> str:
        return self.config["banks"][bank_name]['file_type']
    
    def get_salary_identifiers(self):
        return self.config.get('salary', {}).get('keywords', [])
    
    def get_min_threshold(self):
        return self.config.get('salary', {}).get('min_thresh', 0)

