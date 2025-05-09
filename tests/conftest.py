import pytest
from pathlib import Path
from configparser import ConfigParser

@pytest.fixture
def config():
    config_path = Path(__file__).resolve().parent.parent / 'cmti_tools' / 'config.toml'
    assert config_path.exists(), f"Config found at {config_path}"
    
    cfg = ConfigParser()
    cfg.read(config_path)
    return cfg