def test_config_has_key(config):
    assert 'sources' in config
    assert 'worksheet' in config['sources']