import logging
from scripts.load_data import load_to_gcs

def test_load_to_gcs(caplog):
    with caplog.at_level(logging.INFO):
        result = load_to_gcs()
        assert result is True
        assert "Loading data to GCS" in caplog.text

