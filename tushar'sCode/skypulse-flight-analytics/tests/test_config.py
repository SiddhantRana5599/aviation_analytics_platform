import unittest

from src.common.config import kafka_settings, load_config


class ConfigTest(unittest.TestCase):
    def test_config_loads(self):
        config = load_config("configs/pipeline_config.yaml")
        self.assertEqual(config["project"]["name"], "skypulse")
        self.assertEqual(config["kafka"]["topic"], "flight_fares_stream")

    def test_kafka_settings(self):
        config = load_config("configs/pipeline_config.yaml")
        settings = kafka_settings(config)
        self.assertEqual(settings.topic, "flight_fares_stream")
        self.assertTrue(settings.bootstrap_servers)


if __name__ == "__main__":
    unittest.main()
