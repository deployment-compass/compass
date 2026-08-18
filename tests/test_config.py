import os
import unittest
from unittest.mock import patch

from compass.config import Settings


class TestSettings(unittest.TestCase):
    def test_defaults_use_compose_service_names_in_container(self):
        with patch.dict(os.environ, {"COMPASS_IN_DOCKER": "true"}, clear=False):
            settings = Settings()

        self.assertEqual(settings.prometheus_url, "http://prometheus:9090")
        self.assertEqual(settings.loki_url, "http://loki:3100")
        self.assertEqual(settings.redis_url, "redis://redis:6379/0")
        self.assertEqual(settings.rabbitmq_url, "amqp://guest:guest@rabbitmq:5672/")


if __name__ == "__main__":
    unittest.main()
