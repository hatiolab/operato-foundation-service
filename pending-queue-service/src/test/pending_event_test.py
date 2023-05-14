from unittest import TestCase, main

from pending_event.pending_event_handler import PendingEventHandler

from config import Config

Config("config.yaml")

class PendingEventTest(TestCase):
    def test_initialize(self):
        pending_event_handler = PendingEventHandler()
        pending_event_handler.initialize()
        self.assertIsNotNone(pending_event_handler)


if __name__ == "__main__":
    main()
