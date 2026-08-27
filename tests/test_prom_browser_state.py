from __future__ import annotations

import json
import unittest

from scripts.services.prom_browser_state import BrowserStateError, load_prom_browser_state, storage_state_summary


class PromBrowserStateTests(unittest.TestCase):
    def test_storage_state_has_priority_and_preserves_origins(self) -> None:
        storage_state = {
            "cookies": [{"name": "session", "value": "secret"}],
            "origins": [{"origin": "https://my.prom.ua", "localStorage": []}],
        }

        state = load_prom_browser_state(json.dumps(storage_state), '[{"name": "legacy"}]')

        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state.source, "PROM_STORAGE_STATE")
        self.assertEqual(state.storage_state, storage_state)
        self.assertIsNone(state.cookies)
        self.assertEqual(state.cookie_count, 1)
        self.assertEqual(state.origin_count, 1)

    def test_legacy_cookies_remain_supported(self) -> None:
        state = load_prom_browser_state("", '[{"name": "session", "value": "secret"}]')

        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state.source, "PROM_COOKIES")
        self.assertIsNone(state.storage_state)
        self.assertEqual(state.cookie_count, 1)

    def test_storage_state_summary_omits_secret_values(self) -> None:
        summary = storage_state_summary(
            {
                "cookies": [{"name": "session", "value": "secret"}],
                "origins": [
                    {
                        "origin": "https://my.prom.ua",
                        "localStorage": [{"name": "selected_company", "value": "private"}],
                    }
                ],
            }
        )

        self.assertEqual(
            summary,
            {
                "cookie_count": 1,
                "origin_count": 1,
                "origins": [
                    {"origin": "https://my.prom.ua", "local_storage_keys": ["selected_company"]}
                ],
            },
        )
        self.assertNotIn("secret", json.dumps(summary))
        self.assertNotIn("private", json.dumps(summary))

    def test_rejects_non_list_legacy_cookies(self) -> None:
        with self.assertRaisesRegex(BrowserStateError, "PROM_COOKIES must be a JSON list"):
            load_prom_browser_state("", '{"cookies": []}')
