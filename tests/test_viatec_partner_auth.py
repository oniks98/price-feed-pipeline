from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs

from scrapy.http import HtmlResponse, TextResponse

from suppliers.services.run_status_service import RunStatusService
from suppliers.spiders.base import ViatecBaseSpider


class ViatecAuthProbeSpider(ViatecBaseSpider):
    name = "viatec_auth_probe"
    supplier_id = "viatec"


class ViatecPartnerAuthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spider = ViatecAuthProbeSpider()
        self.spider.email = "partner@example.test"
        self.spider.password = "test-password"

    def test_login_request_uses_homepage_and_ajax_contract(self) -> None:
        initial_request = self.spider.start_partner_login(lambda response: None)
        self.assertEqual(initial_request.url, "https://viatec.ua/")

        homepage = HtmlResponse(
            url="https://viatec.ua/",
            body=b'<meta name="csrf-token" content="csrf-value">',
            encoding="utf-8",
        )
        login_request = self.spider.parse_partner_login_page(homepage, lambda response: None)

        self.assertIsNotNone(login_request)
        assert login_request is not None
        self.assertEqual(login_request.url, "https://viatec.ua/login")
        self.assertEqual(login_request.method, "POST")
        self.assertEqual(login_request.headers[b"Origin"], b"https://viatec.ua")
        self.assertEqual(login_request.headers[b"X-CSRF-Token"], b"csrf-value")
        self.assertEqual(login_request.headers[b"X-Requested-With"], b"XMLHttpRequest")
        self.assertEqual(
            parse_qs(login_request.body.decode("utf-8")),
            {"email": ["partner@example.test"], "password": ["test-password"]},
        )

    def test_successful_json_response_continues_spider(self) -> None:
        response = TextResponse(
            url="https://viatec.ua/login",
            body=b'{"status":"success","message":"/cabinet"}',
            encoding="utf-8",
        )

        result = self.spider.parse_partner_login_response(response, lambda _: ["next"])

        self.assertEqual(result, ["next"])
        self.assertFalse(self.spider._partner_login_failed)

    def test_failed_json_response_marks_supplier_run_as_failed(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.cwd() / "tmp") as temp_dir:
            previous_root = os.environ.get("PROJECT_ROOT")
            os.environ["PROJECT_ROOT"] = temp_dir
            try:
                response = TextResponse(
                    url="https://viatec.ua/login",
                    body=b'{"status":"error"}',
                    encoding="utf-8",
                )

                self.spider.parse_partner_login_response(response, lambda _: None)

                self.assertTrue(
                    RunStatusService.has_failed(Path(temp_dir), "viatec")
                )
            finally:
                if previous_root is None:
                    os.environ.pop("PROJECT_ROOT", None)
                else:
                    os.environ["PROJECT_ROOT"] = previous_root

    def test_http_error_marks_run_failed_and_keeps_safe_cloudflare_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.cwd() / "tmp") as temp_dir:
            previous_root = os.environ.get("PROJECT_ROOT")
            os.environ["PROJECT_ROOT"] = temp_dir
            try:
                response = TextResponse(
                    url="https://viatec.ua/login",
                    status=403,
                    headers={b"Server": b"cloudflare", b"CF-Ray": b"test-ray"},
                )

                self.spider.handle_partner_login_error(
                    SimpleNamespace(value=SimpleNamespace(response=response))
                )

                self.assertTrue(RunStatusService.has_failed(Path(temp_dir), "viatec"))
                self.assertEqual(
                    self.spider._login_error_diagnostics(response),
                    " (server=cloudflare, cf-ray=test-ray)",
                )
            finally:
                if previous_root is None:
                    os.environ.pop("PROJECT_ROOT", None)
                else:
                    os.environ["PROJECT_ROOT"] = previous_root


if __name__ == "__main__":
    unittest.main()
