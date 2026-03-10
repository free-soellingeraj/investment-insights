"""
Adversarial frontend render tests.

Verifies that every page in the Next.js app at http://localhost:3000 renders
without errors, returns correct status codes, and serves static assets.

Requires both servers to be running:
  - Frontend: http://localhost:3000
  - Backend:  http://localhost:8080
"""

import re
import pytest
import httpx

BASE_URL = "http://localhost:3000"
TIMEOUT = 15.0


@pytest.fixture(scope="module")
def client():
    """Shared httpx client for the test module."""
    with httpx.Client(base_url=BASE_URL, timeout=TIMEOUT, follow_redirects=True) as c:
        yield c


# ---------------------------------------------------------------------------
# 1. All routes return 200
# ---------------------------------------------------------------------------


class TestRoutesReturn200:
    """Every known page route should return HTTP 200."""

    @pytest.mark.parametrize(
        "path",
        [
            "/",
            "/trading",
            "/pipeline",
            "/internals",
            "/changelog",
        ],
        ids=["dashboard", "trading", "pipeline", "internals", "changelog"],
    )
    def test_static_routes(self, client: httpx.Client, path: str):
        resp = client.get(path)
        assert resp.status_code == 200, (
            f"GET {path} returned {resp.status_code}, expected 200"
        )

    @pytest.mark.parametrize(
        "ticker",
        ["AAPL", "NVDA"],
    )
    def test_company_routes(self, client: httpx.Client, ticker: str):
        resp = client.get(f"/company/{ticker}")
        assert resp.status_code == 200, (
            f"GET /company/{ticker} returned {resp.status_code}"
        )

    def test_nonexistent_company_still_renders(self, client: httpx.Client):
        """A company page for a ticker that doesn't exist should still return
        200 and show a not-found / empty state rather than crashing."""
        resp = client.get("/company/NONEXISTENT")
        assert resp.status_code == 200, (
            f"GET /company/NONEXISTENT returned {resp.status_code}"
        )


# ---------------------------------------------------------------------------
# 2. HTML content checks
# ---------------------------------------------------------------------------


class TestHTMLContent:
    """Verify each page contains expected content and no error markers."""

    def test_dashboard_heading(self, client: httpx.Client):
        html = client.get("/").text
        assert re.search(
            r"AI Opportunity Index|Dashboard|Investment Insights", html, re.IGNORECASE
        ), "Dashboard page is missing expected heading text"

    def test_trading_heading(self, client: httpx.Client):
        html = client.get("/trading").text
        assert re.search(
            r"Trad(e|ing)|Signal", html, re.IGNORECASE
        ), "Trading page is missing expected heading text"

    def test_pipeline_heading(self, client: httpx.Client):
        html = client.get("/pipeline").text
        assert re.search(
            r"Pipeline", html, re.IGNORECASE
        ), "Pipeline page is missing expected heading text"

    def test_internals_heading(self, client: httpx.Client):
        html = client.get("/internals").text
        assert re.search(
            r"Internal", html, re.IGNORECASE
        ), "Internals page is missing expected heading text"

    def test_company_page_shows_ticker(self, client: httpx.Client):
        html = client.get("/company/AAPL").text
        assert "AAPL" in html, "Company page for AAPL does not contain 'AAPL'"

    @pytest.mark.parametrize(
        "path",
        ["/", "/trading", "/pipeline", "/internals", "/changelog", "/company/AAPL"],
        ids=["dashboard", "trading", "pipeline", "internals", "changelog", "company"],
    )
    def test_no_internal_server_error(self, client: httpx.Client, path: str):
        html = client.get(path).text
        assert "Internal Server Error" not in html, (
            f"{path} contains 'Internal Server Error'"
        )
        assert "500" not in html or not re.search(
            r"<h[12][^>]*>\s*500\s*</h[12]>", html
        ), f"{path} appears to show a 500 error page"

    @pytest.mark.parametrize(
        "path",
        ["/", "/trading", "/pipeline", "/internals", "/changelog", "/company/AAPL"],
        ids=["dashboard", "trading", "pipeline", "internals", "changelog", "company"],
    )
    def test_no_unhandled_runtime_error(self, client: httpx.Client, path: str):
        html = client.get(path).text
        assert "Unhandled Runtime Error" not in html, (
            f"{path} contains 'Unhandled Runtime Error'"
        )


# ---------------------------------------------------------------------------
# 3. Static assets load
# ---------------------------------------------------------------------------


class TestStaticAssets:
    """Verify CSS and JS bundles referenced in the HTML are loadable."""

    def _extract_static_urls(self, html: str) -> list[str]:
        """Pull all /_next/static/... URLs from the HTML."""
        # Match href and src attributes pointing to _next/static
        urls = re.findall(r'(?:href|src)="(/_next/static/[^"]+)"', html)
        return urls

    def test_static_assets_from_dashboard(self, client: httpx.Client):
        html = client.get("/").text
        urls = self._extract_static_urls(html)
        assert len(urls) > 0, "No static asset URLs found in dashboard HTML"
        for url in urls:
            resp = client.get(url)
            assert resp.status_code == 200, (
                f"Static asset {url} returned {resp.status_code}"
            )

    def test_css_is_served(self, client: httpx.Client):
        html = client.get("/").text
        css_urls = re.findall(r'href="(/_next/static/[^"]+\.css)"', html)
        assert len(css_urls) > 0, "No CSS files found in dashboard HTML"
        for url in css_urls:
            resp = client.get(url)
            assert resp.status_code == 200, f"CSS file {url} returned {resp.status_code}"
            assert "text/css" in resp.headers.get("content-type", ""), (
                f"CSS file {url} has wrong content-type: {resp.headers.get('content-type')}"
            )


# ---------------------------------------------------------------------------
# 4. API connectivity from frontend
# ---------------------------------------------------------------------------


class TestAPIConnectivity:
    """Verify the frontend references the GraphQL backend."""

    def test_graphql_endpoint_referenced(self, client: httpx.Client):
        """The rendered HTML or inlined JS should reference the GraphQL endpoint."""
        html = client.get("/").text
        # Check for the endpoint in the HTML itself or in inline scripts
        has_graphql_ref = (
            "graphql" in html.lower()
            or "localhost:8000" in html
            or "localhost:8080" in html
        )
        if not has_graphql_ref:
            # The reference may be in a JS bundle; fetch a couple of JS files
            js_urls = re.findall(r'src="(/_next/static/[^"]+\.js)"', html)
            for url in js_urls[:5]:  # check first 5 bundles only
                js_content = client.get(url).text
                if "graphql" in js_content.lower() or "8000" in js_content or "8080" in js_content:
                    has_graphql_ref = True
                    break
        assert has_graphql_ref, (
            "No reference to GraphQL endpoint found in HTML or JS bundles"
        )


# ---------------------------------------------------------------------------
# 5. Edge case routes
# ---------------------------------------------------------------------------


class TestEdgeCaseRoutes:
    """Boundary and adversarial route tests."""

    def test_nonexistent_page_not_500(self, client: httpx.Client):
        """A truly unknown route should return 404, not 500."""
        resp = client.get("/nonexistent-page")
        assert resp.status_code in (404, 308, 301, 302, 200), (
            f"GET /nonexistent-page returned {resp.status_code}"
        )
        assert resp.status_code != 500, "Unknown route returned 500"

    def test_company_no_ticker(self, client: httpx.Client):
        """GET /company/ with no ticker should handle gracefully."""
        resp = client.get("/company/")
        # Accept 404, redirect, or 200 with an empty/error state -- just not 500
        assert resp.status_code != 500, (
            f"GET /company/ returned 500 -- should handle missing ticker gracefully"
        )
        if resp.status_code == 200:
            assert "Internal Server Error" not in resp.text
            assert "Unhandled Runtime Error" not in resp.text

    def test_path_traversal_blocked(self, client: httpx.Client):
        """Path traversal attempts must not leak filesystem contents."""
        traversal_paths = [
            "/company/../../etc/passwd",
            "/company/..%2F..%2Fetc%2Fpasswd",
            "/company/%2e%2e%2f%2e%2e%2fetc%2fpasswd",
        ]
        for path in traversal_paths:
            resp = client.get(path)
            body = resp.text.lower()
            assert "root:" not in body, (
                f"Path traversal via {path} exposed /etc/passwd content"
            )
            assert resp.status_code != 500, (
                f"Path traversal via {path} caused a 500 error"
            )
