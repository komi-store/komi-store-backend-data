"""
Network-free smoke run of every script in scripts/, meant for warnings-as-errors:

    python -W error -X warn_default_encoding tests/smoke.py

HTTP (aiohttp, urllib, requests) and Postgres (psycopg2.connect) are stubbed with
canned responses, and every file the scripts write lands in a temporary directory,
so it needs no tokens, no database and never touches cached-data/ in the repo.
"""

import asyncio
import contextlib
import importlib
import io
import json
import os
import sys
import tempfile
import urllib.request
from datetime import datetime, timedelta, timezone
from unittest import mock

import aiohttp
import psycopg2
import requests

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOW = datetime.now(timezone.utc).replace(microsecond=0)


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


SEARCH_ITEM = {
    "id": 1,
    "name": "app",
    "full_name": "octo/app",
    "owner": {"login": "octo", "avatar_url": "https://example.test/a.png"},
    "description": "A cross-platform android and desktop app",
    "default_branch": "main",
    "html_url": "https://github.com/octo/app",
    "stargazers_count": 9000,
    "forks_count": 10,
    "language": "Kotlin",
    "topics": ["android", "linux"],
    "releases_url": "https://api.github.com/repos/octo/app/releases{/id}",
    "updated_at": _iso(NOW),
    "created_at": _iso(NOW - timedelta(days=400)),
    "pushed_at": _iso(NOW),
    "open_issues_count": 3,
    "archived": False,
}

# The old release only ships an Alpine package, which must not count as Android.
RELEASES = [
    {
        "tag_name": "v2",
        "draft": False,
        "prerelease": False,
        "published_at": _iso(NOW - timedelta(days=2)),
        "assets": [{"name": n, "download_count": 5} for n in ("app.apk", "setup.exe", "app.dmg", "app.AppImage")],
    },
    {
        "tag_name": "v1",
        "draft": False,
        "prerelease": False,
        "published_at": _iso(NOW - timedelta(days=900)),
        "assets": [{"name": "app_linux_amd64.apk", "download_count": 1}],
    },
]

REPO_ROW = {
    "id": 1,
    "full_name": "octo/app",
    "owner": "octo",
    "name": "app",
    "owner_avatar_url": "https://example.test/a.png",
    "description": "d",
    "default_branch": "main",
    "html_url": "https://github.com/octo/app",
    "stars": 9000,
    "forks": 10,
    "language": "Kotlin",
    "topics": ["android"],
    "latest_release_date": NOW,
    "latest_release_tag": "v2",
    "download_count": 21,
    "has_installers_android": True,
    "has_installers_windows": True,
    "has_installers_macos": True,
    "has_installers_linux": True,
    "platform_releases": json.dumps({"android": {"tag": "v2", "publishedAt": _iso(NOW)}}),
    "trending_score": 1.0,
    "popularity_score": 2,
    "search_score": 3.0,
}


def canned_body(url):
    if "/search/" in url:
        return {"total_count": 1, "items": [SEARCH_ITEM]}
    if url.endswith("/rate_limit"):
        return {"resources": {"core": {"remaining": 5000, "limit": 5000}, "search": {"remaining": 30, "limit": 30}}}
    if "/releases" in url:
        return RELEASES
    if "/v1/feed" in url:
        return {"rotation": "r1", "items": [{"id": 1}], "hasMore": False}
    if "/indexes" in url:
        return {"taskUid": 1}
    raise AssertionError(f"unexpected request: {url}")


RATE_HEADERS = {"X-RateLimit-Remaining": "4000", "X-RateLimit-Reset": "0"}


class FakeAiohttpResponse:
    status = 200

    def __init__(self, url):
        self.url = url
        self.headers = RATE_HEADERS

    async def json(self):
        return canned_body(self.url)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeUrlopenResponse:
    status = 200

    def __init__(self, url):
        self.url = url
        self.headers = RATE_HEADERS

    def read(self):
        return json.dumps(canned_body(self.url)).encode()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeRequestsResponse:
    status_code = 200

    def json(self):
        return {"published_at": "2026-01-01T00:00:00Z", "tag_name": "v1", "prerelease": True, "draft": False}


class FakeCursor:
    rowcount = 1

    def __init__(self, log):
        self.log = log

    def execute(self, sql, params=None):
        self.log.append(sql)

    def fetchall(self):
        return [dict(REPO_ROW)]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConnection:
    def __init__(self, log):
        self.log = log

    def cursor(self, cursor_factory=None):
        return FakeCursor(self.log)

    def commit(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def main():
    sql_log = []
    urls = []

    def fake_urlopen(req, timeout=None):
        urls.append(req.full_url)
        return FakeUrlopenResponse(req.full_url)

    def fake_session_get(self, url, params=None):
        return FakeAiohttpResponse(url)

    env = {"GITHUB_TOKEN": "dummy", "FORCE_REFRESH": "true", "CAPTURE_DAILY_SNAPSHOT": "1"}
    with contextlib.ExitStack() as stack:
        tmp = stack.enter_context(tempfile.TemporaryDirectory())
        stack.enter_context(mock.patch.dict(os.environ, env))
        stack.enter_context(mock.patch.object(sys, "path", [os.path.join(REPO_ROOT, "scripts"), *sys.path]))
        stack.enter_context(mock.patch.object(urllib.request, "urlopen", fake_urlopen))
        stack.enter_context(mock.patch.object(aiohttp.ClientSession, "get", fake_session_get))
        stack.enter_context(mock.patch.object(psycopg2, "connect", lambda *a, **k: FakeConnection(sql_log)))
        stack.enter_context(mock.patch.object(requests, "get", lambda *a, **k: FakeRequestsResponse()))
        stack.enter_context(mock.patch("time.sleep", lambda seconds: None))

        db_writer = importlib.import_module("db_writer")
        db_writer.DATABASE_URL = "postgresql://smoke.test/db"
        fac = importlib.import_module("fetch_all_categories")
        fac.CACHE_DIR = os.path.join(tmp, "cached-data")
        # The real limiter pauses ~60s per 28 searches; the stubbed searches don't need pacing.
        fac.SEARCH_RATE_LIMIT = 10_000
        with contextlib.redirect_stdout(io.StringIO()):
            asyncio.run(fac.main())
        written = [f for _, _, files in os.walk(fac.CACHE_DIR) for f in files]
        assert len(written) == 32, written
        with open(os.path.join(fac.CACHE_DIR, "new-releases", "linux.json"), encoding="utf-8") as f:
            repo = json.load(f)["repositories"][0]
        assert sorted(repo["platformReleases"]) == ["android", "linux", "macos", "windows"], repo
        assert any("repo_daily_snapshot" in sql for sql in sql_log), "daily snapshot was not written"
        print(f"fetch_all_categories: {len(written)} cache files, {len(sql_log)} SQL statements")

        feed = importlib.import_module("fetch_feed_cache")
        feed.OUT_DIR = os.path.join(tmp, "cached-data", "feed")
        with contextlib.redirect_stdout(io.StringIO()):
            assert feed.main() == 0
        assert len(os.listdir(feed.OUT_DIR)) == len(feed.PLATFORMS)
        print(f"fetch_feed_cache: {len(feed.PLATFORMS)} feed files")

        meili = importlib.import_module("meili_sync")
        with contextlib.redirect_stdout(io.StringIO()):
            meili.configure_index()
            meili.bulk_sync()
        assert any(u.endswith("/indexes/repos/documents") for u in urls), urls
        print("meili_sync: index configured, documents pushed")

        backfill = importlib.import_module("backfill_downloads")
        backfill.DATABASE_URL = "postgresql://smoke.test/db"
        backfill.MEILI_URL = "http://meili.smoke.test"
        with contextlib.redirect_stdout(io.StringIO()):
            backfill.backfill()
        assert backfill._updated == 1
        print("backfill_downloads: 1 repo updated")

        validate = importlib.import_module("validate_releases")
        stack.enter_context(mock.patch.object(sys, "argv", ["validate_releases.py", "android"]))
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                validate.main()
        finally:
            os.chdir(cwd)
        assert "MISMATCH" in out.getvalue(), out.getvalue()
        print("validate_releases: checked the new-releases fixture")


if __name__ == "__main__":
    main()
