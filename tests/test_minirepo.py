import sys
import os
import asyncio
import socket
import pytest
import pytest_asyncio
import aiohttp
from aiohttp import web

from wheel_filename import parse_wheel_filename

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from minirepo import (
    fetch,
    fetch_meta_data,
    chain_generators,
    filter_in_python_versions,
    filter_in_package_types,
    filter_in_extensions,
    filter_in_platforms,
    filter_paths_exist,
    parse_wheel_filename,
)

@pytest.fixture
def free_port():
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    

# --- Fixture to start a test server ---
@pytest_asyncio.fixture
async def http_server(free_port):
    """
    Spin up an aiohttp test server that serves static files
    defined as strings in the test.
    """
    files = {}

    async def handler(request):
        path = request.match_info["filename"]
        if path not in files:
            return web.Response(status=404, text="Not Found")

        content, mime = files[path]
        # If JSON requested, use json_response
        if mime == "application/json":
            return web.json_response(content)
        
        return web.Response(text=content, content_type=mime)

    app = web.Application()
    app.router.add_get("/{filename}/json", handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", free_port)
    await site.start()

    base_url = f"http://127.0.0.1:{free_port}"

    # Yield (base_url, files dict) so tests can populate files dynamically
    yield base_url, files

    # Cleanup
    await runner.cleanup()


@pytest.mark.asyncio
async def test_fetch(http_server):
    base_url, files = http_server

    # Define sample "files" as strings
    files["data.json"] = ({"foo": "bar"}, "application/json")

    semaphore = asyncio.Semaphore(100)

    async with aiohttp.ClientSession() as session:        
        json = await(fetch(f"{base_url}/data.json/json", session, semaphore))
        assert json == {"foo": "bar"}




@pytest.mark.asyncio
async def test_fetch_meta_data(http_server):
    base_url, files = http_server

    # Define sample JSON files to serve
    files["packageA.json"] = ({"info": {"name": "packageA", "version": "1.0"}, "urls": []}, "application/json")
    files["packageB.json"] = ({"info": {"name": "packageB", "version": "2.0"}, "urls": []}, "application/json")
    files["packageC.json"] = ({"info": {"name": "packageC", "version": "3.0"}, "urls": []}, "application/json")

    # Patch the names to point to our local server URLs
    names = ["packageA.json", "packageB.json", "packageC.json"]

    results = await fetch_meta_data(names, url=base_url)

    assert len(results) == 3




# Sample repository path for testing
TEST_REPO = "/tmp/fake_repo"

# Patch os.path.exists for filter_paths_exist
@pytest.fixture(autouse=True)
def patch_exists(monkeypatch):
    def fake_exists(path):
        # Only "foo-1.0-py3-none-any.whl" exists
        return "foo-1.0-py3-none-any.whl" in path
    monkeypatch.setattr(os.path, "exists", fake_exists)


# Patch parse_wheel_filename
@pytest.fixture(autouse=True)
def patch_parse_wheel_filename(monkeypatch):
    class DummyWheel:
        platform_tags = ["any"]
    monkeypatch.setattr("wheel_filename.parse_wheel_filename", lambda fn: DummyWheel())


def test_chained_filters_with_arguments():
    # Sample data
    urls = [
        {"filename": "foo-1.0-py3-none-any.whl", "packagetype": "bdist_wheel", "python_version": "py3"},
        {"filename": "bar-1.0-py2-none-any.whl", "packagetype": "bdist_wheel", "python_version": "py2"},
        {"filename": "baz-1.0.tar.gz", "packagetype": "sdist", "python_version": "py3"},
    ]

    python_versions = {"py3"}
    package_types = {"bdist_wheel", "sdist"}
    extensions = {"whl", "tar.gz"}
    platforms = {"any"}

    def dummy_filter(urls):
        for url in urls:
            yield url
    
    filtered = list(filter_paths_exist(urls, repository="~/minirepo"))
    filtered = list(filter_in_platforms(filtered, platforms=platforms))
    filtered = list(filter_in_extensions(filtered, extensions=extensions))
    filtered = list(filter_in_package_types(filtered, package_types=package_types))
    filtered = list(filter_in_python_versions(filtered, python_versions=python_versions))

    expected_filenames = {"baz-1.0.tar.gz"}
    result_filenames = {url["filename"] for url in filtered}

    assert len(result_filenames) == 1
    assert result_filenames == expected_filenames