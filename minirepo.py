#!/usr/bin/env python
#
# download all source packages from https://pypi.python.org

from pathlib import Path
import argparse
import sys
import os
import time
import logging
import json
from typing import Any, NoReturn
import requests
import asyncio
import aiofiles
from lxml import html
import aiohttp
from aiohttp import ClientSession, ClientTimeout
from tqdm.asyncio import tqdm_asyncio
from wheel_filename import parse_wheel_filename, InvalidFilenameError


TIMEOUT: ClientTimeout = aiohttp.ClientTimeout(
    total=300,  # maximum total request time
    connect=10,  # time to establish connection
    sock_connect=10,  # time to wait before socket connect
    sock_read=100,  # time to wait between reads
)

MINIREPO_CONFIG = os.path.expanduser(os.environ.get("MINIREPO_CONFIG", "~/.minirepo"))

DEFAULT_CONFIG = {
    "repository": os.path.expanduser("~/minirepo"),
    "python_versions": ["cp310", "py3", "py2.py3", "py3.10", "py310", "any"],
    "package_types": [
        "bdist_wheel",
    ],
    "extensions": [
        "whl",
    ],
    "platforms": ["win_amd64", "any"],
}


def chain_generators(urls, *filters) -> Any:
    """Apply a series of generator filters to the list of URLs."""

    for f in filters:
        urls = f(urls)
    return urls


async def fetch_and_parse(url: str):
    """Fetch a URL and parse the HTML to extract all links."""

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            total_size = int(resp.headers.get("Content-Length", 0))

            # Accumulate chunks in memory (PyPI index is ~20–30 MB, fits fine)
            content = bytearray()
            with tqdm_asyncio(
                total=total_size, unit="B", unit_scale=True, desc="Downloading"
            ) as pbar:
                async for chunk in resp.content.iter_chunked(65536):
                    content.extend(chunk)
                    pbar.update(len(chunk))

    # Parse once after full download
    tree = html.fromstring(bytes(content))
    links = [a.text for a in tree.xpath("//a")]
    return links


async def get_names():
    """Fetch the list of package names from PyPI simple index."""
    return await fetch_and_parse("https://pypi.org/simple/")


def load_cache(cache_path, ttl):
    """Load cached data if it exists and is fresh."""
    if not cache_path.exists():
        return None
    try:
        with open(cache_path, "r") as f:
            cached = json.load(f)
        if time.time() - cached["timestamp"] > ttl:
            return None
        return cached["data"]
    except Exception:
        return None


def save_cache(cache_path, data) -> None:
    """Save data to cache with current timestamp."""
    with open(cache_path, "w") as f:
        json.dump({"timestamp": time.time(), "data": data}, f)


async def get_names_cached(ttl, cache_path, clear_cache=False):
    """Get package names with caching."""
    if clear_cache:
        cache_path.unlink(missing_ok=True)
    else:
        cached = load_cache(cache_path, ttl)
        if cached is not None:
            logging.info("Loaded package names from cache")
            return cached
    # Fallback to fetch
    names = await fetch_and_parse("https://pypi.org/simple/")
    save_cache(cache_path, names)
    return names


async def fetch(url: str, session: ClientSession, semaphore: asyncio.Semaphore):
    """Download a single URL using aiohttp with concurrency limit."""
    async with semaphore:
        try:
            async with session.get(url, timeout=TIMEOUT) as response:
                if response.status != 200:
                    logging.debug(f"Failed to fetch {url}: {response.status}")
                    return None
                json = await response.json()
                return json
        except Exception as e:
            return f"Error: {e}"


async def fetch_meta_data(names, url="https://pypi.python.org/pypi"):
    """Fetch metadata for a list of package names from PyPI."""
    # names = names[0:1000]

    urls = [f"{url}/{name}/json" for name in names]

    # Limit concurrency so we don’t overwhelm your machine or the remote server
    semaphore = asyncio.Semaphore(1000)  # at most 100 simultaneous requests
    results = []
    chunk_size = 10000

    async with aiohttp.ClientSession() as session:
        with tqdm_asyncio(total=len(urls)) as pbar:
            for i in range(0, len(urls), chunk_size):
                batch = urls[i : i + chunk_size]
                tasks = [fetch(url, session, semaphore) for url in batch]
                finished = await asyncio.gather(*tasks)

                results.extend(finished)
                pbar.update(len(batch))  # update once per batch

    results = [_ for _ in results if _ is not None]

    return results


async def fetch_meta_data_cached(
    names, ttl, cache_path, clear_cache=False, url="https://pypi.python.org/pypi"
):
    """Fetch package metadata with caching."""
    if clear_cache:
        cache_path.unlink(missing_ok=True)
    else:
        cached = load_cache(cache_path, ttl)
        if cached is not None:
            logging.info("Loaded metadata from cache")
            return cached
    # Fallback to fetch
    metadata = await fetch_meta_data(names, url=url)
    save_cache(cache_path, metadata)
    return metadata


async def fetch_file(
    url: str, session: ClientSession, semaphore: asyncio.Semaphore, repository
):
    """Download a single URL using aiohttp with concurrency limit."""

    async with semaphore:
        filename = Path(url).name
        file_path = Path(repository) / filename
        # print(f"Writing to: {file_path}")
        try:
            async with session.get(url, timeout=TIMEOUT) as response:
                if response.status != 200:
                    # logging.warning(f"Failed to fetch {url}: {response.status}")
                    return None

                # Write to file in chunks
                async with aiofiles.open(file_path, "wb") as f:
                    async for chunk in response.content.iter_chunked(
                        1024 * 1024
                    ):  # 1 MB chunks
                        await f.write(chunk)

                return str(file_path)

        except Exception as e:
            logging.error(f"Error fetching {url}: {e}", exc_info=True)
            return None


async def fetch_urls(urls, repository):
    """Fetch multiple URLs concurrently and save to repository."""

    total_bytes = sum(url["size"] for url in urls)
    semaphore = asyncio.Semaphore(100)
    results = []

    async with aiohttp.ClientSession() as session:
        with tqdm_asyncio(
            total=total_bytes,
            unit="GB",
            unit_scale=1 / 1e9,  # convert bytes to GB
            unit_divisor=1,
            bar_format="{l_bar}{bar} {n:.2f}/{total:.2f} {unit} [{elapsed}<{remaining}, {rate_fmt}]",
            desc="Downloading",
        ) as pbar:

            async def fetch_and_update(url):
                filename = await fetch_file(
                    url["url"], session, semaphore, repository=repository
                )
                if filename is not None:
                    pbar.update(url["size"])
                return filename

            tasks = [fetch_and_update(url) for url in urls]

            # as_completed yields tasks as they finish
            for coro in asyncio.as_completed(tasks):
                result = await coro
                results.append(result)

    # Filter out None results
    results = [r for r in results if r is not None]
    return results


def filter_in_python_versions(urls, python_versions):
    """Filter URLs by Python versions."""
    for url in urls:
        if url["python_version"] in python_versions:
            yield url


def filter_in_package_types(urls, package_types):
    """Filter URLs by package types."""
    for url in urls:
        if url["packagetype"] in package_types:
            yield url


def filter_in_extensions(urls, extensions):
    """Filter URLs by file extensions."""
    for url in urls:
        filename = url["filename"]
        suffixes = "".join(Path(filename).suffixes)  # e.g. '.tar.gz'
        for ext in extensions:
            # allow extensions with or without leading dot
            ext = ext if ext.startswith(".") else f".{ext}"
            if suffixes.endswith(ext):
                yield url
                break


def filter_in_platforms(urls, platforms):
    """Filter URLs by platform tags (for wheels)."""
    for url in urls:
        if url["packagetype"] == "bdist_wheel":
            try:
                pkg = parse_wheel_filename(url["filename"])
                for p in pkg.platform_tags:
                    if p in platforms:
                        yield url
                        break
            except InvalidFilenameError:
                logging.warning(f"Invalid wheel filename: {url['filename']}")
                continue
        else:
            yield url  # non-wheel packages are always included


def filter_paths_exist(urls, repository):
    """Filter out URLs whose files already exist in the repository."""
    for url in urls:
        filename = url["filename"]
        path = f"{repository}/{filename}"
        if not os.path.exists(path):
            yield url


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="Minirepo downloader")

    parser.add_argument(
        "-c",
        "--config",
        default=MINIREPO_CONFIG,
        help="Path to config file (default: ~/.minirepo)",
    )
    parser.add_argument(
        "-r", "--repository", help="Repository folder (overrides config)"
    )
    parser.add_argument(
        "-p", "--python-versions", nargs="+", help="Python versions to include"
    )
    parser.add_argument(
        "-t", "--package-types", nargs="+", help="Package types to include"
    )
    parser.add_argument("-e", "--extensions", nargs="+", help="Extensions to include")
    parser.add_argument("-P", "--platforms", nargs="+", help="Platforms to include")
    parser.add_argument(
        "--log-level",
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the log level (default: WARNING)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging (overrides --log-level)",
    )
    parser.add_argument(
        "--print-default-config",
        action="store_true",
        help="Print the default config as JSON and exit",
    )
    parser.add_argument(
        "--names-cache-ttl",
        type=int,
        default=86400,
        help="Cache TTL for package names (seconds, default=86400)",
    )
    parser.add_argument(
        "--metadata-cache-ttl",
        type=int,
        default=86400,
        help="Cache TTL for metadata (seconds, default=86400)",
    )
    parser.add_argument(
        "--clear-names-cache", action="store_true", help="Clear the names cache"
    )
    parser.add_argument(
        "--clear-metadata-cache", action="store_true", help="Clear the metadata cache"
    )

    return parser.parse_args()


def get_config(config_file, cli_args) -> dict[str, Any]:
    config = DEFAULT_CONFIG.copy()
    try:
        with open(config_file, "r") as f:
            config.update(json.load(f))
    except FileNotFoundError:
        logging.warning(f"Config file ({config_file}) not found, using defaults")

    # CLI overrides
    if cli_args.repository:
        config["repository"] = cli_args.repository
    if cli_args.python_versions:
        config["python_versions"] = cli_args.python_versions
    if cli_args.package_types:
        config["package_types"] = cli_args.package_types
    if cli_args.extensions:
        config["extensions"] = cli_args.extensions
    if cli_args.platforms:
        config["platforms"] = cli_args.platforms

    config["repository"] = os.path.expanduser(config["repository"])

    for c in sorted(config):
        print(f"{c:<15} = {config[c]}")

    print(f"Using config file {config_file}")

    return config


def main(cli_args) -> NoReturn:

    print("/******** Minirepo ********/")

    # get configuration values
    config = get_config(cli_args.config, cli_args)

    if not os.path.isdir(config["repository"]):
        os.mkdir(config["repository"])
    assert os.path.isdir(config["repository"])

    logging.info("starting minirepo mirror...")

    # Fetch the complete list of available packages
    logging.info("getting packages names...")

    names = asyncio.run(
        main=get_names_cached(
            ttl=cli_args.names_cache_ttl,
            cache_path=Path(config["repository"]) / ".names_cache",
            clear_cache=cli_args.clear_names_cache,
        )
    )

    print(f"Got {len(names)} names")

    # Fetch package meta data
    package_metadata = asyncio.run(
        main=fetch_meta_data_cached(
            names,
            ttl=cli_args.metadata_cache_ttl,
            cache_path=Path(config["repository"]) / ".metadata_cache",
            clear_cache=cli_args.clear_metadata_cache,
        )
    )

    print(f"Got {len(package_metadata)} package_metadata")

    # Filter out any versions that are already downloaded
    # TBD

    [print(f"string packages: {pkg}" for pkg in package_metadata if type(pkg) is str)]

    urls = [url for pkg in package_metadata for url in pkg["urls"]]

    print(f"Got {len(urls)} urls")

    filtered = list(filter_paths_exist(urls, repository=config["repository"]))
    print(f"After filter_paths_exist {len(filtered)} filtered")

    filtered = list(filter_in_platforms(filtered, platforms=config["platforms"]))
    print(f"After filter_in_platforms {len(filtered)} filtered")
    filtered = list(filter_in_extensions(filtered, extensions=config["extensions"]))
    print(f"After filter_in_extensions {len(filtered)} filtered")
    filtered = list(
        filter_in_package_types(filtered, package_types=config["package_types"])
    )
    print(f"After filter_in_package_types {len(filtered)} filtered")
    filtered = list(
        filter_in_python_versions(filtered, python_versions=config["python_versions"])
    )

    print(f"After filter {len(filtered)} filtered")

    # print(f"Filtered URLs: {[url['filename'] for url in filtered[:10]]}")

    # sys.exit()
    asyncio.run(fetch_urls(urls=filtered, repository=config["repository"]))

    sys.exit()


if __name__ == "__main__":
    args: argparse.Namespace = parse_args()

    if args.print_default_config:
        print(json.dumps(DEFAULT_CONFIG, indent=4))
        sys.exit(0)

    # Set log level
    log_level = (
        logging.DEBUG if args.debug else getattr(logging, args.log_level.upper())
    )
    logging.basicConfig(
        level=log_level, format="%(asctime)s:%(levelname)s: %(message)s"
    )

    main(args)
