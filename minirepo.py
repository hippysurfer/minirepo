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
import itertools
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
    connect=60,  # time to establish connection
    sock_connect=60,  # time to wait before socket connect
    sock_read=100,  # time to wait between reads
)

WORKERS = 10  # number of concurrent download workers

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


def minify_meta(json):
    """Minify the package metadata to only include necessary fields."""
    
    meta = {
        "info": {
            "name": json.get("info", {}).get("name"),
            "version": json.get("info", {}).get("version"),
        }}
    meta['releases'] = {}
    for version, files, in json.get("releases", {}).items():
        meta['releases'][version] = [
            {'filename': file.get('filename') for file in files}]
        
    meta['urls'] = []
    for url in json.get("urls", []):
        meta['urls'].append(
        {'url': url.get('url'), 
         'filename': url.get('filename'),
         'packagetype': url.get('packagetype'),
         'python_version': url.get('python_version'),
         'size': url.get('size'),})
        
    return meta
    
async def fetch(url: str, session: ClientSession):
    """Download a single URL using aiohttp with concurrency limit."""
    
    try:
        async with session.get(url, timeout=TIMEOUT) as response:
            if response.status != 200:
                logging.debug(f"Failed to fetch {url}: {response.status}")
                return None
            json = await response.json()
            return minify_meta(json)
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}", exc_info=True)

def sanitize_json(obj):
    """Recursively replace newlines in all string values in a dict."""
    if isinstance(obj, dict):
        return {k: sanitize_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_json(v) for v in obj]
    elif isinstance(obj, str):
        return obj.replace("\n", "\\n")
    else:
        return obj
    

async def fetch_meta_data(
    names, 
    num_workers, 
    cache_path,
    url="https://pypi.python.org/pypi"
):
    """Fetch metadata for a list of package names and stream to a file."""

    urls = [f"{url}/{name}/json" for name in names]
    queue = asyncio.Queue()

    for u in urls:
        await queue.put(u)

    cache_path.write_text("")
    
    async with aiohttp.ClientSession() as session, aiofiles.open(cache_path, "w") as out_file:
        write_lock = asyncio.Lock()
        with tqdm_asyncio(
            total=len(urls),
            unit="pkg",
            unit_scale=True,
            desc="Fetching metadata",
        ) as pbar:

            async def worker():
                while True:
                    try:
                        u = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    meta = await fetch(u, session)
                    if meta is not None:
                        meta = sanitize_json(meta)
                        async with write_lock:
                            await out_file.write(json.dumps(meta, ensure_ascii=True) + "\n")
                    pbar.update(1)
                    queue.task_done()

            tasks = [asyncio.create_task(worker()) for _ in range(num_workers)]
            await queue.join()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            
    return cache_path

async def fetch_meta_data_cached(
    names, ttl, cache_path, 
    clear_cache=False,
    num_workers=WORKERS,
    url="https://pypi.python.org/pypi"
):
    """Fetch package metadata with caching."""
    if clear_cache:
        cache_path.unlink(missing_ok=True)
    if cache_path.exists():
        mtime = cache_path.stat().st_mtime
        if time.time() - mtime < ttl:
            logging.info("Loaded package metadata from cache")
            return stream_metadata_rows(path=cache_path)
    
    # Fallback to fetch
    cache_path = await fetch_meta_data(
        names, cache_path=cache_path,
        num_workers=num_workers, url=url)
    
    return stream_metadata_rows(path=cache_path)

def unsanitize_json(obj):
    """Recursively convert '\\n' back to '\n' in all string values."""
    if isinstance(obj, dict):
        return {k: unsanitize_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [unsanitize_json(v) for v in obj]
    elif isinstance(obj, str):
        return obj.replace("\\n", "\n")
    else:
        return obj

def stream_metadata_rows(path):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            try:
                obj = json.loads(line)
                yield unsanitize_json(obj)
            except json.JSONDecodeError as e:
                logging.warning(f"Skipping invalid JSON line: {e}: content={line}")
                continue
            
def prune_to_latest_version(package_metadata, repository, dry_run=False):
    """
    For each package, keep only files for the latest version.
    Removes files for older versions.
    """
    pruned = []
    for pkg in package_metadata:
        latest_version = pkg.get("info", {}).get("version")
        releases = pkg.get("releases", [])
        # Only keep URLs matching the latest version
        try:
            logging.debug(f"Pruning package {pkg.get('info', {}).get('name')} to version {latest_version}")
            filtered_releases = [
                (version, releases[version]) for version in releases
                if version != latest_version
            ]
            logging.debug(f"Found {len(filtered_releases)} old version URLs to prune")
            old_versions = [version for version,release in filtered_releases]
            logging.debug(f"Old version URLs: {old_versions}")
            filtered_filenames = []
            for version, releases in filtered_releases:
                for release in releases:
                    if (release.get('filename') and 
                        Path(repository).joinpath(release.get('filename')).exists()):
                        filtered_filenames.append(release.get('filename'))
            logging.debug(f"Filtered filenames to remove: {filtered_filenames}")
            if not dry_run:
                for file_name in filtered_filenames:
                    path = Path(repository) / file_name
                    if path.exists():
                        logging.debug(f"Removing old version file: {path}")
                        path.unlink(missing_ok=True)
            pruned.extend(filtered_filenames)
        except InvalidFilenameError as e:
            logging.warning(f"Invalid wheel filename while pruning: {e}")
            continue
    return pruned

async def fetch_file(
    url: str, session: ClientSession, repository
):
    """Download a single URL using aiohttp with concurrency limit."""

    filename = Path(url).name
    file_path = Path(repository) / filename
    logging.debug(f"Writing to: {file_path}")
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



async def fetch_urls(urls, repository, num_workers=WORKERS):
    """Fetch multiple URLs concurrently using a fixed number of worker tasks and a queue."""

    urls = list(urls)
    
    total_bytes = sum(url["size"] for url in urls)
    results = []
    queue = asyncio.Queue()

    for url in urls:
        await queue.put(url)

    async with aiohttp.ClientSession() as session:
        with tqdm_asyncio(
            total=total_bytes,
            unit="GB",
            unit_scale=1 / 1e9,
            unit_divisor=1,
            bar_format="{l_bar}{bar} {n:.2f}/{total:.2f} {unit} "
                    "[{elapsed}<{remaining}, {rate_fmt}]",
            desc="Downloading",
        ) as pbar:

            async def worker():
                while True:
                    try:
                        url = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    filename = await fetch_file(
                        url["url"], session, repository=repository
                    )
                    if filename is not None:
                        pbar.update(url["size"])
                        results.append(filename)
                    queue.task_done()

            tasks = [asyncio.create_task(worker()) for _ in range(num_workers)]
            await queue.join()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

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
        logging.debug(f"{c:<15} = {config[c]}")

    logging.debug(f"Using config file {config_file}")

    return config


def main(cli_args) -> NoReturn:

    logging.info("/******** Minirepo ********/")

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

    logging.info(f"Got {len(names)} names")

    if cli_args.limit:
        names = names[: cli_args.limit]
        logging.info(f"Limiting to first {len(names)} names")   
        
    # Fetch package meta data
    package_metadata = asyncio.run(
        main=fetch_meta_data_cached(
            names,
            num_workers=cli_args.num_workers,
            ttl=cli_args.metadata_cache_ttl,
            cache_path=Path(config["repository"]) / ".metadata_cache",
            clear_cache=cli_args.clear_metadata_cache,
        )
    )

    #logging.info(f"Got {len(list(package_metadata))} package_metadata")

    #logging.debug("Sample package metadata:")
    #for pkg in itertools.islice(package_metadata, 3):
    #    logging.debug(json.dumps(pkg, indent=4))
    urls = (url for pkg in package_metadata for url in pkg["urls"])

    #logging.info(f"Got {len(urls)} urls")

    filtered = filter_paths_exist(urls, repository=config["repository"])
    #logging.info(f"After filter_paths_exist {len(filtered)} filtered")

    filtered = filter_in_platforms(filtered, platforms=config["platforms"])
    
    #logging.info(f"After filter_in_platforms {len(filtered)} filtered")
    filtered = filter_in_extensions(filtered, extensions=config["extensions"])
    
    #logging.info(f"After filter_in_extensions {len(filtered)} filtered")
    filtered = filter_in_package_types(filtered, package_types=config["package_types"]
    )
    #logging.info(f"After filter_in_package_types {len(filtered)} filtered")
    filtered = filter_in_python_versions(filtered, python_versions=config["python_versions"])

    #logging.info(f"After filter {len(filtered)} filtered")
    
    asyncio.run(fetch_urls(
        urls=filtered, 
        repository=config["repository"],
        num_workers=cli_args.num_workers))

    if cli_args.prune:
        logging.info("Pruning old package versions...")
        pruned_files = prune_to_latest_version(
            package_metadata, repository=config["repository"], dry_run=False
        )
        logging.info(f"Pruned {len(pruned_files)} old package files")
    sys.exit()

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
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (-v = INFO, -vv = DEBUG, default = WARNING)",
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
        "--prune",
        action="store_true",
        help="Prune old package versions from the repository after download",
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
    parser.add_argument(
        "--limit", type=int, default=0, help="Limit to first N packages (for testing)"
    )
    parser.add_argument(
        "--num-workers", type=int, default=10, help="Number of concurrent workers (default=10)"
    )

    return parser.parse_args()

if __name__ == "__main__":
    args: argparse.Namespace = parse_args()

    if args.print_default_config:
        print(json.dumps(DEFAULT_CONFIG, indent=4))
        sys.exit(0)

    # Set log level
    if args.verbose >= 2:
        log_level = logging.DEBUG
    elif args.verbose == 1:
        log_level = logging.INFO
    else:
        log_level = logging.WARNING
        
    if args.debug:
        log_level = logging.DEBUG
        
    logging.basicConfig(
        level=log_level, format="%(asctime)s:%(levelname)s: %(message)s"
    )

    main(args)
