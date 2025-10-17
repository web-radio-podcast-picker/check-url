import os
import csv
import sys
import socket
import json
import asyncio
import aiohttp
import subprocess
from urllib.parse import urlparse
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import threading
import logging
import io
import random

# ------------------------------
# Helper: Clean strings to remove broken Unicode
# ------------------------------
def clean_text(s):
    if not isinstance(s, str):
        return s
    return s.encode('utf-8', 'replace').decode('utf-8')

# ------------------------------
# Safe console stream
# ------------------------------
class SafeStream(io.TextIOBase):
    def __init__(self, stream):
        self.stream = stream
    def write(self, s):
        if not isinstance(s, str):
            s = str(s)
        safe_s = s.encode('utf-8', 'replace').decode('utf-8')
        self.stream.write(safe_s)
        self.stream.flush()
    def flush(self):
        self.stream.flush()

# ------------------------------
# Logging Setup
# ------------------------------
log_filename = "logs/output.log"
os.makedirs(os.path.dirname(log_filename), exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8", errors='replace'),
        logging.StreamHandler(SafeStream(sys.stdout))
    ]
)

def safe_print(*args, **kwargs):
    text = " ".join(str(a) for a in args)
    logging.debug(clean_text(text))
print = safe_print
sys.stderr = sys.stdout

# ------------------------------
# Locks and caches
# ------------------------------
csv_lock = threading.Lock()
entry_lock = threading.Lock()
ip_cache_lock = threading.Lock()
ip_cache = {}
geo_cache_lock = threading.Lock()
geo_cache = {}

# ------------------------------
# Helpers
# ------------------------------
def ensure_output_folder(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def load_existing_entries(output_file):
    existing = set()
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f, delimiter='🙈')
            for row in reader:
                existing.add((row.get('name', '?'), row.get('url', '?')))
    return existing

# ------------------------------
# Retry helper
# ------------------------------
async def async_retry(func, *args, retries=3, delay=1, backoff=2, exceptions=(Exception,), **kwargs):
    """Retry async function with exponential backoff."""
    for attempt in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except exceptions as e:
            if attempt == retries:
                return None
            sleep_time = delay * (backoff ** (attempt - 1)) + random.random() * 0.1
            await asyncio.sleep(sleep_time)

# ------------------------------
# Async HTTP check
# ------------------------------
async def check_radio_url(session, url):
    try:
        async with session.get(url, timeout=10) as resp:
            return resp.status == 200
    except:
        raise

# ------------------------------
# Get IP from URL
# ------------------------------
def get_ip_from_url(url):
    domain = urlparse(url).hostname
    try:
        return socket.gethostbyname(domain)
    except:
        return None

# ------------------------------
# Async IP info with caching
# ------------------------------
async def get_server_info(session, ip_address):
    with ip_cache_lock:
        if ip_address in ip_cache:
            return ip_cache[ip_address]
    try:
        async with session.get(f"http://ipinfo.io/{ip_address}/json?token=604518a7c453f5", timeout=5) as resp:
            data = await resp.json()
            loc = data.get("loc", "").split(",")
            latitude = loc[0] if len(loc) > 0 and loc[0] else "?"
            longitude = loc[1] if len(loc) > 1 and loc[1] else "?"
            with ip_cache_lock:
                ip_cache[ip_address] = (latitude, longitude)
            return latitude, longitude
    except:
        raise

# ------------------------------
# Async reverse geocode
# ------------------------------
async def reverse_geocode(latitude, longitude):
    key = (latitude, longitude)
    with geo_cache_lock:
        if key in geo_cache:
            return geo_cache[key]
    try:
        geolocator = Nominatim(user_agent="GeoCheckerApp", timeout=10)
        location = await asyncio.to_thread(
            geolocator.reverse, (float(latitude), float(longitude)),
            language='en', exactly_one=True
        )
        if location:
            address = location.raw.get('address', {})
            country = address.get('country', '?')
            country_code = address.get('country_code', '?')
            with geo_cache_lock:
                geo_cache[key] = (country, country_code)
            return country, country_code
        return '?', '?'
    except (GeocoderTimedOut, Exception):
        return '?', '?'

# ------------------------------
# Async ffprobe
# ------------------------------
async def get_audio_stream_info_async(url):
    cmd = [
        'ffprobe', '-v', 'error', '-select_streams', 'a',
        '-show_entries', 'stream=codec_name,sample_rate,bit_rate,channels,channel_layout,codec_type',
        '-of', 'json', url
    ]
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        info = json.loads(stdout.decode() if stdout else '{}')
        streams = info.get("streams", [])
        audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), {})
        codec = audio_stream.get("codec_name", "?")
        sample_rate = audio_stream.get("sample_rate", "?")
        bitrate = audio_stream.get("bit_rate", "?")
        channels = audio_stream.get("channels", "?")
        channel_layout = audio_stream.get("channel_layout", "?")
        return codec, sample_rate, bitrate, channels, channel_layout
    except:
        raise

# ------------------------------
# Async ICY metadata
# ------------------------------
async def get_icy_metadata(session, url):
    headers = {"Icy-MetaData": "1", "User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(url, headers=headers, timeout=10) as resp:
            return {
                'icy-br': resp.headers.get('icy-br', '?'),
                'icy-description': resp.headers.get('icy-description', '?'),
                'icy-genre': resp.headers.get('icy-genre', '?'),
                'icy-name': resp.headers.get('icy-name', '?'),
                'icy-pub': resp.headers.get('icy-pub', '?')
            }
    except:
        raise

# ------------------------------
# Process a single radio
# ------------------------------
async def process_radio(row, session, existing_entries, entry_lock):
    name = clean_text(row.get('name', '?'))
    url = clean_text(row.get('url', '?'))
    key = (name, url)

    with entry_lock:
        if key in existing_entries:
            print(f"Skipping already processed: {key}")
            return None
        existing_entries.add(key)

    availability = "1" if await async_retry(check_radio_url, session, url, retries=3) else "0"
    country, country_code, latitude, longitude = "?", "?", "?", "?"
    codec, sample_rate, bitrate, channels, channel_layout = "?", "?", "?", "?", "?"
    icy_metadata = {'icy-br': '?', 'icy-description': '?', 'icy-genre': '?', 'icy-name': '?', 'icy-pub': '?'}

    if availability == "1":
        ip_address = get_ip_from_url(url)
        if ip_address:
            latlong = await async_retry(get_server_info, session, ip_address, retries=3)
            if latlong:
                latitude, longitude = latlong
                if latitude != "?" and longitude != "?":
                    country, country_code = await reverse_geocode(latitude, longitude)
        audio_info = await async_retry(get_audio_stream_info_async, url, retries=2)
        if audio_info:
            codec, sample_rate, bitrate, channels, channel_layout = audio_info
        icy = await async_retry(get_icy_metadata, session, url, retries=2)
        if icy:
            icy_metadata = icy

    country, country_code, latitude, longitude = map(clean_text, [country, country_code, latitude, longitude])
    codec, sample_rate, bitrate, channels, channel_layout = map(clean_text, [codec, sample_rate, bitrate, channels, channel_layout])
    icy_metadata = {k: clean_text(v) for k, v in icy_metadata.items()}

    print(
        f"{name} 🙈 {url} 🙈 {availability} 🙈 {country} 🙈 {country_code} 🙈 {latitude} 🙈 {longitude} 🙈 "
        f"{codec} 🙈 {sample_rate} 🙈 {bitrate} 🙈 {channels} 🙈 {channel_layout} 🙈 "
        f"{icy_metadata['icy-br']} 🙈 {icy_metadata['icy-description']} 🙈 {icy_metadata['icy-genre']} 🙈 "
        f"{icy_metadata['icy-name']} 🙈 {icy_metadata['icy-pub']}"
    )

    return {
        'name': name, 'url': url, 'availability': availability,
        'country': country, 'country_code': country_code,
        'latitude': latitude, 'longitude': longitude,
        'codec': codec, 'sample_rate': sample_rate, 'bitrate': bitrate,
        'channels': channels, 'channel_layout': channel_layout,
        'icy-br': icy_metadata['icy-br'],
        'icy-description': icy_metadata['icy-description'],
        'icy-genre': icy_metadata['icy-genre'],
        'icy-name': icy_metadata['icy-name'],
        'icy-pub': icy_metadata['icy-pub']
    }

# ------------------------------
# Main CSV processor
# ------------------------------
async def process_csv_async(input_file, output_file, concurrency=50):
    ensure_output_folder(output_file)
    existing_entries = load_existing_entries(output_file)

    outfile = open(output_file, 'a', newline='', encoding='utf-8', errors='replace')
    fieldnames = [
        'name','url','availability','country','country_code','latitude','longitude',
        'codec','sample_rate','bitrate','channels','channel_layout',
        'icy-br','icy-description','icy-genre','icy-name','icy-pub'
    ]
    writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter='🙈')
    if os.path.getsize(output_file) == 0:
        writer.writeheader()

    try:
        with open(input_file, 'r', encoding='utf-8', errors='replace') as infile:
            reader = list(csv.DictReader(infile, delimiter='🙈'))

        sem = asyncio.Semaphore(concurrency)

        async with aiohttp.ClientSession() as session:
            async def sem_task(row):
                async with sem:
                    result = await process_radio(row, session, existing_entries, entry_lock)
                    if result:
                        with csv_lock:
                            writer.writerow(result)
                    await asyncio.sleep(0.01)

            tasks = [sem_task(row) for row in reader]
            await asyncio.gather(*tasks)

    finally:
        outfile.close()

# ------------------------------
# Run
# ------------------------------
if __name__ == '__main__':
    input_csv = 'input/radio_urls.csv'
    output_csv = 'output/radio_results.csv'
    try:
        asyncio.run(process_csv_async(input_csv, output_csv, concurrency=50))
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("🛑 Stopped by user. Goodbye!")