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
from datetime import datetime
import logging

# --------------------------------------------------
# Logging Setup
# --------------------------------------------------
log_filename = "../logs/output.log"
os.makedirs(os.path.dirname(log_filename), exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,  # Capture everything
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

# Redirect all print() calls to logging.debug()
print = logging.debug

# Redirect errors into log as well
sys.stderr = sys.stdout

# --------------------------------------------------
# Original Script Below (unchanged logic)
# --------------------------------------------------

# Ensure output folder exists
def ensure_output_folder(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

# CSV writing lock
csv_lock = threading.Lock()
# IP cache lock
ip_cache_lock = threading.Lock()
ip_cache = {}
# Reverse geocode cache
geo_cache_lock = threading.Lock()
geo_cache = {}

# Load existing entries from output CSV
def load_existing_entries(output_file):
    existing = set()
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='🙈')
            for row in reader:
                existing.add((row.get('name', '?'), row.get('url', '?')))
    return existing

# Async HTTP check for URL availability
async def check_radio_url(session, url):
    try:
        async with session.get(url, timeout=10) as resp:
            return resp.status == 200
    except:
        return False

# Get IP from URL
def get_ip_from_url(url):
    domain = urlparse(url).hostname
    try:
        return socket.gethostbyname(domain)
    except:
        return None

# Async IP info with caching
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
        return "?", "?"

# Async reverse geocode using Nominatim
async def reverse_geocode(latitude, longitude):
    key = (latitude, longitude)
    with geo_cache_lock:
        if key in geo_cache:
            return geo_cache[key]
    try:
        geolocator = Nominatim(user_agent="GeoCheckerApp", timeout=10)
        location = await asyncio.to_thread(geolocator.reverse, (float(latitude), float(longitude)), language='en', exactly_one=True)
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

# Audio info via ffprobe (sync)
def get_audio_stream_info(url):
    cmd = [
        'ffprobe', '-v', 'error', '-select_streams', 'a',
        '-show_entries', 'stream=codec_name,sample_rate,bit_rate,channels,channel_layout,codec_type',
        '-of', 'json', url
    ]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        info = json.loads(result.stdout)
        streams = info.get("streams", [])
        audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), {})
        codec = audio_stream.get("codec_name", "?")
        sample_rate = audio_stream.get("sample_rate", "?")
        bitrate = audio_stream.get("bit_rate", "?")
        channels = audio_stream.get("channels", "?")
        channel_layout = audio_stream.get("channel_layout", "?")
        return codec, sample_rate, bitrate, channels, channel_layout
    except:
        return "?", "?", "?", "?", "?"

# Async ICY metadata
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
        return {'icy-br': '?', 'icy-description': '?', 'icy-genre': '?', 'icy-name': '?', 'icy-pub': '?'}

# Process a single row
async def process_radio(row, session, existing_entries, entry_lock):
    name = row.get('name', '?')
    url = row.get('url', '?')
    key = (name, url)

    with entry_lock:
        if key in existing_entries:
            print(f"Skipping already processed: {key}")
            return None
        existing_entries.add(key)

    availability = "1" if await check_radio_url(session, url) else "0"
    country, country_code, latitude, longitude = "?", "?", "?", "?"
    codec, sample_rate, bitrate, channels, channel_layout = "?", "?", "?", "?", "?"
    icy_metadata = {'icy-br': '?', 'icy-description': '?', 'icy-genre': '?', 'icy-name': '?', 'icy-pub': '?'}

    if availability == "1":
        ip_address = get_ip_from_url(url)
        if ip_address:
            latitude, longitude = await get_server_info(session, ip_address)
            if latitude != "?" and longitude != "?":
                country, country_code = await reverse_geocode(latitude, longitude)
        codec, sample_rate, bitrate, channels, channel_layout = get_audio_stream_info(url)
        icy_metadata = await get_icy_metadata(session, url)

    # Console output
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

# Main async CSV processor
async def process_csv_async(input_file, output_file, concurrency=50):
    ensure_output_folder(output_file)
    existing_entries = load_existing_entries(output_file)
    entry_lock = threading.Lock()

    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'a', newline='', encoding='utf-8') as outfile:

        reader = list(csv.DictReader(infile, delimiter='🙈'))
        fieldnames = [
            'name','url','availability','country','country_code','latitude','longitude',
            'codec','sample_rate','bitrate','channels','channel_layout',
            'icy-br','icy-description','icy-genre','icy-name','icy-pub'
        ]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter='🙈')
        if os.path.getsize(output_file) == 0:
            writer.writeheader()

        async with aiohttp.ClientSession() as session:
            sem = asyncio.Semaphore(concurrency)

            async def sem_task(row):
                async with sem:
                    result = await process_radio(row, session, existing_entries, entry_lock)
                    if result:
                        with csv_lock:
                            writer.writerow(result)
                    await asyncio.sleep(0.05)  # polite

            tasks = [sem_task(row) for row in reader]
            await asyncio.gather(*tasks)

# Run
if __name__ == '__main__':
    input_csv = 'input/radio_urls.csv'
    output_csv = 'output/radio_results.csv'
    asyncio.run(process_csv_async(input_csv, output_csv, concurrency=50))
