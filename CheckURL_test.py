import os
import requests
import socket
import csv
import sys
import subprocess
import json
from urllib.parse import urlparse
from geopy.geocoders import Nominatim
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Ensure output folder exists
def ensure_output_folder(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

# Cache for IP -> location
ip_cache = {}
ip_cache_lock = threading.Lock()

# Lock for writing to CSV
csv_lock = threading.Lock()

# Check if the radio URL is available
def check_radio_url(url):
    try:
        response = requests.get(url, timeout=10, stream=True)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

# Get IP address from URL
def get_ip_from_url(url):
    domain = urlparse(url).hostname
    try:
        return socket.gethostbyname(domain)
    except socket.gaierror:
        return None

# Get server info from IP with caching
def get_server_info(ip_address):
    with ip_cache_lock:
        if ip_address in ip_cache:
            return ip_cache[ip_address]
    try:
        url = f"http://ipinfo.io/{ip_address}/json?token=604518a7c453f5"
        response = requests.get(url, timeout=5)
        data = response.json()
        loc = data.get("loc", "").split(",")
        latitude = loc[0] if len(loc) > 0 and loc[0] else "?"
        longitude = loc[1] if len(loc) > 1 and loc[1] else "?"
        with ip_cache_lock:
            ip_cache[ip_address] = (latitude, longitude)
        return latitude, longitude
    except Exception as e:
        print(f"Error in get_server_info for {ip_address}: {e}")
        return "?", "?"

# Reverse-geocode coordinates to country
def reverse_geocode(latitude, longitude):
    geolocator = Nominatim(user_agent="GeoCheckerApp", timeout=10)
    try:
        location = geolocator.reverse((float(latitude), float(longitude)), language='en', exactly_one=True)
        if location:
            address = location.raw.get('address', {})
            country = address.get('country', '?')
            country_code = address.get('country_code', '?')
            return country, country_code
        return '?', '?'
    except Exception as e:
        print(f"Error in reverse geocoding: {e}")
        return '?', '?'

# Get audio stream info using ffprobe
def get_audio_stream_info(url):
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-select_streams', 'a',
        '-show_entries', 'stream=codec_name,sample_rate,bit_rate,channels,channel_layout,codec_type',
        '-of', 'json',
        url
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
    except Exception as e:
        print(f"ffprobe error for URL {url}: {e}")
        return "?", "?", "?", "?", "?"

# Get ICY metadata safely
def get_icy_metadata(url):
    headers = {"Icy-MetaData": "1", "User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, stream=True, timeout=10)
        return {
            'icy-br': response.headers.get('icy-br', '?'),
            'icy-description': response.headers.get('icy-description', '?'),
            'icy-genre': response.headers.get('icy-genre', '?'),
            'icy-name': response.headers.get('icy-name', '?'),
            'icy-pub': response.headers.get('icy-pub', '?')
        }
    except Exception as e:
        print(f"Error getting ICY metadata for {url}: {e}")
        return {'icy-br': '?', 'icy-description': '?', 'icy-genre': '?', 'icy-name': '?', 'icy-pub': '?'}

# Process a single URL
def process_radio(row):
    name = row.get('name', '?')
    url = row.get('url', '?')
    availability = "1" if check_radio_url(url) else "0"

    country, country_code, latitude, longitude = "?", "?", "?", "?"
    codec, sample_rate, bitrate, channels, channel_layout = "?", "?", "?", "?", "?"
    icy_metadata = {'icy-br': '?', 'icy-description': '?', 'icy-genre': '?', 'icy-name': '?', 'icy-pub': '?'}

    if availability == "1":
        ip_address = get_ip_from_url(url)
        if ip_address:
            latitude, longitude = get_server_info(ip_address)
            if latitude != "?" and longitude != "?":
                country, country_code = reverse_geocode(latitude, longitude)

        codec, sample_rate, bitrate, channels, channel_layout = get_audio_stream_info(url)
        icy_metadata = get_icy_metadata(url)

    # Console output
    sys.stdout.write(
        f"{url} | {availability} | {country} | {country_code} | {latitude} | {longitude} | "
        f"{codec} | {sample_rate} | {bitrate} | {channels} | {channel_layout} | "
        f"{icy_metadata['icy-br']} | {icy_metadata['icy-description']} | {icy_metadata['icy-genre']} | "
        f"{icy_metadata['icy-name']} | {icy_metadata['icy-pub']}\n"
    )

    return {
        'name': name,
        'url': url,
        'availability': availability,
        'country': country,
        'country_code': country_code,
        'latitude': latitude,
        'longitude': longitude,
        'codec': codec,
        'sample_rate': sample_rate,
        'bitrate': bitrate,
        'channels': channels,
        'channel_layout': channel_layout,
        'icy-br': icy_metadata['icy-br'],
        'icy-description': icy_metadata['icy-description'],
        'icy-genre': icy_metadata['icy-genre'],
        'icy-name': icy_metadata['icy-name'],
        'icy-pub': icy_metadata['icy-pub']
    }

# Main function
def process_csv_parallel(input_file, output_file, max_workers=5):
    ensure_output_folder(output_file)
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:

        reader = list(csv.DictReader(infile))
        fieldnames = [
            'name', 'url', 'availability',
            'country', 'country_code',
            'latitude', 'longitude',
            'codec', 'sample_rate', 'bitrate', 'channels', 'channel_layout',
            'icy-br', 'icy-description', 'icy-genre', 'icy-name', 'icy-pub'
        ]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_row = {executor.submit(process_radio, row): row for row in reader}

            for future in as_completed(future_to_row):
                result = future.result()
                with csv_lock:
                    writer.writerow(result)
                sleep(1)  # politeness delay per thread

# Main program
if __name__ == '__main__':
    input_csv = 'input/radio_urls.csv'
    output_csv = 'output/radio_results.csv'
    process_csv_parallel(input_csv, output_csv, max_workers=5)
