import requests
import socket
import csv
import sys
import subprocess
import json
import math
import colorsys
from urllib.parse import urlparse
from geopy.geocoders import Nominatim
from time import sleep
from collections import defaultdict
import folium  # For map generation
from folium.plugins import MarkerCluster

# Function to check if the radio URL is available
def check_radio_url(url):
    try:
        response = requests.get(url, timeout=10, stream=True)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

# Function to get the IP address of the server
def get_ip_from_url(url):
    domain = urlparse(url).hostname
    try:
        ip_address = socket.gethostbyname(domain)
        return ip_address
    except socket.gaierror:
        return None

# Function to get server location info from IP
def get_server_info(ip_address):
    try:
        url = f"http://ipinfo.io/{ip_address}/json?token=604518a7c453f5"
        response = requests.get(url, timeout=5)
        data = response.json()
        # Debug print
        print(f"IPinfo for {ip_address}: {data}")

        country_code = data.get("country", "?")
        loc = data.get("loc", "").split(",")
        latitude = loc[0] if len(loc) > 0 and loc[0] != "" else "?"
        longitude = loc[1] if len(loc) > 1 and loc[1] != "" else "?"

        return country_code, country_code, latitude, longitude
    except Exception as e:
        print(f"Error in get_server_info for {ip_address}: {e}")
        return "?", "?", "?", "?"


# Reverse-geocode coordinates to a country
def reverse_geocode(latitude, longitude):
    geolocator = Nominatim(user_agent="GeoCheckerApp", timeout=10)
    try:
        location = geolocator.reverse((latitude, longitude), language='en', exactly_one=True)
        if location:
            address = location.raw.get('address', {})
            country = address.get('country', '?')
            country_code = address.get('country_code', '?')
            return country, country_code
        else:
            return '?', '?'
    except Exception as e:
        print(f"Error in reverse geocoding: {e}")
        return '?', '?'

# Function to get audio stream properties using ffprobe
def get_audio_stream_info(url):
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-select_streams', 'a:0',
        '-show_entries', 'stream=codec_name,sample_rate,bit_rate,channels',
        '-of', 'json',
        url
    ]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        info = json.loads(result.stdout)
        stream = info.get("streams", [])[0] if info.get("streams") else {}

        codec = stream.get("codec_name", "?")
        sample_rate = stream.get("sample_rate", "?")
        bitrate = stream.get("bit_rate", "?")
        channels = stream.get("channels", "?")
        channel_layout = stream.get("channel_layout", "?")

        return codec, sample_rate, bitrate, channels, channel_layout

    except Exception as e:
        print(f"ffprobe error for URL {url}: {e}")
        return "?", "?", "?", "?", "?"

# Function to get ICY metadata from a stream
def get_icy_metadata(url):
    headers = {
        "Icy-MetaData": "1",
        "User-Agent": "Mozilla/5.0"
    }

    try:
        response = requests.get(url, headers=headers, stream=True, timeout=10)
        icy_metadata = {
            'icy-br': response.headers.get('icy-br', '?'),
            'icy-description': response.headers.get('icy-description', '?'),
            'icy-genre': response.headers.get('icy-genre', '?'),
            'icy-name': response.headers.get('icy-name', '?'),
            'icy-pub': response.headers.get('icy-pub', '?'),
            'StreamTitle': '?'
        }

        if 'icy-metaint' in response.headers:
            metaint = int(response.headers['icy-metaint'])
            response.raw.read(metaint)  # skip audio data
            metadata_length = int.from_bytes(response.raw.read(1), 'big') * 16
            metadata = response.raw.read(metadata_length).decode('utf-8', errors='ignore')
            if "StreamTitle=" in metadata:
                icy_metadata['StreamTitle'] = metadata.split("StreamTitle='")[1].split("';")[0]

        return icy_metadata

    except Exception as e:
        print(f"Error getting ICY metadata for {url}: {e}")
        return {
            'icy-br': '?',
            'icy-description': '?',
            'icy-genre': '?',
            'icy-name': '?',
            'icy-pub': '?',
            'StreamTitle': '?'
        }

# Function to create a map from the CSV output
def get_color_intensity(n, max_n=10):
    # Normalize count to [0, 1], then convert to RGB red hue with varying lightness
    norm = min(n / max_n, 1.0)
    h, l, s = 0, 1 - norm * 0.5, 1  # Red hue, lower lightness = darker
    r, g, b = colorsys.hls_to_rgb(h, l, s)
    return f'#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}'

def create_map_from_csv(output_file, map_file='map.html'):
    m = folium.Map(location=[20, 0], zoom_start=2)
    coord_groups = defaultdict(list)

    # Group entries by lat/lon
    with open(output_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            lat = row['latitude']
            lon = row['longitude']
            url = row['url']
            if lat not in ['Unknown', '', None] and lon not in ['Unknown', '', None]:
                try:
                    key = (float(lat), float(lon))
                    coord_groups[key].append(url)
                except ValueError:
                    continue

    max_count = max(len(urls) for urls in coord_groups.values())

    for (lat, lon), urls in coord_groups.items():
        count = len(urls)
        color = get_color_intensity(count, max_n=max_count)
        radius = 5 + count * 2  # Bigger for more sources

        popup_html = "<b>Stations at this location:</b><br>" + "<br>".join(urls)

        folium.CircleMarker(
            location=[lat, lon],
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.85,
            popup=folium.Popup(popup_html, max_width=400)
        ).add_to(m)

    m.save(map_file)
    print(f"📍 Map saved with intensity-based markers → {map_file}")

# Main function to process CSV input and generate output
def process_csv(input_file, output_file):
    with open(input_file, 'r') as infile:
        reader = csv.DictReader(infile)

        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            fieldnames = [
                'url', 'availability',
                'country', 'country_code',
                'latitude', 'longitude',
                'reverse_country', 'reverse_country_code',
                'codec', 'sample_rate', 'bitrate', 'channels', 'channel_layout',
                'icy-br', 'icy-description', 'icy-genre', 'icy-name', 'icy-pub', 'StreamTitle'
            ]
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                url = row['url']
                availability = "1" if check_radio_url(url) else "0"

                # Default values for all optional fields
                reverse_country, reverse_country_code = "?", "?"
                country, country_code, latitude, longitude = "?", "?", "?", "?"
                codec, sample_rate, bitrate, channels, channel_layout = "?", "?", "?", "?", "?"
                icy_metadata = {
                    'icy-br': '?',
                    'icy-description': '?',
                    'icy-genre': '?',
                    'icy-name': '?',
                    'icy-pub': '?',
                    'StreamTitle': '?'
                }

                if availability == "1":
                    ip_address = get_ip_from_url(url)
                    if ip_address:
                        country, country_code, latitude, longitude = get_server_info(ip_address)

                        if latitude != "?" and longitude != "?":
                            reverse_country, reverse_country_code = reverse_geocode(latitude, longitude)
                            country = reverse_country if country == "?" else country
                            country_code = reverse_country_code if country_code == "?" else country_code

                    codec, sample_rate, bitrate, channels, channel_layout = get_audio_stream_info(url)
                    icy_metadata = get_icy_metadata(url)

                # Console output (optional)
                sys.stdout.write(
                    f"{url} | {availability} | {country} | {country_code} | {latitude} | {longitude} | "
                    f"{reverse_country} | {reverse_country_code} | {codec} | {sample_rate} | {bitrate} | {channels} | {channel_layout} | "
                    f"{icy_metadata['icy-br']} | {icy_metadata['icy-description']} | {icy_metadata['icy-genre']} | "
                    f"{icy_metadata['icy-name']} | {icy_metadata['icy-pub']} | {icy_metadata['StreamTitle']}\n"
                )

                # Write row to CSV
                writer.writerow({
                    'url': url,
                    'availability': availability,
                    'country': country,
                    'country_code': country_code,
                    'latitude': latitude,
                    'longitude': longitude,
                    'reverse_country': reverse_country,
                    'reverse_country_code': reverse_country_code,
                    'codec': codec,
                    'sample_rate': sample_rate,
                    'bitrate': bitrate,
                    'channels': channels,
                    'channel_layout': channel_layout,
                    'icy-br': icy_metadata['icy-br'],
                    'icy-description': icy_metadata['icy-description'],
                    'icy-genre': icy_metadata['icy-genre'],
                    'icy-name': icy_metadata['icy-name'],
                    'icy-pub': icy_metadata['icy-pub'],
                    'StreamTitle': icy_metadata['StreamTitle']
                })

                # Be kind to external services
                sleep(1)

    # Create map after CSV is written
    create_map_from_csv(output_file)

# Main program
if __name__ == '__main__':
    input_csv = 'radio_urls.csv'         # Input file
    output_csv = 'radio_results.csv'     # Output file
    process_csv(input_csv, output_csv)
