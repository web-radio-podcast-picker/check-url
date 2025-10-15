import requests
import socket
import csv
import sys
import subprocess
import json
from urllib.parse import urlparse
from geopy.geocoders import Nominatim
from time import sleep


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
        response = requests.get(f"http://ipinfo.io/{ip_address}/json")
        data = response.json()

        country_code = data.get("country", "Unknown")
        loc = data.get("loc", "").split(",")
        latitude = loc[0] if len(loc) > 0 else "Unknown"
        longitude = loc[1] if len(loc) > 1 else "Unknown"

        return country_code, country_code, latitude, longitude  # country_name is not provided by ipinfo.io
    except requests.exceptions.RequestException:
        return "Unknown", "Unknown", "Unknown", "Unknown"


# Reverse-geocode coordinates to a country
def reverse_geocode(latitude, longitude):
    geolocator = Nominatim(user_agent="GeoCheckerApp")
    try:
        location = geolocator.reverse((latitude, longitude), language='en', exactly_one=True)
        if location:
            address = location.raw.get('address', {})
            country = address.get('country', 'Unknown')
            country_code = address.get('country_code', 'Unknown')
            return country, country_code
        else:
            return 'Unknown', 'Unknown'
    except Exception as e:
        print(f"Error in reverse geocoding: {e}")
        return 'Unknown', 'Unknown'


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

        codec = stream.get("codec_name", "Unknown")
        sample_rate = stream.get("sample_rate", "Unknown")
        bitrate = stream.get("bit_rate", "Unknown")
        channels = stream.get("channels", "Unknown")

        return codec, sample_rate, bitrate, channels

    except Exception as e:
        print(f"ffprobe error for URL {url}: {e}")
        return "Unknown", "Unknown", "Unknown", "Unknown"


# Main function to process CSV input and generate output
def process_csv(input_file, output_file):
    with open(input_file, 'r') as infile:
        reader = csv.DictReader(infile)

        with open(output_file, 'w', newline='') as outfile:
            fieldnames = [
                'url', 'availability',
                'country', 'country_code',
                'latitude', 'longitude',
                'reverse_country', 'reverse_country_code',
                'codec', 'sample_rate', 'bitrate', 'channels'
            ]
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                url = row['url']
                availability = "available" if check_radio_url(url) else "unavailable"

                if availability == "available":
                    ip_address = get_ip_from_url(url)
                    if ip_address:
                        country, country_code, latitude, longitude = get_server_info(ip_address)

                        if latitude != "Unknown" and longitude != "Unknown":
                            reverse_country, reverse_country_code = reverse_geocode(latitude, longitude)
                            country = reverse_country if country == "Unknown" else country
                            country_code = reverse_country_code if country_code == "Unknown" else country_code
                    else:
                        country, country_code, latitude, longitude = "Unknown", "Unknown", "Unknown", "Unknown"
                        reverse_country, reverse_country_code = "Unknown", "Unknown"

                    codec, sample_rate, bitrate, channels = get_audio_stream_info(url)

                else:
                    country, country_code, latitude, longitude = "Unknown", "Unknown", "Unknown", "Unknown"
                    reverse_country, reverse_country_code = "Unknown", "Unknown"
                    codec, sample_rate, bitrate, channels = "Unknown", "Unknown", "Unknown", "Unknown"

                # Console output (optional)
                sys.stdout.write(f"{url} | {availability} | {reverse_country} | {reverse_country_code} | "
                                 f"{latitude} | {longitude} | {codec} | {sample_rate} | {bitrate} | {channels}\n")

                # Write to CSV
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
                    'channels': channels
                })

                # Be kind to geolocation services
                sleep(1)


# Main program
if __name__ == '__main__':
    input_csv = 'radio_urls.csv'         # Input file
    output_csv = 'radio_results.csv'     # Output file
    process_csv(input_csv, output_csv)