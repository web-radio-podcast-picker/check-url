import requests
import socket
import csv
import sys
from urllib.parse import urlparse
from geopy.geocoders import Nominatim
from time import sleep


# Function to check if the radio URL is available
def check_radio_url(url):
    try:
        response = requests.get(url, timeout=10, stream=True)
        if response.status_code == 200:
            return True
        else:
            return False
    except requests.exceptions.RequestException as e:
        return False


# Function to get the IP address of the server
def get_ip_from_url(url):
    domain = urlparse(url).hostname
    try:
        ip_address = socket.gethostbyname(domain)
        return ip_address
    except socket.gaierror:
        return None


# Function to get the full information (country, coordinates) of the server
def get_server_info(ip_address):
    try:
        response = requests.get(f"http://ipinfo.io/{ip_address}/json")
        data = response.json()

        country_code = data.get("country", "Unknown")  # Country code (e.g., US, DE)
        country_name = data.get("country_name", "Unknown")  # Full country name
        loc = data.get("loc", "").split(",")  # Latitude and Longitude
        latitude = loc[0] if len(loc) > 0 else "Unknown"
        longitude = loc[1] if len(loc) > 1 else "Unknown"

        return country_name, country_code, latitude, longitude
    except requests.exceptions.RequestException as e:
        return "Unknown", "Unknown", "Unknown", "Unknown"


# Function to reverse-geocode the latitude and longitude to a country
def reverse_geocode(latitude, longitude):
    # Use a custom User-Agent to avoid 403 errors
    geolocator = Nominatim(user_agent="GeoCheckerApp")  # Provide a custom User-Agent
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


# Function to process the CSV input and output
def process_csv(input_file, output_file):
    # Open the input CSV and read the URLs
    with open(input_file, 'r') as infile:
        reader = csv.DictReader(infile)

        # Prepare to write the results to the output CSV
        with open(output_file, 'w', newline='') as outfile:
            fieldnames = ['url', 'availability', 'country', 'country_code', 'latitude', 'longitude', 'reverse_country',
                          'reverse_country_code']
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                url = row['url']
                availability = "available" if check_radio_url(url) else "unavailable"

                if availability == "available":
                    ip_address = get_ip_from_url(url)
                    if ip_address:
                        country, country_code, latitude, longitude = get_server_info(ip_address)

                        # If coordinates are available, reverse geocode them
                        if latitude != "Unknown" and longitude != "Unknown":
                            reverse_country, reverse_country_code = reverse_geocode(latitude, longitude)
                            # Only update country and code if the reverse geocode gives us a better result
                            country = reverse_country if country == "Unknown" else country
                            country_code = reverse_country_code if country_code == "Unknown" else country_code
                    else:
                        country, country_code, latitude, longitude = "Unknown", "Unknown", "Unknown", "Unknown"
                        reverse_country, reverse_country_code = "Unknown", "Unknown"
                else:
                    country, country_code, latitude, longitude = "Unknown", "Unknown", "Unknown", "Unknown"
                    reverse_country, reverse_country_code = "Unknown", "Unknown"

                # Output
                sys.stdout.write(url+' | '+availability+' | '+reverse_country+' | '+reverse_country_code+' | '+latitude+' | '+longitude+'\n')

                # Write each result row to the output CSV
                writer.writerow({
                    'url': url,
                    'availability': availability,
                    'country': country,
                    'country_code': country_code,
                    'latitude': latitude,
                    'longitude': longitude,
                    'reverse_country': reverse_country,
                    'reverse_country_code': reverse_country_code
                })

                # Sleep to avoid overloading the geocoding service
                sleep(1)


# main program de checkURL par ARP188
input_csv = 'radio_urls.csv'  # Your input CSV file containing URLs
output_csv = 'radio_results.csv'  # The output CSV file where results will be saved
process_csv(input_csv, output_csv)