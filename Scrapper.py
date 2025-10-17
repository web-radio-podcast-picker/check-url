import requests

# URL to fetch
url = "https://radioregistry.com/station-directory/?keyword=&country=&genre=&perpage=1000&sort=asc&paginate="
pages = range(1, 52)

output_file = "output/all_pages.txt"  # Single file to store everything

# Open the file once in write or append mode
with open(output_file, "w", encoding="utf-8") as f:
    for i in pages:
        full_url = url + str(i)
        print(f"Fetching: {full_url}")

        try:
            response = requests.get(full_url, timeout=10)
            response.raise_for_status()  # Raise an exception for bad status codes

            # Write a header to separate pages
            f.write(f"\n\n=== PAGE {i}: {full_url} ===\n")
            f.write(response.text)

        except requests.RequestException as e:
            print(f"Failed to fetch {full_url}: {e}")
            f.write(f"\n\n=== PAGE {i}: {full_url} - FAILED ({e}) ===\n")