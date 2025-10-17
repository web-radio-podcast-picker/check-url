import folium
from collections import defaultdict
import csv

# Sunset neon gradient colors
SUNSET_COLORS = [
    "#ffcc80",  # Low density - Peach
    "#ff8c00",  # Medium density - Bright Orange
    "#cc5500"   # High density - Ember
]

def get_sunset_color(n, max_n=10):
    """Return sunset gradient color based on station count"""
    norm = min(n / max_n, 1.0)
    if norm < 0.33:
        return SUNSET_COLORS[0]
    elif norm < 0.66:
        return SUNSET_COLORS[1]
    else:
        return SUNSET_COLORS[2]

def truncate_text(text, max_len=50):
    """Truncate text if too long and add ellipsis"""
    return text if len(text) <= max_len else text[:max_len] + "..."

def create_map_from_csv(output_file, map_file='output/map.html'):
    # Satellite base with attribution
    m = folium.Map(
        location=[20, 0],
        zoom_start=2,
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='Tiles © Esri — Source: Esri, Maxar, Earthstar Geographics, and the GIS User Community'
    )

    # Warm overlay
    overlay_html = """
    <div style="
        position: fixed;
        top: 0; left: 0; width: 100%; height: 100%;
        background: rgba(255,140,0,0.3);
        pointer-events: none;
        mix-blend-mode: overlay;
        z-index: 9998;">
    </div>
    """
    m.get_root().html.add_child(folium.Element(overlay_html))

    # Group URLs by coordinates
    coord_groups = defaultdict(list)
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

    # Add markers for each coordinate
    for (lat, lon), urls in coord_groups.items():
        count = len(urls)
        color = get_sunset_color(count, max_count)

        # Build popup with clickable, truncated URLs
        popup_html = "<b>Stations at this location:</b><br>"
        for url in urls:
            truncated = truncate_text(url, max_len=50)
            popup_html += f'<a href="{url}" target="_blank">{truncated}</a><br>'

        # Outer glow
        folium.CircleMarker(
            location=[lat, lon],
            radius=12,
            color=None,
            fill=True,
            fill_color=color,
            fill_opacity=0.25,
            weight=0
        ).add_to(m)

        # Inner dot
        folium.CircleMarker(
            location=[lat, lon],
            radius=5,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=1.0,
            popup=folium.Popup(popup_html, max_width=400)
        ).add_to(m)

    # Sunset-themed legend
    legend_html = f"""
    <div style="
        position: fixed;
        bottom: 50px;
        left: 50px;
        width: 200px;
        background: white;
        padding: 10px;
        border: 2px solid grey;
        z-index: 9999;
        font-size: 14px;">
        <b>Station Density (Sunset Glow)</b><br>
        <span style="background:{SUNSET_COLORS[0]};width:20px;height:10px;display:inline-block;"></span> 1 station<br>
        <span style="background:{SUNSET_COLORS[1]};width:20px;height:10px;display:inline-block;"></span> Few stations<br>
        <span style="background:{SUNSET_COLORS[2]};width:20px;height:10px;display:inline-block;"></span> Many stations
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    m.save(map_file)
    print(f"✅ Map with clickable sources saved → {map_file}")

if __name__ == '__main__':
    create_map_from_csv("output/radio_results.csv")
