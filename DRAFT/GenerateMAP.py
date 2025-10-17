import folium
from collections import defaultdict
import csv
import math
import colorsys

# ----------------------------
# Utility functions
# ----------------------------

def truncate_text(text, max_len=50):
    """Truncate text if too long and add ellipsis"""
    return text if len(text) <= max_len else text[:max_len] + "..."

def interpolate_color(value, max_value):
    """Return hex color smoothly based on density using logarithmic scaling"""
    if value < 1:
        value = 1
    ratio = math.log(value) / math.log(max_value) if max_value > 1 else 0

    hue = 0.10 - ratio * 0.08          # orange → deep red
    lightness = 0.65 - ratio * 0.3
    saturation = 0.9

    r, g, b = colorsys.hls_to_rgb(hue, lightness, saturation)
    return '#{:02x}{:02x}{:02x}'.format(int(r*255), int(g*255), int(b*255))

def add_pulse_marker(map_obj, lat, lon, color, popup_html, strength, availability):
    """Add a pulse-shimmer marker with size/speed based on density and availability metadata"""
    scale = 1.5 + strength * 2
    duration = 2.5 - strength * 1.5
    pulse_html = f"""
    <div class="pulse-marker" data-availability="{availability}" style="position: relative; width: 12px; height: 12px;">
        <div style="
            width: 12px; height: 12px;
            background: {color};
            border-radius: 50%;
            position: absolute;
            top: 0; left: 0;
            z-index: 2;">
        </div>
        <div style="
            width: 12px; height: 12px;
            background: {color};
            border-radius: 50%;
            position: absolute;
            top: 0; left: 0;
            animation: pulse {duration}s infinite;
            transform-origin: center;
            opacity: 0.6;">
        </div>
    </div>
    """
    folium.Marker(
        location=[lat, lon],
        icon=folium.DivIcon(html=pulse_html.replace("scale(2.5)", f"scale({scale})")),
        popup=folium.Popup(popup_html, max_width=400)
    ).add_to(map_obj)

# ----------------------------
# Main map creation
# ----------------------------

def create_map_from_csv(output_file, map_file='output/map.html'):
    # Base map
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
        background: rgba(255,140,0,0.2);
        pointer-events: none;
        mix-blend-mode: overlay;
        z-index: 9998;">
    </div>
    """
    m.get_root().html.add_child(folium.Element(overlay_html))

    # Embedded audio player
    audio_player_html = """
    <div style="position: fixed; bottom: 10px; left: 50%; transform: translateX(-50%);
                z-index:9999; background:white; padding:5px; border:1px solid gray; border-radius:5px;">
        <audio id="stationPlayer" controls style="width:300px;">
            Your browser does not support the audio element.
        </audio>
    </div>
    """
    m.get_root().html.add_child(folium.Element(audio_player_html))

    # ----------------------------
    # Load CSV and group by coordinates
    # ----------------------------
    coord_groups = defaultdict(list)
    with open(output_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile, delimiter='🙈')
        for row in reader:
            lat = row['latitude']
            lon = row['longitude']
            url = row['url']
            availability = row.get('availability', '0')  # default to 0
            if lat not in ['Unknown', '', None] and lon not in ['Unknown', '', None]:
                try:
                    key = (float(lat), float(lon))
                    coord_groups[key].append((url, availability))
                except ValueError:
                    continue

    max_count = max(len(urls) for urls in coord_groups.values())

    # ----------------------------
    # Add pulse markers
    # ----------------------------
    for (lat, lon), url_list in coord_groups.items():
        count = len(url_list)
        color = interpolate_color(count, max_count)
        strength = math.log(count) / math.log(max_count) if max_count > 1 else 0

        # Scrollable popup with embedded player links, including station count
        station_count = len(url_list)
        popup_html = f'<div style="max-height:200px; overflow-y:auto;"><b>Stations at this location: {station_count}</b><br>'
        for url, availability in url_list:
            truncated = truncate_text(url, max_len=50)
            color_style = "red" if availability != '1' else "blue"
            popup_html += (
                f'<a href="#" style="color:{color_style}; text-decoration:underline; cursor:pointer;" '
                f'onclick="document.getElementById(\'stationPlayer\').src=\'{url}\'; '
                f'document.getElementById(\'stationPlayer\').play(); return false;">'
                f'{truncated}</a><br>'
            )
        popup_html += '</div>'

        add_pulse_marker(m, lat, lon, color, popup_html, strength, url_list[0][1])

    # ----------------------------
    # Pulse CSS
    # ----------------------------
    pulse_css = """
    <style>
    @keyframes pulse {
      0% { transform: scale(1); opacity: 0.6; }
      50% { transform: scale(2.5); opacity: 0.1; }
      100% { transform: scale(1); opacity: 0.6; }
    }
    </style>
    """
    m.get_root().html.add_child(folium.Element(pulse_css))

    # ----------------------------
    # Radio static audio
    # ----------------------------
    audio_html = """
    <audio id="staticSound">
        <source src="data:audio/wav;base64,UklGRiQAAABXQVZFZm10IBIAAAABAAEARKwAAIhYAQACABAAZGF0YQAA" type="audio/wav">
    </audio>
    <script>
    function playStatic() {
        var audio = document.getElementById('staticSound');
        if (audio) {
            audio.volume = 0.3;  
            audio.currentTime = 0; 
            audio.play();
        }
    }
    document.addEventListener('click', function(e) {
        if (e.target.closest('.leaflet-popup') || e.target.closest('.leaflet-marker-icon')) {
            playStatic();
        }
    });
    </script>
    """
    m.get_root().html.add_child(folium.Element(audio_html))

    # ----------------------------
    # Availability selector (All / Yes)
    # ----------------------------
    filter_html = """
    <div style="position: fixed; top: 10px; left: 50px; z-index:9999; background:white; padding:5px; border:1px solid gray; border-radius:5px;">
        <label for="availabilityFilter"><b>Availability:</b></label>
        <select id="availabilityFilter">
            <option value="all">All</option>
            <option value="1">Yes</option>
        </select>
    </div>
    <script>
    document.getElementById('availabilityFilter').addEventListener('change', function() {
        var value = this.value;
        document.querySelectorAll('.pulse-marker').forEach(function(marker) {
            if(value === 'all' || marker.getAttribute('data-availability') === '1'){
                marker.style.display = 'block';
            } else {
                marker.style.display = 'none';
            }
        });
    });
    </script>
    """
    m.get_root().html.add_child(folium.Element(filter_html))

    # ----------------------------
    # Gradient legend for density
    # ----------------------------
    legend_html = """
    <div style="
        position: fixed;
        bottom: 50px;
        left: 50px;
        width: 180px;
        background: white;
        padding: 10px;
        border: 2px solid grey;
        border-radius: 5px;
        z-index: 9999;
        font-size: 14px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.3);">
        <b>Station Density</b><br>
        <div style="display: flex; align-items: center; margin-top:5px;">
            <div style="width:20px; height:10px; background:#ffcc80; margin-right:5px;"></div> Low density
        </div>
        <div style="display: flex; align-items: center; margin-top:5px;">
            <div style="width:20px; height:10px; background:#ff8c00; margin-right:5px;"></div> Medium density
        </div>
        <div style="display: flex; align-items: center; margin-top:5px;">
            <div style="width:20px; height:10px; background:#cc5500; margin-right:5px;"></div> High density
        </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # ----------------------------
    # Save map
    # ----------------------------
    m.save(map_file)
    print(f"✅ Fully interactive map saved → {map_file}")

# ----------------------------
# Run script
# ----------------------------
if __name__ == '__main__':
    create_map_from_csv("output/radio_results.csv")
