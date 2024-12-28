import csv
from datetime import datetime
import openmeteo_requests
import requests
import requests_cache
import pandas as pd
from retry_requests import retry

# Setup Indian Railways API key and base URL
API_KEY = '4ca75cf9c1cb385e61c3a856ae817766'
BASE_URL = f"http://indianrailapi.com/api/v2/StationCodeToName/apikey/{API_KEY}/StationCode/"

# File path for station data
stations_file_path = "stations.xls"

def fetch_station_details_from_api(station_code):
    """Fetch station details from the Indian Railway API."""
    url = BASE_URL + station_code
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if data['ResponseCode'] == "200" and data['Status'] == "SUCCESS":
            latitude = data['Station']['Latitude']
            longitude = data['Station']['Longitude']
            return {
                "label": station_code,
                "latitude": float(latitude) if latitude else None,
                "longitude": float(longitude) if longitude else None
            }
        else:
            return None
    except Exception as e:
        print(f"Error fetching details for station '{station_code}' from API: {e}")
        return None

def fetch_station_details(station_code, station_data):
    """Fetch station details with priority: Excel file > API > Manual Input."""
    if station_code in station_data:
        # Fetch from Excel file
        print(f"Found station '{station_code}' in the Excel file.")
        return {
            "label": station_code,
            "latitude": station_data[station_code]["Longitude"],
            "longitude": station_data[station_code]["Latitude"]
        }
    else:
        # Fetch from API
        print(f"Station '{station_code}' not found in Excel file. Querying API...")
        station_details = fetch_station_details_from_api(station_code)
        if station_details:
            print(f"Found station '{station_code}' in API.")
            return station_details
        else:
            # Prompt for manual input
            print(f"Station '{station_code}' not found in API. Please enter details manually.")
            try:
                latitude = float(input(f"Enter latitude for station '{station_code}': "))
                longitude = float(input(f"Enter longitude for station '{station_code}': "))
                return {"label": station_code, "latitude": latitude, "longitude": longitude}
            except ValueError:
                print(f"Invalid input for station '{station_code}'. Skipping it.")
                return None

# Read station data from Excel file
stations_df = pd.read_excel(stations_file_path)
stations_df = stations_df.drop_duplicates(subset="StationCode", keep="first")
station_data = stations_df.set_index("StationCode")[["Latitude", "Longitude"]].to_dict("index")

# List of station codes
station_codes = [ 'YPR', 'BAW', 'SDVL', 'GHL', 'BNKH', 'DBL', 'MDLL','NDV', 'DBS','HHL','KIAT','TK']

stations = []

for station_code in station_codes:
    station_details = fetch_station_details(station_code, station_data)
    if station_details:
        stations.append(station_details)

# Save updated station details
stations_df_output = pd.DataFrame(stations)
stations_df_output.to_excel("updated_stations.xlsx", index=False)
print("Updated station details saved to 'updated_stations.xlsx'.")

# Set up Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Prepare weather data
data = [
    [datetime(2023,12,23),2,7,6,8,6,5,5,5,5,7,7,-1]
,[datetime(2023,12,24),2,8,7,10,10,12,9,12,12,15,13,1]
,[datetime(2023,12,25),4,5,2,5,5,3,0,3,3,2,0,-13]
,[datetime(2023,12,26),4,5,2,7,0,8,7,7,7,9,8,-5]
,[datetime(2023,12,27),1,0,0,2,0,3,0,6,4,12,10,-2]
,[datetime(2023,12,28),8,9,6,11,11,12,10,10,0,7,5,-7]
,[datetime(2023,12,29),3,7,6,9,9,8,8,11,11,11,9,-2]
,[datetime(2023,12,30),53,53,51,54,54,52,53,51,53,50,48,37]
,[datetime(2023,12,31),0,0,0,2,0,0,0,0,0,0,0,-10]
,[datetime(2024,1,1),1,0,0,2,0,0,0,2,0,3,0,-13]
,[datetime(2024,1,2),22,23,20,25,25,26,24,24,24,25,22,10]
,[datetime(2024,1,3),1,2,0,3,3,5,0,5,5,4,3,-6]
,[datetime(2024,1,4),1,4,3,4,3,3,0,4,4,2,0,-9]
,[datetime(2024,1,5),1,1,0,5,5,5,4,6,4,5,4,-10]
,[datetime(2024,1,6),15,14,13,14,13,11,12,10,12,7,5,6]
,[datetime(2024,1,7),1,1,0,2,0,2,0,1,0,1,0,-10]
,[datetime(2024,1,8),0,1,0,5,5,5,4,5,4,4,3,-10]
,[datetime(2024,1,9),0,1,0,1,0,0,0,4,4,2,3,-7]
,[datetime(2024,1,10),34,33,32,32,32,32,31,31,31,31,20,10]
,[datetime(2024,1,11),1,0,0,3,3,4,3,3,3,2,0,-14]
,[datetime(2024,1,12),4,6,0,7,6,6,5,8,8,8,6,-7]
,[datetime(2024,1,13),20,20,18,20,18,16,15,13,0,10,7,0]
,[datetime(2024,1,14),0,4,3,7,7,7,6,7,6,8,7,-6]
,[datetime(2024,1,15),1,2,0,3,3,5,4,6,0,4,0,-8]
,[datetime(2024,1,16),1,4,3,8,0,7,0,8,8,9,12,-1]
,[datetime(2024,1,17),2,0,0,2,0,5,4,6,4,11,9,0]
,[datetime(2024,1,18),1,1,0,2,0,1,0,2,0,3,0,-10]
,[datetime(2024,1,19),1,3,0,5,5,6,4,5,4,6,4,-7]
,[datetime(2024,1,20),9,9,7,11,11,9,10,7,7,5,5,0]
,[datetime(2024,1,21),1,2,0,3,3,4,3,5,3,6,4,-8]
,[datetime(2024,1,22),1,4,3,4,3,2,0,0,0,0,0,-11]
,[datetime(2024,1,23),0,2,0,2,0,1,0,0,0,0,0,-14]
,[datetime(2024,1,24),1,3,0,6,6,7,5,4,5,3,3,-3]
,[datetime(2024,1,25),5,8,7,9,7,8,6,9,9,9,7,-5]
,[datetime(2024,1,26),7,6,5,10,10,12,9,12,12,11,10,0]
,[datetime(2024,1,27),4,6,5,7,5,7,4,6,4,7,5,-8]
,[datetime(2024,1,28),14,14,12,14,12,10,11,11,11,11,9,-5]
,[datetime(2024,1,29),1,1,0,3,3,2,0,4,4,2,0,0]
,[datetime(2024,1,30),9,11,10,11,10,10,9,7,9,6,4,0]
,[datetime(2024,1,31),1,4,3,5,3,7,6,6,0,6,4,-5]
,[datetime(2024,2,1),3,5,0,9,9,5,4,3,4,2,2,-11]
,[datetime(2024,2,2),1,4,3,6,0,6,5,7,5,6,3,-5]
,[datetime(2024,2,3),9,8,7,7,7,7,6,12,12,9,7,-4]
,[datetime(2024,2,4),1,1,0,1,0,2,0,1,0,1,0,-13]
,[datetime(2024,2,5),1,3,0,3,3,0,0,0,0,0,0,-11]
,[datetime(2024,2,6),6,7,0,10,10,8,9,7,9,6,4,-1]
,[datetime(2024,2,7),1,3,0,3,3,1,0,1,0,0,0,-16]
,[datetime(2024,2,8),None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,2,9),0,3,0,7,7,7,6,7,6,8,7,6]
,[datetime(2024,2,10),8,8,0,8,8,6,7,6,7,5,4,-4]
,[datetime(2024,2,11),1,3,0,0,0,0,0,0,0,0,9,0]
,[datetime(2024,2,12),1,5,4,8,8,5,4,7,7,5,2,-11]
,[datetime(2024,2,13),1,2,0,2,0,0,0,3,3,4,0,-12]
,[datetime(2024,2,14),1,1,0,2,0,7,6,9,9,9,7,-1]
,[datetime(2024,2,15),1,2,0,2,0,1,0,0,0,0,0,-14]
,[datetime(2024,2,16),23,21,21,21,21,17,16,15,16,12,10,7]
,[datetime(2024,2,17),4,5,4,7,7,7,6,6,6,6,7,-3]
,[datetime(2024,2,18),0,3,0,5,5,3,4,4,4,5,3,-2]
,[datetime(2024,2,19),1,1,0,2,0,0,0,1,0,0,0,-3]
,[datetime(2024,2,20),23,22,21,22,21,17,17,13,13,10,8,4]
,[datetime(2024,2,21),5,10,9,12,12,12,11,13,11,11,9,-1]
,[datetime(2024,2,22),2,2,0,2,0,1,0,0,0,0,0,-13]
,[datetime(2024,2,23),4,5,2,5,5,2,1,0,1,0,0,-11]
,[datetime(2024,2,24),13,13,11,15,15,13,14,11,0,8,6,0]
,[datetime(2024,2,25),3,18,17,18,17,15,16,14,16,11,9,7]
,[datetime(2024,2,26),0,3,0,3,3,0,0,1,0,0,0,-14]
,[datetime(2024,2,27),3,10,8,11,11,7,6,4,6,6,4,-10]
,[datetime(2024,2,28),7,6,5,7,5,5,0,5,0,5,3,0]
,[datetime(2024,2,29),1,0,0,2,0,0,0,0,0,0,0,-16]
,[datetime(2024,3,1),10,11,8,12,12,11,11,10,11,11,9,-3]
,[datetime(2024,3,2),19,20,17,20,20,19,19,15,16,13,11,2]
,[datetime(2024,3,3),0,3,0,5,5,3,4,2,4,2,0,-5]
,[datetime(2024,3,4),5,19,18,19,18,18,17,19,17,19,15,10]
,[datetime(2024,3,5),1,6,5,8,8,10,7,10,10,10,8,-3]
,[datetime(2024,3,6),0,4,3,5,3,2,0,1,0,0,0,-14]
,[datetime(2024,3,7),1,1,0,3,3,2,0,1,0,0,0,-13]
,[datetime(2024,3,8),6,7,4,9,9,9,8,9,8,10,7,-4]
,[datetime(2024,3,9),10,11,8,11,0,11,0,11,0,10,8,-3]
,[datetime(2024,3,10),1,1,0,1,0,0,0,0,0,0,0,-11]
,[datetime(2024,3,11),4,5,2,7,7,6,6,6,0,5,3,-5]
,[datetime(2024,3,12),1,1,0,5,5,7,4,6,4,7,5,-7]
,[datetime(2024,3,13),2,2,0,3,3,3,0,3,3,4,4,22]
,[datetime(2024,3,14),0,0,0,1,0,0,0,0,0,0,0,-12]
,[datetime(2024,3,15),1,0,0,2,0,2,0,3,3,8,7,-6]
,[datetime(2024,3,16),3,4,3,7,0,4,3,5,3,4,3,-2]
,[datetime(2024,3,17),1,2,0,7,7,6,6,5,6,4,1,0]
,[datetime(2024,3,18),1,1,0,3,3,4,3,4,3,5,3,-6]
,[datetime(2024,3,19),3,4,3,7,7,6,0,8,8,7,6,0]
,[datetime(2024,3,20),1,0,0,2,0,0,0,1,0,4,0,-7]
,[datetime(2024,3,21),1,0,0,5,5,3,4,2,4,1,0,-14]
,[datetime(2024,3,22),6,14,13,14,13,15,12,17,17,15,15,2]
,[datetime(2024,3,23),1,1,0,4,4,1,0,0,0,4,0,-10]
,[datetime(2024,3,24),1,5,4,5,4,3,3,3,3,6,6,-6]
,[datetime(2024,3,25),4,5,4,7,7,5,0,4,4,3,0,-10]
,[datetime(2024,3,26),1,2,0,2,0,4,3,4,3,6,4,-9]
,[datetime(2024,3,27),6,6,4,9,9,10,8,12,12,11,8,-4]
,[datetime(2024,3,28),2,5,4,8,8,5,0,5,5,6,2,-5]
,[datetime(2024,3,29),5,6,3,6,6,3,2,1,2,1,0,-10]
,[datetime(2024,3,30),14,14,12,14,12,12,11,10,11,8,6,-1]
,[datetime(2024,3,31),1,0,0,7,7,9,6,10,10,10,8,0]
,[datetime(2024,4,1),None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,4,2),None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,4,3),None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,4,4),None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,4,5),1,3,3,4,3,0,0,0,0,0,14,-5]
,[datetime(2024,4,6),1,4,3,8,8,9,7,10,10,8,5,-4]
,[datetime(2024,4,7),0,2,0,3,3,3,0,3,3,3,0,-11]
,[datetime(2024,4,8),37,40,39,42,42,46,44,63,63,81,79,66]
,[datetime(2024,4,9),2,4,3,7,7,4,3,1,3,2,1,-12]
,[datetime(2024,4,10),6,6,4,5,4,4,3,0,0,0,0,-15]
,[datetime(2024,4,11),1,2,0,5,5,7,0,7,7,9,5,-2]
,[datetime(2024,4,12),3,6,4,8,8,8,7,7,7,6,5,-4]
,[datetime(2024,4,13),9,9,7,13,0,10,9,10,9,11,10,0]
,[datetime(2024,4,14),4,11,10,12,10,9,9,8,9,9,7,-5]
,[datetime(2024,4,15),8,10,9,12,12,9,8,8,8,6,6,-4]
,[datetime(2024,4,16),1,2,0,5,5,3,4,3,4,2,0,-12]
,[datetime(2024,4,17),8,10,9,12,12,14,11,15,15,18,16,2]
,[datetime(2024,4,18),3,6,5,6,5,4,4,1,1,0,0,-16]
,[datetime(2024,4,19),1,3,0,4,4,2,3,0,0,2,0,-11]
,[datetime(2024,4,20),11,11,9,12,13,11,12,9,9,7,7,0]
,[datetime(2024,4,21),1,3,0,6,6,5,5,6,5,5,3,-4]
,[datetime(2024,4,22),1,0,0,2,0,0,0,1,0,8,6,-2]
,[datetime(2024,4,23),2,8,7,11,11,10,10,11,10,12,11,0]
,[datetime(2024,4,24),1,1,0,5,5,4,4,3,4,1,0,-5]
,[datetime(2024,4,25),3,7,6,9,9,9,8,10,8,8,6,-6]
,[datetime(2024,4,26),2,3,0,5,5,3,4,2,4,0,0,-13]
,[datetime(2024,4,27),2,4,3,4,3,5,4,5,4,5,3,-8]
,[datetime(2024,4,28),1,3,0,4,0,3,0,4,4,1,0,-10]
,[datetime(2024,4,29),6,6,4,6,7,2,1,0,1,0,0,-15]
,[datetime(2024,4,30),0,3,0,0,0,0,0,0,0,1,0,0]
,[datetime(2024,5,1),6,4,4,6,4,6,3,6,6,6,4,-7]
,[datetime(2024,5,2),6,8,7,9,7,9,6,10,0,12,12,5]
,[datetime(2024,5,3),0,5,3,5,0,3,0,2,0,4,6,-7]
,[datetime(2024,5,4),4,4,2,7,7,4,3,3,3,2,1,-13]
,[datetime(2024,5,5),1,1,0,5,0,6,5,5,5,6,6,-3]
,[datetime(2024,5,6),20,19,18,18,18,18,17,17,17,17,15,14]
,[datetime(2024,5,7),10,10,8,13,13,12,12,11,12,12,10,5]
,[datetime(2024,5,8),1,1,0,2,0,0,0,2,0,1,0,-11]
,[datetime(2024,5,9),1,2,0,4,4,2,3,0,0,0,0,-1]
,[datetime(2024,5,10),1,3,0,3,3,4,3,6,6,10,8,-4]
,[datetime(2024,5,11),1,2,0,2,0,2,0,0,0,0,0,-11]
,[datetime(2024,5,12),41,42,39,44,44,69,68,72,72,74,70,57]
,[datetime(2024,5,13),3,5,4,5,4,5,3,7,7,7,5,-6]
,[datetime(2024,5,14),5,10,9,15,15,13,14,16,14,21,19,7]
,[datetime(2024,5,15),10,11,0,11,11,10,0,10,10,8,8,0]
,[datetime(2024,5,16),8,8,6,9,0,9,8,9,8,10,6,-4]
,[datetime(2024,5,17),1,4,3,4,3,2,0,1,0,0,0,-13]
,[datetime(2024,5,18),1,3,0,5,5,13,12,11,12,8,6,0]
,[datetime(2024,5,19),0,1,0,3,3,4,3,4,3,2,0,-8]
,[datetime(2024,5,20),2,3,0,4,4,2,3,3,3,2,0,-11]
,[datetime(2024,5,21),0,1,0,1,0,0,0,0,0,1,0,-12]
,[datetime(2024,5,22),1,2,0,3,3,5,4,5,4,6,4,-8]
,[datetime(2024,5,23),6,20,19,21,19,21,18,23,23,22,21,7]
,[datetime(2024,5,24),4,8,7,11,11,12,11,11,11,8,6,7]
,[datetime(2024,5,25),12,13,10,14,14,11,10,9,10,7,5,1]
,[datetime(2024,5,26),11,15,14,17,17,17,16,17,16,17,14,2]
,[datetime(2024,5,27),1,0,0,5,5,1,0,0,0,0,0,-10]
,[datetime(2024,5,28),0,2,3,5,3,6,5,5,5,5,3,-6]
,[datetime(2024,5,29),6,14,13,15,13,17,16,14,16,12,10,-2]
,[datetime(2024,5,30),24,23,22,23,22,23,21,23,21,26,24,9]
,[datetime(2024,5,31),1,2,0,4,4,6,3,5,3,5,3,-10]
,[datetime(2024,6,1),3,10,0,12,12,13,11,11,11,9,9,-5]
,[datetime(2024,6,2),2,3,0,9,0,7,6,6,6,5,4,-4]
,[datetime(2024,6,3),1,6,4,11,11,16,15,17,15,17,13,4]
,[datetime(2024,6,4),8,10,9,13,13,11,12,9,9,7,7,-4]
,[datetime(2024,6,5),1,1,0,3,3,3,0,6,0,20,18,7]
,[datetime(2024,6,6),3,13,12,16,15,14,14,28,28,25,23,14]
,[datetime(2024,6,7),5,4,3,16,16,18,0,17,17,17,15,5]
,[datetime(2024,6,8),7,6,0,11,11,9,10,10,10,10,8,-5]
,[datetime(2024,6,9),1,11,9,15,15,15,14,15,14,12,12,3]
,[datetime(2024,6,10),3,4,3,4,0,3,0,2,0,0,0,-12]
,[datetime(2024,6,11),1,9,8,14,13,18,15,18,18,18,16,9]
,[datetime(2024,6,12),1,2,0,6,6,7,5,8,8,11,9,-2]
,[datetime(2024,6,13),0,1,0,3,3,4,3,6,6,6,4,-6]
,[datetime(2024,6,14),15,14,13,15,13,11,12,9,9,6,4,0]
,[datetime(2024,6,15),2,3,0,4,4,4,3,7,7,9,10,-3]
,[datetime(2024,6,16),26,24,24,24,24,21,20,21,20,21,18,6]
,[datetime(2024,6,17),1,3,0,3,3,1,0,0,0,0,0,-14]
,[datetime(2024,6,18),1,6,5,8,8,6,7,5,7,9,11,-1]
,[datetime(2024,6,19),1,2,0,3,3,7,6,7,6,10,8,0]
,[datetime(2024,6,20),10,11,8,14,14,12,13,12,13,13,11,10]
,[datetime(2024,6,21),0,1,0,4,4,6,3,6,6,6,4,-4]
,[datetime(2024,6,22),5,5,3,6,6,4,5,6,5,8,6,-6]
,[datetime(2024,6,23),1,1,0,2,0,0,0,0,0,0,0,-13]
,[datetime(2024,6,24),8,10,9,13,13,16,15,15,15,15,13,10]
,[datetime(2024,6,25),4,5,0,8,8,10,7,12,12,14,14,1]
,[datetime(2024,6,26),1,1,0,4,4,6,3,6,6,6,4,-2]
,[datetime(2024,6,27),1,1,0,3,3,8,7,4,4,4,2,0]
,[datetime(2024,6,28),0,6,5,6,5,5,4,6,0,6,4,-4]
,[datetime(2024,6,29),1,2,0,4,4,6,3,8,8,11,13,1]
,[datetime(2024,6,30),1,8,7,11,11,14,13,14,13,15,15,2]
,[datetime(2024,7,1),1,1,0,2,0,1,0,1,0,1,0,-7]
,[datetime(2024,7,2),10,15,14,16,14,12,0,11,0,10,11,5]
,[datetime(2024,7,3),1,0,0,2,0,2,0,1,0,1,0,-10]
,[datetime(2024,7,4),4,7,6,11,11,10,10,10,0,9,7,3]
,[datetime(2024,7,5),7,7,5,10,10,11,9,11,9,7,11,1]
,[datetime(2024,7,6),6,7,4,8,8,6,7,2,2,1,0,-14]
,[datetime(2024,7,7),1,8,7,13,13,11,12,9,9,5,4,4]
,[datetime(2024,7,8),15,17,16,22,22,20,21,18,18,19,16,14]
,[datetime(2024,7,9),0,11,10,13,13,16,15,11,12,7,6,-6]
,[datetime(2024,7,10),3,4,0,8,8,9,7,19,0,17,15,2]
,[datetime(2024,7,11),6,21,20,22,20,22,19,20,19,16,14,12]
,[datetime(2024,7,12),2,5,4,9,9,10,8,11,11,9,9,1]
,[datetime(2024,7,13),3,3,0,3,3,2,0,3,0,0,0,-10]
,[datetime(2024,7,14),0,5,4,9,9,14,13,15,0,17,18,7]
,[datetime(2024,7,15),2,7,6,10,10,8,9,11,0,11,9,-2]
,[datetime(2024,7,16),7,8,5,11,11,11,0,14,15,13,13,0]
,[datetime(2024,7,17),1,2,0,3,3,1,0,1,0,1,0,0]
,[datetime(2024,7,18),2,3,0,3,3,0,0,0,0,0,0,-12]
,[datetime(2024,7,19),0,2,1,5,5,6,0,7,7,6,5,-5]
,[datetime(2024,7,20),8,8,6,14,14,15,13,16,16,15,14,11]
,[datetime(2024,7,21),1,2,0,2,0,2,0,1,0,2,4,-7]
,[datetime(2024,7,22),1,1,0,1,0,0,0,0,0,1,0,-9]
,[datetime(2024,7,23),9,9,7,13,13,12,0,11,11,10,16,3]
,[datetime(2024,7,24),0,4,3,8,8,9,7,8,7,7,5,-1]
,[datetime(2024,7,25),1,3,0,4,4,5,0,5,5,5,3,-7]
,[datetime(2024,7,26),1,3,0,3,3,2,0,2,0,0,0,-11]
,[datetime(2024,7,27),1,1,0,9,9,11,8,11,11,11,9,-2]
,[datetime(2024,7,28),1,0,0,1,0,1,0,1,0,2,0,-7]
,[datetime(2024,7,29),19,23,20,25,25,23,0,22,22,19,17,8]
,[datetime(2024,7,30),1,1,0,2,0,3,0,3,3,2,0,-15]
,[datetime(2024,7,31),6,7,4,7,0,10,9,10,9,10,7,-3]
,[datetime(2024,8,1),6,17,16,17,0,16,15,15,15,14,17,10]
,[datetime(2024,8,2),4,6,5,8,8,6,0,5,0,7,9,-4]
,[datetime(2024,8,3),2,3,0,5,5,4,4,3,4,2,0,-12]
,[datetime(2024,8,4),5,8,7,9,7,10,9,10,9,11,10,-2]
,[datetime(2024,8,5),1,3,0,5,5,5,4,5,4,4,6,0]
,[datetime(2024,8,6),0,2,0,4,4,6,3,7,7,9,10,-2]
,[datetime(2024,8,7),4,4,3,7,7,9,6,10,10,9,11,0]
,[datetime(2024,8,8),1,1,0,3,3,2,0,3,3,5,7,-3]
,[datetime(2024,8,9),46,51,48,55,55,59,58,59,58,63,65,53]
,[datetime(2024,8,10),1,2,0,2,0,4,3,5,3,9,7,-1]
,[datetime(2024,8,11),1,2,0,3,3,5,4,5,4,6,13,0]
,[datetime(2024,8,12),1,1,0,2,0,4,3,4,3,5,3,-10]
,[datetime(2024,8,13),36,38,37,41,41,44,43,46,46,49,50,40]
,[datetime(2024,8,14),2,4,3,5,3,4,3,3,3,2,0,-11]
,[datetime(2024,8,15),1,3,0,3,3,0,0,0,0,0,0,-7]
,[datetime(2024,8,16),1,0,0,2,0,4,3,7,7,8,8,-4]
,[datetime(2024,8,17),3,3,0,5,5,8,7,7,7,10,8,0]
,[datetime(2024,8,18),0,1,0,1,0,0,0,0,0,0,0,-13]
,[datetime(2024,8,19),1,0,0,3,3,3,0,5,5,6,6,0]
,[datetime(2024,8,20),1,3,0,6,6,9,8,9,8,8,6,-3]
,[datetime(2024,8,21),19,20,17,21,21,22,20,22,20,23,21,12]
,[datetime(2024,8,22),3,13,12,15,15,15,14,16,14,15,12,3]
,[datetime(2024,8,23),28,27,26,28,26,24,25,20,20,18,18,15]
,[datetime(2024,8,24),19,22,20,24,24,22,23,18,18,15,13,15]
,[datetime(2024,8,25),5,12,11,13,0,11,10,7,0,5,8,8]
,[datetime(2024,8,26),6,8,7,8,7,7,6,4,6,4,4,-5]
,[datetime(2024,8,27),1,4,3,6,6,10,9,10,0,10,8,0]
,[datetime(2024,8,28),17,19,18,19,18,18,17,16,17,13,9,10]
,[datetime(2024,8,29),9,11,10,15,15,13,0,12,12,11,10,8]
,[datetime(2024,8,30),1,6,5,10,10,14,0,14,14,17,15,5]
,[datetime(2024,8,31),4,7,6,16,16,18,15,18,0,17,16,8]
,[datetime(2024,9,1),1,4,3,6,6,8,5,8,8,9,6,-4]
,[datetime(2024,9,2),1,4,3,6,6,12,11,13,11,15,13,5]
,[datetime(2024,9,3),1,1,0,3,3,2,0,3,0,2,0,-13]
,[datetime(2024,9,4),3,6,5,8,8,7,7,8,7,9,5,-1]
,[datetime(2024,9,5),2,4,3,6,6,9,8,11,11,13,13,3]
,[datetime(2024,9,6),1,3,0,6,6,5,5,5,5,5,3,-8]
,[datetime(2024,9,7),1,5,4,7,7,10,9,13,13,10,8,-3]
,[datetime(2024,9,8),0,2,0,6,6,7,5,9,9,11,11,0]
,[datetime(2024,9,9),1,6,4,8,8,7,7,6,7,5,5,-5]
,[datetime(2024,9,10),2,2,1,7,7,6,6,10,10,9,8,-5]
,[datetime(2024,9,11),29,34,33,34,33,29,29,24,25,20,18,15]
,[datetime(2024,9,12),2,5,4,10,10,11,9,11,9,8,7,0]
,[datetime(2024,9,13),18,18,16,18,16,14,15,12,12,9,7,13]
,[datetime(2024,9,14),31,30,29,33,33,35,32,36,36,51,52,42]
,[datetime(2024,9,15),1,3,0,5,5,4,4,3,4,2,0,-9]
,[datetime(2024,9,16),15,17,16,19,19,18,18,18,18,18,19,12]
,[datetime(2024,9,17),1,2,0,3,3,2,0,2,0,2,0,-7]
,[datetime(2024,9,18),18,18,16,21,21,22,20,23,23,20,18,8]
,[datetime(2024,9,19),3,5,3,7,7,5,6,7,6,6,4,-6]
,[datetime(2024,9,20),0,2,0,5,5,7,4,7,7,9,7,-4]
,[datetime(2024,9,21),1,1,0,3,3,7,6,8,6,9,7,5]
,[datetime(2024,9,22),1,2,0,5,5,3,4,3,4,2,0,-11]
,[datetime(2024,9,23),4,5,4,7,7,10,9,10,9,9,7,0]
,[datetime(2024,9,24),1,0,0,0,0,0,0,3,3,3,0,-5]
,[datetime(2024,9,25),1,0,0,3,3,3,0,2,0,5,3,-8]
,[datetime(2024,9,26),1,1,0,3,3,7,6,6,6,9,7,0]
,[datetime(2024,9,27),1,3,0,6,6,6,5,6,5,6,3,0]
,[datetime(2024,9,28),7,83,82,82,82,82,81,81,81,81,79,10]
,[datetime(2024,9,29),19,19,17,20,20,20,19,17,19,16,14,5]
,[datetime(2024,9,30),1,10,9,19,19,18,18,17,18,16,13,4]
,[datetime(2024,10,1),9,16,15,19,19,20,18,20,18,17,13,9]
,[datetime(2024,10,2),10,12,11,16,16,14,15,10,10,9,11,11]
,[datetime(2024,10,3),1,1,0,3,3,3,0,3,3,3,3,-4]
,[datetime(2024,10,4),1,0,0,5,6,3,2,2,2,1,4,-7]
,[datetime(2024,10,5),1,5,4,4,5,5,4,4,4,4,0,15]
,[datetime(2024,10,6),1,1,0,3,3,2,0,1,0,1,0,-9]
,[datetime(2024,10,7),1,0,0,0,0,0,0,5,5,3,3,-3]
,[datetime(2024,10,8),1,3,0,3,3,5,4,4,4,2,3,-5]
,[datetime(2024,10,9),4,7,5,11,11,11,10,15,15,12,10,0]
,[datetime(2024,10,10),8,7,6,11,11,14,13,14,13,12,11,5]
,[datetime(2024,10,11),1,7,6,10,10,8,9,7,9,5,3,-3]
,[datetime(2024,10,12),21,27,26,27,26,25,25,23,25,19,18,8]
,[datetime(2024,10,13),0,3,0,6,6,8,5,8,8,6,3,-8]
,[datetime(2024,10,14),4,6,5,6,5,3,4,2,4,1,0,-13]
,[datetime(2024,10,15),1,7,6,10,10,15,14,16,14,14,12,10]
,[datetime(2024,10,16),1,2,0,5,5,8,7,12,12,18,16,7]
,[datetime(2024,10,17),5,7,6,12,12,12,11,14,14,13,12,0]
,[datetime(2024,10,18),1,2,0,6,6,6,5,7,5,6,3,-3]
,[datetime(2024,10,19),4,5,4,5,4,7,6,6,6,8,4,-3]
,[datetime(2024,10,20),1,3,0,3,3,3,0,2,0,0,0,-10]
,[datetime(2024,10,21),4,5,4,7,7,6,6,4,6,7,4,-4]
,[datetime(2024,10,22),1,1,0,2,0,0,0,0,0,1,0,-10]
,[datetime(2024,10,23),1,3,0,5,5,5,4,5,4,6,4,-5]
,[datetime(2024,10,24),2,4,3,5,3,7,6,4,6,8,7,-4]
,[datetime(2024,10,25),2,3,0,4,4,6,3,6,6,7,7,-3]
,[datetime(2024,10,26),2,2,0,2,0,4,3,5,3,7,5,-3]
,[datetime(2024,10,27),1,2,0,2,0,0,0,2,0,4,0,-11]
,[datetime(2024,10,28),1,3,0,10,10,12,9,11,9,10,7,-3]
,[datetime(2024,10,29),10,11,8,13,13,13,0,12,12,10,10,-2]
,[datetime(2024,10,30),0,3,0,4,4,6,3,7,7,10,8,-2]
,[datetime(2024,10,31),0,3,0,4,4,6,3,6,6,7,7,-6]
,[datetime(2024,11,1),0,2,0,5,5,4,4,4,4,4,0,-11]
,[datetime(2024,11,2),1,3,0,4,3,6,5,5,5,6,3,-6]
,[datetime(2024,11,3),1,2,0,2,0,1,0,1,0,1,0,-10]
,[datetime(2024,11,4),19,23,22,24,25,19,19,18,19,16,18,15]
,[datetime(2024,11,5),1,2,0,2,0,1,0,1,0,0,0,-12]
,[datetime(2024,11,6),1,0,0,1,0,2,0,4,4,4,3,-10]
,[datetime(2024,11,7),3,3,0,6,5,8,7,9,7,11,9,-2]
,[datetime(2024,11,8),17,16,15,16,15,18,17,19,17,20,18,5]
,[datetime(2024,11,9),2,2,0,10,10,10,9,10,9,10,11,0]
,[datetime(2024,11,10),2,7,4,8,0,8,7,8,7,6,5,-5]
,[datetime(2024,11,11),3,3,0,6,6,8,5,6,6,9,11,5]
,[datetime(2024,11,12),4,9,8,11,11,11,10,11,10,9,8,0]
,[datetime(2024,11,13),1,0,0,0,0,2,0,2,0,2,0,-13]
,[datetime(2024,11,14),8,9,6,11,11,11,10,11,10,9,8,-3]
,[datetime(2024,11,15),1,1,0,6,6,7,5,6,0,7,6,-5]
,[datetime(2024,11,16),2,3,0,6,6,8,5,9,9,11,7,0]
,[datetime(2024,11,17),1,1,0,1,0,1,0,1,0,0,0,-11]
,[datetime(2024,11,18),1,0,0,3,3,4,3,6,6,7,8,0]
,[datetime(2024,11,19),0,2,0,3,3,4,3,4,3,4,3,-10]
,[datetime(2024,11,20),41,44,43,45,43,45,42,45,45,42,40,31]
,[datetime(2024,11,21),3,2,0,2,0,2,0,2,0,1,0,-12]
,[datetime(2024,11,22),1,2,0,4,4,4,3,4,3,4,3,-3]
,[datetime(2024,11,23),0,3,0,4,4,5,3,9,9,8,7,0]
,[datetime(2024,11,24),2,1,0,4,4,1,0,3,3,2,0,-9]
,[datetime(2024,11,25),5,4,3,8,8,6,7,7,0,6,4,-5]
,[datetime(2024,11,26),1,2,0,4,4,3,3,3,3,3,3,-8]
,[datetime(2024,11,27),1,0,0,1,0,4,3,5,3,5,3,-5]
,[datetime(2024,11,28),2,5,4,5,4,8,7,6,7,6,5,-8]
,[datetime(2024,11,29),0,0,0,1,0,0,0,1,0,1,0,-13]
,[datetime(2024,11,30),1,1,0,1,0,0,0,0,0,0,0,-12]
,[datetime(2024,12,1),1,2,0,4,4,5,3,7,7,7,5,-4]
,[datetime(2024,12,2),5,5,3,7,7,8,6,7,6,6,13,0]
,[datetime(2024,12,3),0,0,0,2,0,2,0,2,0,1,0,-10]
,[datetime(2024,12,4),4,10,9,12,12,12,11,11,11,9,9,-7]
,[datetime(2024,12,5),5,6,3,6,6,4,0,3,3,2,0,-13]
,[datetime(2024,12,6),1,3,0,4,0,4,3,5,3,6,4,-8]
,[datetime(2024,12,7),0,2,0,2,0,3,0,2,0,1,0,-9]
,[datetime(2024,12,8),1,1,0,2,0,3,0,2,0,2,0,-12]
,[datetime(2024,12,9),1,2,0,5,5,6,4,7,7,6,4,-2]
,[datetime(2024,12,10),0,11,10,12,10,9,9,8,9,10,7,-3]
,[datetime(2024,12,11),4,2,0,3,3,5,4,6,4,5,3,-8]
,[datetime(2024,12,12),19,20,17,20,20,18,19,18,19,19,17,10]
,[datetime(2024,12,13),1,3,0,3,3,3,0,3,3,4,3,-8]
,[datetime(2024,12,14),0,3,0,3,3,3,0,4,4,5,3,-5]
,[datetime(2024,12,15),0,2,0,5,5,3,4,3,4,3,3,-8]
,[datetime(2024,12,16),2,2,0,6,6,3,2,3,2,3,0,-12]
,[datetime(2024,12,17),0,1,0,2,0,0,0,3,3,2,0,-12]
,[datetime(2024,12,18),6,10,8,10,8,8,7,8,7,8,5,-5]
,[datetime(2024,12,19),1,4,3,6,6,5,5,4,5,3,0,-12]
,[datetime(2024,12,20),3,2,0,2,0,4,3,4,3,3,0,-8]
,[datetime(2024,12,21),7,9,8,8,8,8,7,7,7,12,10,-2]
    # Add more rows as needed
]

# Prepare the initial table with just dates and delays
dates = [row[0].strftime('%d/%m/%y') for row in data]
df = pd.DataFrame({"Date": dates})

# Add delay data for each station in the format specified
for i, station in enumerate(stations):
    df[station["label"]] = [row[i + 1] for row in data]

# Fetch and merge weather data for each station
for station in stations:
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": station["longitude"],
        "longitude": station["latitude"],
        "start_date": "2023-12-23",
        "end_date": "2024-12-21",
        "daily": ["weather_code", "temperature_2m_mean", "wind_speed_10m_max"]
    }

    # Fetch data from Open-Meteo API
    try:
        response = openmeteo.weather_api(url, params=params)[0]
        print(response)
        daily = response.Daily()
        dates = pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        )

        # Ensure dates is a list, matching the length of weather data variables
        dates = list(dates)
        weather_code = daily.Variables(0).ValuesAsNumpy()
        temperature = daily.Variables(1).ValuesAsNumpy()
        wind_speed = daily.Variables(2).ValuesAsNumpy()

        # Create a temporary DataFrame to store the data for this station
        temp_df = pd.DataFrame({
            "Date": [date.strftime('%d/%m/%y') for date in dates],
            f"{station['label']}_Delay": df[station["label"]].values,
            f"weather_code_{station['label']}": weather_code,
            f"Temperature_{station['label']}": temperature,
            f"Wind_Speed_{station['label']}": wind_speed,
            f"Distance_travelled_{station['label']}": [None] * len(weather_code)  # Placeholder for Distance
        })

        # Merge this station's data back into the main DataFrame
        df = df.merge(temp_df, on="Date", how="left")
    except Exception as e:
        print(f"Error fetching data for station '{station['label']}': {e}")

# Drop the original delay columns used for initialization
df.drop(columns=[station["label"] for station in stations], inplace=True)

# Save final table with delays and weather details
df.to_csv("100stations/06573(12).csv", index=False)
print("Data has been formatted and saved to 'merged_train_weather_data.csv'.")
