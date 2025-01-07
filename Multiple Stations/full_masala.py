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
station_codes = [ 'SBC', 'MWM', 'YPR', 'BAW', 'SDVL', 'GHL', 'BNKH','DBL', 'MDLL','NDV','DBS','HHL','KIAT','TK']

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
    [datetime(2023,12,27),1,0,5,17,16,16,16,16,12,21,20,19,0,-17]
,[datetime(2023,12,28),2,0,9,12,11,16,16,16,12,17,16,18,0,-17]
,[datetime(2023,12,29),1,0,5,3,3,2,3,6,4,6,0,3,0,-30]
,[datetime(2023,12,30),0,0,8,10,0,11,11,14,12,13,0,15,0,-19]
,[datetime(2023,12,31),4,3,6,8,7,10,10,10,6,9,0,9,0,-25]
,[datetime(2024,1,1),1,0,8,9,6,9,0,12,10,14,13,13,0,-23]
,[datetime(2024,1,2),2,0,4,2,1,1,1,2,0,2,0,3,0,-33]
,[datetime(2024,1,3),1,0,7,7,5,9,9,9,5,11,10,12,0,-13]
,[datetime(2024,1,4),1,0,2,0,0,1,0,4,0,4,3,5,0,-27]
,[datetime(2024,1,5),1,0,9,10,7,9,7,6,3,5,4,2,0,-33]
,[datetime(2024,1,6),1,0,12,12,10,13,13,13,9,14,13,16,0,-14]
,[datetime(2024,1,7),0,0,3,2,0,2,0,2,0,2,0,3,0,-31]
,[datetime(2024,1,8),1,0,0,0,0,2,0,3,0,2,0,2,0,-33]
,[datetime(2024,1,9),0,0,0,0,0,0,0,0,0,0,0,0,0,-31]
,[datetime(2024,1,10),0,0,0,5,4,6,4,9,7,9,0,11,0,-23]
,[datetime(2024,1,11),8,7,9,8,7,7,7,7,3,6,5,8,0,-25]
,[datetime(2024,1,12),0,0,4,4,2,5,5,6,0,4,3,6,0,-29]
,[datetime(2024,1,13),1,0,10,11,8,13,13,15,13,16,15,15,0,-19]
,[datetime(2024,1,14),5,4,21,20,19,19,19,19,15,20,19,22,0,-13]
,[datetime(2024,1,15),1,0,6,6,4,10,0,13,11,10,7,12,0,-23]
,[datetime(2024,1,16),47,46,45,43,43,42,0,42,40,41,40,40,10,7]
,[datetime(2024,1,17),1,0,3,2,0,4,4,5,3,5,4,5,0,-31]
,[datetime(2024,1,18),1,0,1,1,0,0,0,2,0,3,0,7,0,-23]
,[datetime(2024,1,19),1,0,14,15,12,16,16,18,15,18,17,18,0,-19]
,[datetime(2024,1,20),5,4,9,9,7,14,14,13,10,16,15,20,0,-11]
,[datetime(2024,1,21),3,2,4,5,2,5,5,6,4,9,8,7,0,-24]
,[datetime(2024,1,22),0,0,0,0,0,1,0,2,0,1,0,2,0,-33]
,[datetime(2024,1,23),1,0,1,3,0,5,5,5,3,7,6,9,0,-26]
,[datetime(2024,1,24),0,0,0,2,0,2,0,2,0,0,0,0,0,-32]
,[datetime(2024,1,25),0,0,0,0,0,1,0,0,0,2,0,1,0,-36]
,[datetime(2024,1,26),0,0,1,0,0,3,3,1,0,0,0,1,0,-35]
,[datetime(2024,1,27),1,0,4,3,2,5,5,7,5,8,7,12,0,-19]
,[datetime(2024,1,28),2,0,1,0,0,1,0,3,0,2,0,2,0,-32]
,[datetime(2024,1,29),4,3,2,1,0,1,0,0,0,0,0,0,0,-33]
,[datetime(2024,1,30),0,0,1,0,0,0,0,0,0,0,0,0,0,-26]
,[datetime(2024,1,31),0,0,2,5,4,6,4,8,6,8,7,11,0,-18]
,[datetime(2024,2,1),14,13,29,40,39,43,43,47,45,46,45,48,17,25]
,[datetime(2024,2,2),1,0,0,0,0,1,0,2,0,2,0,6,0,-23]
,[datetime(2024,2,3),1,0,3,4,3,6,6,8,6,7,6,8,0,-22]
,[datetime(2024,2,4),1,0,1,0,0,1,0,3,0,1,0,0,0,-33]
,[datetime(2024,2,5),0,0,22,30,29,44,44,46,44,43,40,41,10,11]
,[datetime(2024,2,6),2,0,2,8,5,13,13,12,9,9,8,11,0,-23]
,[datetime(2024,2,7),0,0,2,2,0,3,3,5,3,2,0,1,0,-33]
,[datetime(2024,2,8),2,0,2,0,0,0,0,1,0,0,0,0,0,-33]
,[datetime(2024,2,9),1,0,5,35,34,35,34,32,30,27,26,22,20,14]
,[datetime(2024,2,10),0,0,10,18,17,30,30,32,30,44,43,43,13,13]
,[datetime(2024,2,11),0,0,2,2,0,2,0,5,0,3,0,4,0,-30]
,[datetime(2024,2,12),6,5,14,24,23,23,23,22,19,22,21,19,0,-16]
,[datetime(2024,2,13),1,0,0,2,0,3,3,4,0,4,3,27,0,-9]
,[datetime(2024,2,14),3,2,6,6,4,5,4,6,4,5,4,9,0,-23]
,[datetime(2024,2,15),2,0,1,6,5,7,5,8,6,9,8,10,0,-23]
,[datetime(2024,2,16),0,0,7,11,10,11,10,8,6,11,11,9,0,-24]
,[datetime(2024,2,17),1,0,6,4,4,4,4,6,4,22,21,19,0,-15]
,[datetime(2024,2,18),0,0,16,14,0,15,15,15,11,12,11,9,0,-25]
,[datetime(2024,2,19),1,0,2,10,0,12,12,12,8,12,11,13,0,-18]
,[datetime(2024,2,20),1,0,1,0,0,1,0,1,0,1,0,3,0,-26]
,[datetime(2024,2,21),0,0,3,9,8,9,8,10,8,8,7,8,0,-27]
,[datetime(2024,2,22),14,13,16,15,14,15,14,18,16,18,16,36,11,7]
,[datetime(2024,2,23),10,9,18,17,16,20,20,21,19,23,22,20,0,-13]
,[datetime(2024,2,24),0,0,5,3,2,6,6,6,4,11,10,15,0,-22]
,[datetime(2024,2,25),0,0,23,23,21,22,21,19,17,20,19,20,0,-13]
,[datetime(2024,2,26),47,46,48,47,46,45,46,41,39,37,35,37,7,12]
,[datetime(2024,2,27),1,0,4,5,2,6,6,5,3,2,0,1,0,-35]
,[datetime(2024,2,28),5,4,8,13,12,14,12,14,12,16,15,13,0,-18]
,[datetime(2024,2,29),5,4,11,9,0,10,10,11,9,24,23,24,0,-6]
,[datetime(2024,3,1),9,8,10,10,8,11,11,10,7,9,8,23,0,0]
,[datetime(2024,3,2),1,0,6,4,4,5,4,8,6,7,6,8,0,-23]
,[datetime(2024,3,3),5,4,7,8,5,7,5,4,0,3,0,2,0,-30]
,[datetime(2024,3,4),1,0,3,2,0,6,4,6,0,8,7,6,0,-29]
,[datetime(2024,3,5),0,0,1,7,6,6,6,5,0,4,3,5,0,-28]
,[datetime(2024,3,6),13,12,13,11,0,11,11,13,11,11,10,12,0,-23]
,[datetime(2024,3,7),1,0,6,8,7,7,7,7,3,9,8,10,0,-24]
,[datetime(2024,3,8),0,0,5,9,8,11,11,13,11,22,21,23,0,-10]
,[datetime(2024,3,9),2,0,6,4,4,6,4,6,4,7,6,7,0,-24]
,[datetime(2024,3,10),0,0,24,33,32,34,32,34,32,33,32,33,3,-1]
,[datetime(2024,3,11),0,0,13,14,0,17,0,17,15,20,19,21,0,-14]
,[datetime(2024,3,12),16,15,18,16,16,15,16,15,12,13,12,14,0,-16]
,[datetime(2024,3,13),1,0,8,8,6,11,11,10,7,4,3,14,0,-22]
,[datetime(2024,3,14),0,0,0,4,0,16,16,17,15,37,36,39,9,5]
,[datetime(2024,3,15),0,0,6,6,0,21,21,22,20,22,21,25,0,-10]
,[datetime(2024,3,16),3,2,4,13,12,13,12,21,19,25,24,29,0,-6]
,[datetime(2024,3,17),0,0,3,5,3,6,6,5,3,7,6,9,0,-23]
,[datetime(2024,3,18),1,0,5,4,3,5,3,7,5,8,7,10,0,-27]
,[datetime(2024,3,19),1,0,1,0,0,0,0,0,0,0,0,4,0,-32]
,[datetime(2024,3,20),0,0,5,6,3,9,9,11,9,13,12,15,0,-13]
,[datetime(2024,3,21),1,0,9,9,7,10,0,12,10,10,0,12,0,-23]
,[datetime(2024,3,22),0,0,5,6,3,8,8,8,0,7,6,10,0,-22]
,[datetime(2024,3,23),4,3,7,12,10,15,15,17,15,16,15,17,0,-15]
,[datetime(2024,3,24),1,0,1,19,18,19,18,16,14,13,10,18,0,-8]
,[datetime(2024,3,25),0,0,6,6,4,5,4,4,0,3,0,2,0,-34]
,[datetime(2024,3,26),1,0,1,4,3,3,3,2,0,1,0,0,0,-37]
,[datetime(2024,3,27),10,9,10,8,8,10,8,13,11,13,0,15,0,-19]
,[datetime(2024,3,28),1,0,1,0,0,1,0,2,0,4,3,5,0,-31]
,[datetime(2024,3,29),0,0,2,1,0,1,0,1,0,0,0,0,0,-35]
,[datetime(2024,3,30),0,0,1,3,0,1,0,0,0,0,0,13,0,-18]
,[datetime(2024,3,31),1,0,8,6,6,5,6,6,2,3,0,2,0,-29]
,[datetime(2024,4,1),0,0,25,24,23,34,34,29,27,23,23,19,0,-15]
,[datetime(2024,4,2),0,0,0,0,0,2,0,2,0,2,0,3,0,-30]
,[datetime(2024,4,3),0,0,2,1,0,0,0,0,0,2,0,0,0,-33]
,[datetime(2024,4,4),7,6,7,5,5,5,5,44,42,40,38,39,9,6]
,[datetime(2024,4,5),1,0,11,18,17,17,0,16,14,16,15,16,0,-17]
,[datetime(2024,4,6),1,0,1,0,0,0,0,2,0,0,0,1,0,-18]
,[datetime(2024,4,7),2,0,8,6,6,6,6,6,2,5,3,5,0,-31]
,[datetime(2024,4,8),1,0,10,10,0,13,13,15,12,17,16,17,0,-15]
,[datetime(2024,4,9),0,0,1,0,0,2,0,2,0,0,0,0,0,-34]
,[datetime(2024,4,10),0,0,1,3,0,2,0,0,0,0,0,0,0,-33]
,[datetime(2024,4,11),0,0,0,0,0,2,0,1,0,0,0,0,0,-33]
,[datetime(2024,4,12),0,0,7,8,5,9,0,8,6,7,6,12,0,-23]
,[datetime(2024,4,13),1,0,4,3,2,7,7,7,3,10,8,12,0,-20]
,[datetime(2024,4,14),2,0,7,7,5,8,8,8,4,7,6,7,0,-22]
,[datetime(2024,4,15),2,0,6,15,14,16,14,17,15,16,15,16,0,-8]
,[datetime(2024,4,16),20,19,20,18,18,17,18,15,14,12,0,25,0,-3]
,[datetime(2024,4,17),0,0,11,12,9,12,12,14,12,16,15,12,0,-23]
,[datetime(2024,4,18),0,0,1,0,0,0,0,2,0,0,0,0,0,-24]
,[datetime(2024,4,19),0,0,2,3,0,2,0,3,0,1,0,1,0,-32]
,[datetime(2024,4,20),3,2,3,5,4,5,4,4,0,5,4,5,0,-25]
,[datetime(2024,4,21),1,0,3,4,3,3,3,5,3,4,3,7,0,-27]
,[datetime(2024,4,22),0,0,8,18,0,17,17,15,13,12,9,9,0,-13]
,[datetime(2024,4,23),0,0,0,0,0,7,7,8,6,8,7,10,0,-21]
,[datetime(2024,4,24),0,0,5,9,7,8,7,9,7,8,7,10,0,-23]
,[datetime(2024,4,25),2,0,4,2,2,3,0,3,0,3,0,9,0,-25]
,[datetime(2024,4,26),1,0,16,15,14,14,14,13,0,11,10,10,0,-24]
,[datetime(2024,4,27),1,0,14,13,12,17,0,19,0,18,17,21,0,-9]
,[datetime(2024,4,28),4,3,9,17,16,29,29,30,28,32,29,33,4,-3]
,[datetime(2024,4,29),4,3,6,5,4,18,18,21,19,20,19,22,0,-11]
,[datetime(2024,4,30),0,0,7,5,5,4,5,4,0,2,0,0,0,-28]
,[datetime(2024,5,1),0,0,1,14,12,13,12,10,8,5,0,5,0,-23]
,[datetime(2024,5,2),6,5,7,6,5,8,8,10,8,9,8,12,0,-25]
,[datetime(2024,5,3),1,0,0,24,23,23,23,23,19,21,20,22,0,-13]
,[datetime(2024,5,4),23,22,24,25,22,24,22,22,18,24,23,22,10,-3]
,[datetime(2024,5,5),1,0,15,20,19,19,0,20,0,21,20,26,0,-5]
,[datetime(2024,5,6),1,0,5,6,3,6,0,7,0,9,8,13,0,-21]
,[datetime(2024,5,7),1,0,2,0,0,7,7,12,9,17,16,22,0,-11]
,[datetime(2024,5,8),1,0,6,9,8,12,12,13,0,15,13,14,0,-18]
,[datetime(2024,5,9),1,0,1,1,0,1,0,6,3,7,6,5,0,-31]
,[datetime(2024,5,10),0,0,6,4,0,10,10,12,10,7,6,17,0,-13]
,[datetime(2024,5,11),0,0,5,3,3,12,12,15,13,13,0,12,0,-22]
,[datetime(2024,5,12),0,0,8,9,6,12,12,13,11,18,17,29,0,-8]
,[datetime(2024,5,13),0,0,3,2,0,4,4,8,6,7,6,13,0,-22]
,[datetime(2024,5,14),4,3,4,4,2,3,2,1,0,0,0,0,0,-38]
,[datetime(2024,5,15),1,0,4,3,2,3,2,0,0,0,0,4,0,-23]
,[datetime(2024,5,16),5,4,6,5,4,5,4,4,0,4,3,6,0,-24]
,[datetime(2024,5,17),1,0,6,5,4,5,4,6,4,5,4,6,0,-26]
,[datetime(2024,5,18),0,0,3,5,4,5,4,7,5,6,5,3,0,-31]
,[datetime(2024,5,19),1,0,8,8,6,7,6,6,2,5,4,4,0,-31]
,[datetime(2024,5,20),0,0,2,0,0,2,0,3,0,1,0,1,0,-30]
,[datetime(2024,5,21),1,0,0,0,0,1,0,1,0,2,0,1,0,-36]
,[datetime(2024,5,22),0,0,4,2,2,3,2,2,0,1,0,1,0,-32]
,[datetime(2024,5,23),0,0,1,9,8,13,11,13,11,11,10,10,0,-19]
,[datetime(2024,5,24),0,0,1,0,0,1,0,1,0,20,19,17,0,-17]
,[datetime(2024,5,25),1,0,5,15,14,16,14,17,15,15,14,16,0,-19]
,[datetime(2024,5,26),1,0,6,6,4,9,9,11,8,14,11,17,0,-12]
,[datetime(2024,5,27),1,0,7,9,8,8,8,9,7,13,11,16,0,-20]
,[datetime(2024,5,28),8,7,6,6,4,5,4,5,3,4,3,2,0,-17]
,[datetime(2024,5,29),0,0,1,9,8,9,8,7,4,6,5,5,0,-23]
,[datetime(2024,5,30),1,0,3,4,3,6,6,9,7,10,9,11,0,-23]
,[datetime(2024,5,31),0,0,1,1,0,0,0,1,0,0,0,0,0,-36]
,[datetime(2024,6,1),2,0,8,16,0,15,15,14,11,19,18,22,0,-9]
,[datetime(2024,6,2),1,0,8,10,9,9,9,10,8,9,8,8,0,-28]
,[datetime(2024,6,3),20,19,19,27,0,0,0,0,0,0,0,0,0,2]
,[datetime(2024,6,4),1,0,3,17,16,21,0,24,21,25,24,25,0,-9]
,[datetime(2024,6,5),12,11,11,12,9,20,0,20,18,23,0,27,0,-3]
,[datetime(2024,6,6),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,6,7),0,0,2,0,0,2,0,2,0,1,0,3,0,-22]
,[datetime(2024,6,8),0,0,12,25,24,28,28,28,24,31,30,33,11,11]
,[datetime(2024,6,9),41,40,42,40,39,42,42,41,38,35,34,31,2,10]
,[datetime(2024,6,10),2,0,3,1,0,3,0,5,0,6,5,11,0,-18]
,[datetime(2024,6,11),0,0,1,2,0,19,19,18,0,18,17,20,0,-9]
,[datetime(2024,6,12),0,0,5,13,12,13,0,14,12,16,15,15,0,-18]
,[datetime(2024,6,13),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,6,14),1,0,2,2,0,7,0,15,13,17,16,20,0,-14]
,[datetime(2024,6,15),1,0,5,11,10,12,10,11,9,10,9,11,0,-23]
,[datetime(2024,6,16),1,0,10,29,28,32,32,38,36,38,37,40,10,10]
,[datetime(2024,6,17),1,0,4,5,4,6,4,13,11,16,15,17,0,-16]
,[datetime(2024,6,18),1,0,5,6,3,9,9,9,5,9,8,11,0,-27]
,[datetime(2024,6,19),1,0,0,0,0,1,0,1,0,2,0,0,0,-27]
,[datetime(2024,6,20),5,4,12,12,10,12,10,14,12,34,33,35,10,7]
,[datetime(2024,6,21),0,0,6,7,4,9,9,9,5,10,9,14,0,-16]
,[datetime(2024,6,22),2,0,23,26,25,26,25,25,21,36,34,33,3,7]
,[datetime(2024,6,23),1,0,7,7,5,12,12,12,8,23,22,23,0,-8]
,[datetime(2024,6,24),0,0,5,5,3,4,3,5,3,5,4,6,0,-28]
,[datetime(2024,6,25),0,0,9,7,0,9,9,10,8,10,9,12,0,-23]
,[datetime(2024,6,26),1,0,4,3,0,0,0,10,8,7,4,8,0,-21]
,[datetime(2024,6,27),0,0,1,1,0,3,3,6,4,6,5,6,0,-24]
,[datetime(2024,6,28),4,3,6,5,4,4,4,7,5,9,8,13,0,-21]
,[datetime(2024,6,29),4,3,4,2,1,4,4,6,4,7,6,5,0,-22]
,[datetime(2024,6,30),5,4,4,3,0,2,0,4,0,5,4,6,0,-23]
,[datetime(2024,7,1),1,0,1,7,6,14,14,15,0,18,17,21,0,-6]
,[datetime(2024,7,2),1,0,0,11,10,14,14,14,10,23,22,21,0,-9]
,[datetime(2024,7,3),4,3,9,13,12,17,16,20,18,20,19,23,0,-7]
,[datetime(2024,7,4),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,7,5),0,0,3,2,0,4,4,7,5,9,8,13,0,-18]
,[datetime(2024,7,6),0,0,11,11,9,11,0,14,0,12,0,28,0,14]
,[datetime(2024,7,7),0,0,3,4,3,10,10,11,0,12,11,14,0,-17]
,[datetime(2024,7,8),0,0,8,9,6,11,0,11,0,10,9,12,0,-19]
,[datetime(2024,7,9),11,10,12,11,10,13,13,13,0,16,0,33,12,8]
,[datetime(2024,7,10),1,0,4,9,8,8,8,9,7,10,9,12,0,-21]
,[datetime(2024,7,11),11,10,12,16,15,19,19,19,0,17,16,24,0,11]
,[datetime(2024,7,12),1,0,5,3,3,8,8,9,7,10,9,13,0,-17]
,[datetime(2024,7,13),1,0,12,11,0,12,12,11,8,10,9,14,0,-21]
,[datetime(2024,7,14),0,0,5,6,3,6,6,5,3,2,0,3,0,-28]
,[datetime(2024,7,15),1,0,6,17,16,18,16,19,17,24,23,26,0,-7]
,[datetime(2024,7,16),1,0,1,1,0,4,4,4,0,0,0,5,0,-28]
,[datetime(2024,7,17),0,0,6,6,4,9,9,9,5,9,8,10,0,-22]
,[datetime(2024,7,18),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,7,19),6,5,5,6,3,5,3,7,5,6,5,2,0,-22]
,[datetime(2024,7,20),0,0,12,11,0,17,17,18,16,16,15,14,0,-17]
,[datetime(2024,7,21),0,0,7,5,5,6,5,8,6,8,7,10,0,-20]
,[datetime(2024,7,22),0,0,3,3,0,4,4,5,3,8,7,9,0,-23]
,[datetime(2024,7,23),0,0,2,2,0,2,0,0,0,0,0,2,0,-33]
,[datetime(2024,7,24),8,7,6,6,5,5,5,6,4,13,12,12,0,-15]
,[datetime(2024,7,25),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,7,26),1,0,3,1,0,2,0,2,0,1,0,3,0,-27]
,[datetime(2024,7,27),1,0,7,6,5,6,0,5,3,6,5,8,0,-26]
,[datetime(2024,7,28),0,0,11,10,0,10,10,11,9,10,9,13,0,-21]
,[datetime(2024,7,29),1,0,0,11,10,14,14,17,0,19,16,18,0,-13]
,[datetime(2024,7,30),2,0,17,15,15,14,15,14,11,10,0,11,0,-21]
,[datetime(2024,7,31),0,0,6,7,4,6,4,4,0,3,0,5,0,-27]
,[datetime(2024,8,1),2,0,2,1,0,2,0,4,0,6,5,10,0,-18]
,[datetime(2024,8,2),1,0,6,4,0,5,5,8,6,8,7,11,0,-19]
,[datetime(2024,8,3),1,0,3,6,5,9,9,13,11,12,11,16,0,-14]
,[datetime(2024,8,4),0,0,5,3,3,2,3,2,0,3,0,5,0,-23]
,[datetime(2024,8,5),0,0,2,1,0,4,4,6,4,5,4,10,0,-23]
,[datetime(2024,8,6),0,0,4,2,0,3,3,4,0,3,0,5,0,-27]
,[datetime(2024,8,7),1,0,10,12,11,12,11,15,13,15,14,17,0,-16]
,[datetime(2024,8,8),9,8,15,14,13,15,13,20,18,21,20,20,0,-13]
,[datetime(2024,8,9),4,3,2,2,0,4,4,5,3,4,3,5,0,-33]
,[datetime(2024,8,10),6,5,11,21,20,49,49,50,48,48,47,49,19,15]
,[datetime(2024,8,11),None,None,8,8,6,7,6,8,6,7,6,11,0,-25]
,[datetime(2024,8,12),15,14,15,14,13,15,13,17,15,16,15,25,0,-3]
,[datetime(2024,8,13),2,0,1,2,1,2,4,5,3,5,4,6,0,-29]
,[datetime(2024,8,14),1,0,3,8,7,10,10,21,19,20,19,36,10,6]
,[datetime(2024,8,15),1,0,1,2,0,1,0,0,0,1,0,1,0,-32]
,[datetime(2024,8,16),2,0,9,14,12,14,12,16,14,19,17,19,0,-13]
,[datetime(2024,8,17),1,0,3,7,6,8,6,9,7,10,9,13,0,-21]
,[datetime(2024,8,18),3,0,13,12,11,13,11,14,12,17,16,19,0,-13]
,[datetime(2024,8,19),1,0,10,13,11,13,11,13,11,12,11,15,0,-17]
,[datetime(2024,8,20),1,0,12,11,10,10,10,11,9,11,10,12,0,-20]
,[datetime(2024,8,21),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,8,22),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,8,23),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,8,24),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,8,25),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,8,26),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,8,27),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,8,28),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,8,29),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,8,30),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,8,31),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,1),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,2),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,3),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,4),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,5),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,6),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,7),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,8),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,9),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,10),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,11),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,12),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,13),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,14),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,15),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,16),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,17),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,18),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,19),None,None,None,None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,9,20),0,0,2,4,3,5,3,9,7,8,7,9,0,-27]
,[datetime(2024,9,21),0,0,11,10,9,11,9,14,12,13,12,15,0,-18]
,[datetime(2024,9,22),1,0,13,13,11,15,14,15,13,17,16,22,0,-8]
,[datetime(2024,9,23),0,0,6,7,4,7,7,7,3,7,6,10,0,-24]
,[datetime(2024,9,24),57,56,55,53,51,52,51,51,47,50,49,52,22,24]
,[datetime(2024,9,25),0,0,10,14,13,16,16,18,16,19,18,21,0,-10]
,[datetime(2024,9,26),2,0,1,0,0,3,3,3,0,3,0,2,0,-32]
,[datetime(2024,9,27),0,0,1,1,0,2,0,3,0,5,4,3,0,-28]
,[datetime(2024,9,28),1,0,8,7,0,6,5,7,5,7,6,11,0,-14]
,[datetime(2024,9,29),0,0,0,9,8,10,8,13,10,12,11,20,0,-13]
,[datetime(2024,9,30),4,3,19,17,16,17,16,17,15,18,17,21,0,-8]
,[datetime(2024,10,1),1,0,0,0,0,0,0,1,0,1,0,1,0,-33]
,[datetime(2024,10,2),1,0,7,10,9,13,13,14,12,11,8,13,0,-18]
,[datetime(2024,10,3),6,5,11,10,9,12,12,13,11,13,12,15,0,-13]
,[datetime(2024,10,4),0,0,3,8,7,9,7,12,10,13,12,17,0,-13]
,[datetime(2024,10,5),1,0,10,10,8,13,13,16,14,16,15,20,0,-15]
,[datetime(2024,10,6),1,0,8,10,9,11,9,14,12,15,14,16,0,-21]
,[datetime(2024,10,7),1,0,3,3,0,3,3,3,0,2,0,2,0,-28]
,[datetime(2024,10,8),0,0,0,0,0,2,0,2,0,2,0,3,0,-29]
,[datetime(2024,10,9),1,0,6,6,4,8,8,9,7,10,9,14,0,-19]
,[datetime(2024,10,10),3,0,4,5,3,12,12,11,8,11,10,11,0,-21]
,[datetime(2024,10,11),0,0,1,0,0,2,0,2,0,2,0,5,0,-29]
,[datetime(2024,10,12),0,0,4,4,3,7,6,7,5,8,7,9,0,-22]
,[datetime(2024,10,13),1,0,7,6,5,7,5,7,5,6,5,7,0,-29]
,[datetime(2024,10,14),4,3,3,4,3,3,3,2,0,0,0,3,0,-31]
,[datetime(2024,10,15),1,0,0,4,3,6,6,11,9,13,12,33,3,1]
,[datetime(2024,10,16),0,0,5,8,7,14,14,14,10,17,16,20,0,-8]
,[datetime(2024,10,17),13,12,22,21,20,22,20,28,26,45,44,46,16,15]
,[datetime(2024,10,18),1,0,3,2,0,3,3,5,3,7,6,8,0,-23]
,[datetime(2024,10,19),0,0,4,4,3,6,6,8,6,7,6,9,0,-22]
,[datetime(2024,10,20),0,0,13,14,11,13,11,13,11,15,14,17,0,-18]
,[datetime(2024,10,21),0,0,4,7,6,6,6,8,6,7,6,8,0,-25]
,[datetime(2024,10,22),1,0,1,0,0,1,0,2,0,0,0,2,0,-30]
,[datetime(2024,10,23),1,0,3,3,0,3,0,2,0,6,5,6,0,-29]
,[datetime(2024,10,24),1,0,5,6,3,6,6,4,0,2,0,4,0,-28]
,[datetime(2024,10,25),1,0,8,8,6,9,9,9,5,8,7,13,0,-19]
,[datetime(2024,10,26),1,0,10,11,8,22,22,24,22,42,41,43,13,13]
,[datetime(2024,10,27),0,0,16,18,17,18,0,18,16,17,16,16,0,-16]
,[datetime(2024,10,28),1,0,3,13,12,15,15,15,11,16,16,15,0,-19]
,[datetime(2024,10,29),0,0,0,7,6,9,9,11,9,12,11,14,0,-22]
,[datetime(2024,10,30),3,0,7,5,5,5,5,7,5,6,5,7,0,-28]
,[datetime(2024,10,31),0,0,1,1,0,3,3,3,0,2,0,6,0,-25]
,[datetime(2024,11,1),0,0,11,11,9,12,12,14,12,13,12,14,0,-21]
,[datetime(2024,11,2),1,0,25,26,23,26,26,32,30,33,32,32,2,-2]
,[datetime(2024,11,3),0,0,4,6,5,5,5,5,3,6,5,5,0,-31]
,[datetime(2024,11,4),1,0,14,12,12,15,15,15,11,16,15,19,0,-9]
,[datetime(2024,11,5),7,6,7,7,5,7,5,9,7,11,10,12,0,-20]
,[datetime(2024,11,6),0,0,14,13,12,13,12,11,8,8,7,8,0,-29]
,[datetime(2024,11,7),0,0,0,9,6,12,12,15,13,39,38,39,9,7]
,[datetime(2024,11,8),1,0,1,2,0,1,0,1,0,19,18,24,0,-8]
,[datetime(2024,11,9),14,13,14,12,12,14,12,17,15,18,17,20,0,-12]
,[datetime(2024,11,10),0,0,5,10,9,12,12,12,8,10,9,13,0,-18]
,[datetime(2024,11,11),1,0,4,5,4,8,8,7,4,7,6,8,0,-23]
,[datetime(2024,11,12),0,0,2,0,0,1,0,4,0,5,4,8,0,-23]
,[datetime(2024,11,13),0,0,3,4,3,7,7,5,3,10,9,14,0,-19]
,[datetime(2024,11,14),1,0,1,0,0,0,0,0,0,1,0,0,0,-32]
,[datetime(2024,11,15),0,0,9,12,11,14,14,19,17,19,18,24,0,-12]
,[datetime(2024,11,16),1,0,6,5,4,6,4,8,6,9,8,14,0,-21]
,[datetime(2024,11,17),1,0,3,3,0,3,3,5,3,7,6,8,0,-23]
,[datetime(2024,11,18),1,0,4,5,3,4,3,5,3,5,4,7,0,-28]
,[datetime(2024,11,19),0,0,2,0,0,0,0,2,0,2,0,6,0,-23]
,[datetime(2024,11,20),0,0,4,4,3,4,3,8,0,11,10,12,0,-23]
,[datetime(2024,11,21),2,0,3,7,6,11,11,14,12,12,11,12,0,-24]
,[datetime(2024,11,22),1,0,0,5,4,5,4,6,4,8,7,10,0,-25]
,[datetime(2024,11,23),61,60,62,64,63,66,66,70,None,None,None,None,None,None]
,[datetime(2024,11,24),0,0,6,6,4,5,4,8,6,11,10,14,0,-18]
,[datetime(2024,11,25),0,0,8,10,9,11,9,9,5,12,11,12,0,-20]
,[datetime(2024,11,26),1,0,5,3,3,4,3,6,4,11,10,13,0,-23]
,[datetime(2024,11,27),0,0,2,2,0,4,4,4,0,3,0,5,0,-28]
,[datetime(2024,11,28),0,0,1,2,0,5,5,6,4,9,6,10,0,-2]
,[datetime(2024,11,29),0,0,4,3,0,3,3,4,0,4,3,4,0,-32]
,[datetime(2024,11,30),0,0,4,2,0,2,0,6,4,6,5,8,0,-23]
,[datetime(2024,12,1),0,0,11,9,9,9,9,8,5,9,8,12,0,-26]
,[datetime(2024,12,2),0,0,14,13,12,15,15,16,14,16,15,18,0,-17]
,[datetime(2024,12,3),0,0,0,0,0,2,0,2,0,4,3,6,0,-30]
,[datetime(2024,12,4),0,0,9,7,6,8,6,8,6,10,9,11,0,-26]
,[datetime(2024,12,5),0,0,1,1,0,1,0,1,0,0,0,4,0,-28]
,[datetime(2024,12,6),0,0,6,5,4,12,12,12,8,11,10,13,0,-19]
,[datetime(2024,12,7),1,0,3,7,6,9,9,9,5,8,7,8,0,-27]
,[datetime(2024,12,8),1,0,0,1,0,2,0,2,0,4,3,8,0,-21]
,[datetime(2024,12,9),4,3,3,4,3,4,3,6,4,4,3,4,0,-29]
,[datetime(2024,12,10),0,0,0,0,0,0,0,1,0,1,0,2,0,-30]
,[datetime(2024,12,11),0,0,6,6,4,6,4,5,3,7,6,5,0,-28]
,[datetime(2024,12,12),1,0,1,0,0,1,0,4,0,28,27,30,0,-4]
,[datetime(2024,12,13),1,0,4,8,7,8,7,9,7,11,10,13,0,-18]
,[datetime(2024,12,14),1,0,8,12,11,11,11,13,11,13,12,14,0,-18]
,[datetime(2024,12,15),0,0,3,2,1,2,1,3,0,1,0,2,0,-32]
,[datetime(2024,12,16),0,0,2,5,4,4,4,6,4,7,6,11,0,-22]
,[datetime(2024,12,17),0,0,0,0,0,3,3,3,0,5,4,2,0,-33]
,[datetime(2024,12,18),1,0,5,6,3,6,6,7,5,9,8,11,0,-23]
,[datetime(2024,12,19),0,0,2,3,0,3,3,6,4,28,28,30,3,-2]
,[datetime(2024,12,20),1,0,3,1,0,1,0,25,23,25,24,29,0,-4]
,[datetime(2024,12,21),0,0,16,25,24,26,24,26,24,25,24,30,3,-3]
,[datetime(2024,12,22),0,0,8,6,6,7,6,7,5,6,5,7,0,-28]
,[datetime(2024,12,23),1,0,3,4,3,3,3,5,3,6,5,8,0,-25]
,[datetime(2024,12,24),1,0,2,0,0,3,3,4,0,4,3,6,0,-22]
,[datetime(2024,12,25),0,0,13,31,30,35,35,34,31,35,34,36,6,8]
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
        "start_date": "2023-12-27",
        "end_date": "2024-12-25",
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
df.to_csv("100stations/06575(14).csv", index=False)
print("Data has been formatted and saved to 'merged_train_weather_data.csv'.")
