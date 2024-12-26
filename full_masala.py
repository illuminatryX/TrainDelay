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
station_codes = ['SBC', 'BNC', 'BNCE', 'BYLP', 'KJM', 'HDIH', 'WFD', 'DKN', 'MLO', 'TCL', 'BWT']

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
    [datetime(2023,12,21),15,19,18,26,27,25,34,35,38,34,27]
,[datetime(2023,12,23),1,3,3,8,8,6,12,16,16,16,4]
,[datetime(2023,12,24),17,17,17,14,13,13,9,7,6,49,42]
,[datetime(2023,12,22),21,21,21,23,27,26,31,36,41,45,38]
,[datetime(2023,12,25),2,1,2,3,11,10,14,14,14,10,0]
,[datetime(2023,12,26),0,1,0,1,2,0,3,4,7,9,-2]
,[datetime(2023,12,27),3,3,3,9,9,8,11,13,15,11,-5]
,[datetime(2023,12,28),21,36,36,34,35,0,37,36,35,32,23]
,[datetime(2023,12,29),1,1,0,0,9,9,13,19,20,19,3]
,[datetime(2023,12,30),42,43,42,49,54,54,68,69,69,71,55]
,[datetime(2023,12,31),1,2,2,3,3,3,7,9,9,14,9]
,[datetime(2024,1,1),1,1,0,1,1,0,4,4,4,6,-2]
,[datetime(2024,1,2),1,1,0,4,8,7,12,14,17,16,5]
,[datetime(2024,1,3),13,14,13,17,16,15,18,19,21,23,6]
,[datetime(2024,1,4),10,17,16,17,18,16,20,23,26,38,31]
,[datetime(2024,1,5),1,3,3,0,11,10,15,16,19,19,10]
,[datetime(2024,1,6),10,11,10,13,0,0,18,21,25,23,15]
,[datetime(2024,1,7),0,2,2,1,3,3,23,23,22,21,15]
,[datetime(2024,1,8),0,2,2,33,39,0,44,45,47,50,44]
,[datetime(2024,1,9),1,1,0,6,9,9,10,11,19,23,14]
,[datetime(2024,1,10),6,6,6,32,34,0,34,37,36,35,28]
,[datetime(2024,1,11),0,8,7,13,12,0,19,22,25,42,35]
,[datetime(2024,1,12),35,36,35,37,37,35,43,51,54,55,48]
,[datetime(2024,1,13),9,8,9,11,12,10,14,12,11,7,11]
,[datetime(2024,1,14),0,1,0,3,3,3,8,13,17,13,10]
,[datetime(2024,1,15),0,0,0,5,6,4,13,13,18,17,9]
,[datetime(2024,1,16),16,17,16,19,20,20,21,25,27,28,15]
,[datetime(2024,1,17),2,3,2,6,6,4,8,10,11,11,1]
,[datetime(2024,1,18),1,2,0,9,13,13,16,20,17,12,14]
,[datetime(2024,1,19),1,13,13,21,22,20,28,29,34,36,27]
,[datetime(2024,1,20),1,2,2,7,7,6,10,12,15,16,5]
,[datetime(2024,1,21),71,71,71,69,71,71,74,76,75,76,64]
,[datetime(2024,1,22),11,11,11,14,13,12,14,19,21,19,11]
,[datetime(2024,1,23),0,38,36,33,32,32,28,26,25,21,13]
,[datetime(2024,1,24),1,5,5,6,5,4,18,34,37,38,27]
,[datetime(2024,1,25),33,33,33,90,92,92,88,95,96,96,84]
,[datetime(2024,1,26),40,40,40,39,41,41,49,51,52,53,38]
,[datetime(2024,1,27),10,12,12,13,15,14,21,22,22,18,10]
,[datetime(2024,1,28),6,8,8,8,8,7,9,9,12,12,8]
,[datetime(2024,1,29),0,2,2,3,2,0,2,3,3,2,0]
,[datetime(2024,1,30),0,0,0,3,5,4,6,8,12,25,11]
,[datetime(2024,1,31),31,32,31,62,70,69,74,76,80,77,60]
,[datetime(2024,2,1),0,9,9,9,12,0,14,34,39,42,33]
,[datetime(2024,2,2),1,3,3,6,6,4,9,8,8,9,0]
,[datetime(2024,2,3),5,17,17,14,13,13,9,7,6,0,14]
,[datetime(2024,2,4),0,1,0,8,12,12,8,6,15,16,11]
,[datetime(2024,2,5),0,0,0,1,1,0,6,5,4,8,12]
,[datetime(2024,2,6),3,4,3,5,5,3,6,8,7,9,1]
,[datetime(2024,2,7),0,2,2,4,2,0,10,9,19,15,4]
,[datetime(2024,2,8),20,20,20,17,29,28,31,29,38,38,32]
,[datetime(2024,2,9),24,24,24,25,26,26,30,33,35,31,15]
,[datetime(2024,2,10),13,14,13,26,27,25,32,34,31,27,10]
,[datetime(2024,2,11),0,0,0,4,8,7,3,10,11,14,4]
,[datetime(2024,2,12),0,18,18,16,15,14,14,13,11,7,15]
,[datetime(2024,2,13),18,20,18,22,19,19,15,13,12,8,15]
,[datetime(2024,2,14),0,2,2,1,3,3,10,11,11,18,7]
,[datetime(2024,2,15),1,1,0,0,1,0,10,12,12,13,4]
,[datetime(2024,2,16),None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,2,17),13,14,13,14,13,12,13,13,14,15,12]
,[datetime(2024,2,18),0,2,2,4,4,3,8,10,10,10,9]
,[datetime(2024,2,19),14,15,14,14,13,13,18,18,15,29,15]
,[datetime(2024,2,20),5,8,8,6,6,4,9,11,13,20,9]
,[datetime(2024,2,21),5,7,5,11,11,10,14,13,12,12,14]
,[datetime(2024,2,22),0,12,10,10,12,12,14,13,0,0,12]
,[datetime(2024,2,23),0,1,0,8,7,7,17,18,17,13,9]
,[datetime(2024,2,24),1,1,0,5,7,7,8,11,14,24,15]
,[datetime(2024,2,25),2,3,2,2,2,0,9,8,12,8,6]
,[datetime(2024,2,26),7,8,7,10,17,17,20,22,24,26,15]
,[datetime(2024,2,27),19,26,26,28,30,30,32,32,37,35,17]
,[datetime(2024,2,28),0,2,2,2,0,0,10,16,18,19,12]
,[datetime(2024,2,29),0,1,0,5,8,8,14,15,15,22,13]
,[datetime(2024,3,1),0,0,0,2,0,0,8,10,13,14,4]
,[datetime(2024,3,2),1,2,2,8,10,10,13,15,18,18,13]
,[datetime(2024,3,3),0,0,0,7,9,9,17,17,18,19,9]
,[datetime(2024,3,4),5,5,5,2,11,11,18,18,19,21,10]
,[datetime(2024,3,5),9,11,9,17,17,16,20,23,23,24,13]
,[datetime(2024,3,6),1,3,2,16,16,0,21,23,25,21,15]
,[datetime(2024,3,7),0,1,0,9,9,7,11,11,16,16,6]
,[datetime(2024,3,8),25,26,25,24,28,28,27,25,24,20,14]
,[datetime(2024,3,9),0,1,0,10,9,9,12,13,13,15,10]
,[datetime(2024,3,10),1,0,0,0,4,4,7,10,13,15,8]
,[datetime(2024,3,11),0,1,0,0,7,0,7,8,11,13,3]
,[datetime(2024,3,12),0,0,0,4,5,4,8,6,9,9,5]
,[datetime(2024,3,13),0,0,0,2,3,3,11,11,12,11,9]
,[datetime(2024,3,14),0,1,0,4,3,3,4,8,8,9,5]
,[datetime(2024,3,15),0,1,0,6,7,5,8,9,11,13,8]
,[datetime(2024,3,16),7,8,7,9,10,10,20,20,20,19,4]
,[datetime(2024,3,17),0,3,3,4,5,3,11,14,15,16,14]
,[datetime(2024,3,18),1,1,0,2,3,3,5,5,8,9,7]
,[datetime(2024,3,19),9,14,13,19,19,17,19,19,19,17,10]
,[datetime(2024,3,20),0,2,2,3,2,0,0,16,14,11,3]
,[datetime(2024,3,21),0,1,0,9,24,24,16,21,23,25,11]
,[datetime(2024,3,22),19,20,19,23,24,24,26,27,27,23,10]
,[datetime(2024,3,23),17,17,17,18,18,16,20,21,22,19,12]
,[datetime(2024,3,24),2,4,2,6,7,7,8,13,11,7,14]
,[datetime(2024,3,25),17,18,17,18,21,21,22,24,27,31,14]
,[datetime(2024,3,26),0,1,0,1,1,0,2,1,0,0,-13]
,[datetime(2024,3,27),1,2,2,4,5,4,6,5,6,7,0]
,[datetime(2024,3,28),11,13,11,13,13,11,15,18,21,28,15]
,[datetime(2024,3,29),0,0,0,1,0,0,0,2,3,4,-5]
,[datetime(2024,3,30),10,10,10,8,11,11,11,16,16,17,3]
,[datetime(2024,3,31),0,1,0,9,14,13,14,15,17,21,14]
,[datetime(2024,4,1),0,1,0,5,10,9,15,17,17,18,14]
,[datetime(2024,4,2),19,21,21,24,27,27,31,32,38,39,35]
,[datetime(2024,4,3),1,8,6,3,9,9,12,11,10,15,6]
,[datetime(2024,4,4),0,2,2,0,0,0,4,4,7,8,0]
,[datetime(2024,4,5),1,2,2,8,12,12,13,15,16,18,14]
,[datetime(2024,4,6),16,17,16,13,22,23,19,16,18,16,15]
,[datetime(2024,4,7),5,8,8,8,9,7,11,12,12,13,6]
,[datetime(2024,4,8),1,1,0,9,8,0,8,11,12,12,-1]
,[datetime(2024,4,9),0,2,2,9,12,12,14,25,16,12,2]
,[datetime(2024,4,10),1,1,0,59,80,79,81,86,98,102,94]
,[datetime(2024,4,11),0,1,0,0,1,0,0,6,18,14,1]
,[datetime(2024,4,12),1,2,2,0,1,0,0,0,16,12,10]
,[datetime(2024,4,13),1,3,3,3,4,4,6,8,8,9,1]
,[datetime(2024,4,14),3,4,3,7,7,6,8,11,9,11,2]
,[datetime(2024,4,15),7,7,7,15,15,14,16,17,19,18,10]
,[datetime(2024,4,16),6,9,9,11,14,14,17,17,27,27,11]
,[datetime(2024,4,17),3,6,6,7,7,6,15,17,16,12,5]
,[datetime(2024,4,18),23,24,23,26,27,27,28,28,52,48,15]
,[datetime(2024,4,19),0,1,0,3,11,11,14,17,15,18,5]
,[datetime(2024,4,20),0,1,0,4,5,4,7,7,6,8,5]
,[datetime(2024,4,21),1,4,3,6,8,7,10,13,14,9,11]
,[datetime(2024,4,22),8,13,12,15,14,14,14,13,12,8,2]
,[datetime(2024,4,23),3,5,3,6,6,5,9,10,9,15,4]
,[datetime(2024,4,24),0,2,2,3,4,3,4,5,5,11,7]
,[datetime(2024,4,25),21,24,24,24,29,29,33,34,33,35,19]
,[datetime(2024,4,26),0,5,4,5,6,0,7,10,10,8,3]
,[datetime(2024,4,27),5,17,16,22,20,21,20,20,20,22,12]
,[datetime(2024,4,28),8,9,8,8,12,0,16,18,16,13,13]
,[datetime(2024,4,29),1,0,0,13,12,13,13,22,24,19,10]
,[datetime(2024,4,30),4,7,7,9,9,7,11,12,16,18,7]
,[datetime(2024,5,1),44,49,48,51,50,49,89,118,135,151,141]
,[datetime(2024,5,2),15,19,19,24,26,26,26,24,21,19,10]
,[datetime(2024,5,3),0,1,0,3,5,4,0,11,14,15,7]
,[datetime(2024,5,4),None,None,None,None,None,None,17,19,17,14,-1]
,[datetime(2024,5,5),4,14,13,18,22,22,29,30,31,26,13]
,[datetime(2024,5,6),None,None,None,None,None,None,0,3,4,6,-3]
,[datetime(2024,5,7),None,None,None,None,None,None,0,17,19,15,0]
,[datetime(2024,5,8),12,13,12,24,28,0,30,28,27,23,10]
,[datetime(2024,5,9),None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,5,10),21,21,21,18,17,17,13,11,10,6,15]
,[datetime(2024,5,11),1,2,0,6,17,17,28,29,31,26,15]
,[datetime(2024,5,12),1,2,2,2,4,3,10,13,14,21,14]
,[datetime(2024,5,13),1,1,0,3,3,3,12,16,23,20,7]
,[datetime(2024,5,14),19,19,19,18,26,0,30,34,32,31,15]
,[datetime(2024,5,15),17,19,17,14,13,13,9,7,6,0,27]
,[datetime(2024,5,16),0,35,34,31,30,30,26,24,23,19,13]
,[datetime(2024,5,17),1,3,3,2,4,4,7,9,10,10,5]
,[datetime(2024,5,18),10,10,10,13,16,15,18,18,16,11,15]
,[datetime(2024,5,19),1,2,0,10,11,9,11,20,19,70,74]
,[datetime(2024,5,20),11,13,13,14,19,18,21,22,23,23,13]
,[datetime(2024,5,21),3,13,11,12,14,14,17,16,14,13,11]
,[datetime(2024,5,22),14,14,14,21,20,19,23,23,25,34,32]
,[datetime(2024,5,23),None,None,None,None,None,None,0,0,0,0,5]
,[datetime(2024,5,24),2,3,3,5,8,8,10,10,7,3,19]
,[datetime(2024,5,25),4,4,4,11,12,10,16,19,19,23,13]
,[datetime(2024,5,26),1,7,7,9,10,10,9,11,13,14,7]
,[datetime(2024,5,27),2,5,5,8,7,6,13,14,17,18,10]
,[datetime(2024,5,28),6,6,6,10,10,10,12,21,36,52,41]
,[datetime(2024,5,29),None,None,None,None,None,None,0,30,31,30,14]
,[datetime(2024,5,30),3,3,3,6,6,4,6,7,7,9,1]
,[datetime(2024,5,31),1,12,12,32,33,31,37,38,37,33,15]
,[datetime(2024,6,1),6,9,9,9,14,13,13,13,13,0,-9]
,[datetime(2024,6,2),7,9,7,18,18,17,21,22,22,35,13]
,[datetime(2024,6,3),20,21,20,28,28,26,35,42,47,52,44]
,[datetime(2024,6,4),6,9,9,6,6,5,9,0,0,0,10]
,[datetime(2024,6,5),1,2,2,4,6,0,0,0,0,12,15]
,[datetime(2024,6,6),0,4,4,11,12,10,13,12,15,13,6]
,[datetime(2024,6,7),21,24,24,29,31,30,36,42,46,45,30]
,[datetime(2024,6,8),2,3,2,3,4,0,11,18,17,13,14]
,[datetime(2024,6,9),0,0,0,22,28,28,32,36,39,36,20]
,[datetime(2024,6,10),5,15,13,19,19,17,18,18,16,13,5]
,[datetime(2024,6,11),22,29,27,28,29,27,41,48,47,75,67]
,[datetime(2024,6,12),3,4,3,21,20,19,23,23,22,24,15]
,[datetime(2024,6,13),2,4,2,8,9,7,21,22,25,21,4]
,[datetime(2024,6,14),25,27,25,27,25,25,25,22,20,19,9]
,[datetime(2024,6,15),0,2,0,23,22,22,25,22,19,14,15]
,[datetime(2024,6,16),1,2,0,2,3,3,8,11,13,15,4]
,[datetime(2024,6,17),0,8,7,10,10,8,12,14,17,21,14]
,[datetime(2024,6,18),25,26,25,31,30,29,31,29,28,24,15]
,[datetime(2024,6,19),0,9,8,11,11,11,17,24,27,23,9]
,[datetime(2024,6,20),45,47,45,54,54,52,59,71,73,75,65]
,[datetime(2024,6,21),20,20,19,25,23,23,22,20,18,15,3]
,[datetime(2024,6,22),0,2,0,3,3,3,6,8,9,10,11]
,[datetime(2024,6,23),3,3,3,4,5,4,8,12,15,17,5]
,[datetime(2024,6,24),0,1,0,2,2,3,3,6,8,7,-2]
,[datetime(2024,6,25),0,2,0,0,3,3,4,9,9,5,5]
,[datetime(2024,6,26),5,39,39,75,74,74,70,68,67,24,15]
,[datetime(2024,6,27),10,11,10,11,10,9,11,9,7,6,10]
,[datetime(2024,6,28),18,30,30,42,45,45,51,52,59,60,52]
,[datetime(2024,6,29),20,17,17,19,20,20,22,22,22,21,8]
,[datetime(2024,6,30),13,10,10,11,11,9,35,39,41,41,30]
,[datetime(2024,7,1),1,1,0,12,14,14,17,22,24,20,10]
,[datetime(2024,7,2),1,5,3,9,10,10,16,17,17,14,0]
,[datetime(2024,7,3),22,26,26,33,32,32,28,26,25,21,8]
,[datetime(2024,7,4),5,6,5,15,16,14,21,20,19,15,11]
,[datetime(2024,7,5),2,6,5,6,6,4,6,8,9,10,2]
,[datetime(2024,7,6),15,20,18,24,23,0,25,30,28,25,14]
,[datetime(2024,7,7),7,16,15,16,17,15,20,22,21,17,7]
,[datetime(2024,7,8),2,6,5,12,13,0,16,19,18,19,13]
,[datetime(2024,7,9),6,9,9,11,14,14,17,18,18,19,10]
,[datetime(2024,7,10),7,10,10,21,19,20,19,19,18,18,4]
,[datetime(2024,7,11),11,13,11,12,10,10,10,20,24,25,11]
,[datetime(2024,7,12),0,5,3,13,15,16,18,16,16,0,15]
,[datetime(2024,7,13),10,13,13,15,15,13,17,17,22,25,11]
,[datetime(2024,7,14),0,15,15,19,18,18,22,21,19,25,14]
,[datetime(2024,7,15),10,13,13,20,20,19,21,21,19,17,8]
,[datetime(2024,7,16),1,1,0,3,2,0,10,12,14,15,6]
,[datetime(2024,7,17),0,9,7,16,19,19,21,24,24,24,15]
,[datetime(2024,7,18),11,12,11,14,15,13,16,18,17,36,30]
,[datetime(2024,7,19),20,22,20,25,26,24,45,49,52,54,45]
,[datetime(2024,7,20),0,2,0,4,5,0,10,12,13,11,14]
,[datetime(2024,7,21),6,9,9,7,10,10,14,16,19,18,12]
,[datetime(2024,7,22),0,13,11,14,13,12,20,22,24,25,14]
,[datetime(2024,7,23),19,22,22,21,20,18,31,34,39,44,37]
,[datetime(2024,7,24),None,None,None,None,None,None,11,18,19,20,11]
,[datetime(2024,7,25),1,2,0,4,4,4,6,12,10,5,2]
,[datetime(2024,7,26),18,22,22,29,30,28,31,33,32,28,14]
,[datetime(2024,7,27),17,20,20,23,23,21,36,55,59,60,49]
,[datetime(2024,7,28),1,5,4,8,11,10,13,15,17,16,11]
,[datetime(2024,7,29),20,22,22,24,26,25,26,25,26,25,27]
,[datetime(2024,7,30),None,None,None,None,None,None,None,None,None,None,None]
,[datetime(2024,7,31),30,32,32,33,34,32,36,37,34,29,14]
,[datetime(2024,8,1),19,21,19,18,18,18,19,19,18,19,11]
,[datetime(2024,8,2),19,21,19,29,31,31,33,36,41,41,31]
,[datetime(2024,8,3),19,22,22,24,25,25,26,37,43,51,42]
,[datetime(2024,8,4),1,3,3,18,17,17,22,22,24,30,23]
,[datetime(2024,8,5),0,2,0,3,4,3,6,8,9,9,-2]
,[datetime(2024,8,6),0,5,4,0,12,11,14,17,20,22,11]
,[datetime(2024,8,7),6,9,9,10,10,9,12,12,13,12,7]
,[datetime(2024,8,8),33,36,36,36,37,35,39,41,43,47,43]
,[datetime(2024,8,9),1,6,4,8,8,7,19,23,24,24,20]
,[datetime(2024,8,10),0,4,4,10,10,9,14,14,16,19,9]
,[datetime(2024,8,11),1,4,4,3,8,8,10,12,13,10,-4]
,[datetime(2024,8,12),1,9,7,10,17,16,20,25,28,29,15]
,[datetime(2024,8,13),5,7,5,9,12,12,15,21,20,24,16]
,[datetime(2024,8,14),1,4,4,9,9,7,11,16,16,19,17]
,[datetime(2024,8,15),10,14,14,17,19,19,21,23,23,29,17]
,[datetime(2024,8,16),0,4,4,7,8,6,11,14,16,18,13]
,[datetime(2024,8,17),6,10,10,14,15,13,16,18,20,17,4]
,[datetime(2024,8,18),0,2,0,4,6,5,7,8,11,13,1]
,[datetime(2024,8,19),10,12,10,21,22,23,24,33,33,35,30]
,[datetime(2024,8,20),0,0,0,16,17,15,22,25,27,26,26]
,[datetime(2024,8,21),1,4,4,9,8,7,10,13,15,17,5]
,[datetime(2024,8,22),0,3,3,7,19,19,8,6,13,15,3]
,[datetime(2024,8,23),1,10,9,15,18,17,24,30,33,35,25]
,[datetime(2024,8,24),1,4,4,8,15,15,17,14,17,13,8]
,[datetime(2024,8,25),1,2,0,5,5,4,7,7,12,15,9]
,[datetime(2024,8,26),0,0,0,18,17,17,21,26,27,34,23]
,[datetime(2024,8,27),1,4,4,8,7,7,12,13,9,11,1]
,[datetime(2024,8,28),4,7,7,12,17,16,20,23,27,23,20]
,[datetime(2024,8,29),1,3,3,8,7,5,0,6,6,23,13]
,[datetime(2024,8,30),1,1,0,4,6,0,9,7,11,11,1]
,[datetime(2024,8,31),0,1,0,6,10,9,12,13,14,16,7]
,[datetime(2024,9,1),0,1,0,5,5,3,7,8,12,15,1]
,[datetime(2024,9,2),None,None,None,None,None,None,8,13,15,20,8]
,[datetime(2024,9,3),1,3,3,13,15,15,18,21,25,25,16]
,[datetime(2024,9,4),0,8,6,10,10,8,14,16,18,18,15]
,[datetime(2024,9,5),9,11,9,15,16,16,19,20,20,22,28]
,[datetime(2024,9,6),1,2,0,33,34,32,38,41,44,44,34]
,[datetime(2024,9,7),1,0,0,8,8,6,14,17,20,25,16]
,[datetime(2024,9,8),1,1,0,4,5,4,13,18,20,30,22]
,[datetime(2024,9,9),0,1,0,4,3,3,12,20,23,35,28]
,[datetime(2024,9,10),0,1,0,9,10,10,12,13,16,19,9]
,[datetime(2024,9,11),1,2,0,8,11,10,25,29,32,32,28]
,[datetime(2024,9,12),7,8,7,9,11,11,12,10,31,35,26]
,[datetime(2024,9,13),1,3,3,10,11,11,16,21,23,21,17]
,[datetime(2024,9,14),2,2,0,3,4,3,12,12,16,18,7]
,[datetime(2024,9,15),1,3,3,15,22,21,22,23,25,26,17]
,[datetime(2024,9,16),5,7,5,9,10,8,22,23,20,25,14]
,[datetime(2024,9,17),0,1,0,3,3,3,6,32,45,47,40]
,[datetime(2024,9,18),0,3,3,4,4,3,0,27,30,31,25]
,[datetime(2024,9,19),1,2,0,5,5,4,7,10,13,16,8]
,[datetime(2024,9,20),1,0,0,0,5,4,6,7,10,9,4]
,[datetime(2024,9,21),11,11,11,8,12,11,15,13,20,21,11]
,[datetime(2024,9,22),0,0,0,6,8,8,9,10,13,15,6]
,[datetime(2024,9,23),0,0,0,1,3,3,3,8,10,15,5]
,[datetime(2024,9,24),0,None,0,5,4,4,9,11,11,15,9]
,[datetime(2024,9,25),22,None,21,20,22,24,29,31,35,37,34]
,[datetime(2024,9,26),4,None,3,4,4,4,7,7,8,12,9]
,[datetime(2024,9,27),1,None,0,8,10,10,10,11,15,15,6]
,[datetime(2024,9,28),0,None,0,0,1,0,0,4,11,12,2]
,[datetime(2024,9,29),None,None,None,None,None,None,0,5,7,9,3]
,[datetime(2024,9,30),1,None,0,1,1,0,4,6,7,5,-1]
,[datetime(2024,10,1),3,None,0,3,4,3,5,7,10,10,4]
,[datetime(2024,10,2),0,None,0,2,4,3,16,28,31,30,26]
,[datetime(2024,10,3),0,None,0,2,3,0,7,9,14,17,12]
,[datetime(2024,10,4),0,None,0,2,4,4,8,11,12,20,10]
,[datetime(2024,10,5),0,None,0,1,1,0,6,7,10,8,0]
,[datetime(2024,10,6),0,None,0,7,9,9,12,10,9,5,3]
,[datetime(2024,10,7),5,None,4,5,10,10,14,16,20,22,14]
,[datetime(2024,10,8),10,None,9,22,23,21,27,28,31,32,25]
,[datetime(2024,10,9),1,None,0,1,2,0,36,61,65,69,65]
,[datetime(2024,10,10),17,None,16,19,18,17,21,22,25,27,15]
,[datetime(2024,10,11),1,None,9,16,16,17,19,23,24,22,14]
,[datetime(2024,10,12),2,None,0,2,5,4,7,10,15,18,4]
,[datetime(2024,10,13),0,None,0,0,2,0,11,17,21,23,14]
,[datetime(2024,10,14),17,None,16,18,18,16,24,25,28,30,19]
,[datetime(2024,10,15),0,None,0,9,9,8,25,26,25,21,8]
,[datetime(2024,10,16),0,None,0,14,13,13,16,19,23,24,17]
,[datetime(2024,10,17),1,None,0,3,4,4,11,13,15,17,14]
,[datetime(2024,10,18),0,None,0,2,3,3,8,10,19,20,14]
,[datetime(2024,10,19),1,None,0,2,5,4,9,11,15,17,10]
,[datetime(2024,10,20),1,None,0,1,2,0,9,13,14,16,8]
,[datetime(2024,10,21),0,None,0,3,3,3,5,7,9,10,1]
,[datetime(2024,10,22),0,None,0,11,11,9,13,15,18,18,13]
,[datetime(2024,10,23),1,None,0,5,8,7,12,12,15,17,14]
,[datetime(2024,10,24),0,None,0,1,1,0,2,3,5,7,0]
,[datetime(2024,10,25),0,None,0,3,2,0,7,9,9,12,14]
,[datetime(2024,10,26),0,None,0,4,4,4,5,8,10,9,1]
,[datetime(2024,10,27),1,None,0,0,1,0,5,10,12,20,12]
,[datetime(2024,10,28),2,None,0,0,0,0,4,6,9,12,14]
,[datetime(2024,10,29),4,None,3,4,6,6,7,8,8,10,3]
,[datetime(2024,10,30),0,None,0,1,1,0,4,5,7,9,3]
,[datetime(2024,10,31),0,None,0,1,0,0,1,3,5,8,10]
,[datetime(2024,11,1),0,None,0,3,3,3,0,0,0,0,-15]
,[datetime(2024,11,2),5,None,4,8,8,6,12,15,20,16,3]
,[datetime(2024,11,3),2,None,0,0,1,0,3,10,17,20,12]
,[datetime(2024,11,4),1,None,0,1,3,3,8,9,12,15,-1]
,[datetime(2024,11,5),1,None,0,0,3,3,8,11,15,16,3]
,[datetime(2024,11,6),4,None,3,9,7,8,11,11,9,21,11]
,[datetime(2024,11,7),1,None,0,3,5,5,8,10,13,13,2]
,[datetime(2024,11,8),0,None,0,5,7,7,7,10,17,20,14]
,[datetime(2024,11,9),37,None,36,49,48,48,50,53,57,58,49]
,[datetime(2024,11,10),1,None,0,9,15,14,19,20,23,26,15]
,[datetime(2024,11,11),0,None,0,9,10,8,17,18,18,19,14]
,[datetime(2024,11,12),1,None,0,6,6,4,11,12,15,17,6]
,[datetime(2024,11,13),0,None,0,0,0,0,4,4,5,7,2]
,[datetime(2024,11,14),0,None,0,2,1,0,10,13,16,17,5]
,[datetime(2024,11,15),0,None,0,1,3,3,7,10,14,16,6]
,[datetime(2024,11,16),2,None,0,5,5,4,6,11,8,4,4]
,[datetime(2024,11,17),4,None,3,4,4,3,7,10,16,18,12]
,[datetime(2024,11,18),1,None,0,2,3,3,9,12,17,15,0]
,[datetime(2024,11,19),0,None,0,2,1,0,0,6,20,8,11]
,[datetime(2024,11,20),0,None,0,0,1,0,4,11,14,17,12]
,[datetime(2024,11,21),25,None,24,41,41,39,48,51,52,54,43]
,[datetime(2024,11,22),1,None,0,4,3,3,11,11,14,16,0]
,[datetime(2024,11,23),0,None,0,0,0,0,6,8,10,14,2]
,[datetime(2024,11,24),0,None,0,4,4,3,10,11,13,17,10]
,[datetime(2024,11,25),5,None,4,17,16,15,29,27,26,22,3]
,[datetime(2024,11,26),0,None,0,2,2,0,10,11,14,16,5]
,[datetime(2024,11,27),7,None,6,10,11,11,19,21,22,18,15]
,[datetime(2024,11,28),4,None,3,13,13,11,16,18,20,20,10]
,[datetime(2024,11,29),1,None,0,6,7,7,11,12,15,16,14]
,[datetime(2024,11,30),1,None,0,4,4,4,13,12,13,15,5]
,[datetime(2024,12,1),0,None,0,3,4,3,8,10,13,13,12]
,[datetime(2024,12,2),0,None,0,8,8,7,13,15,14,21,10]
,[datetime(2024,12,3),0,None,0,5,5,3,0,12,16,19,16]
,[datetime(2024,12,4),8,None,7,10,11,9,14,18,17,21,10]
,[datetime(2024,12,5),10,None,9,12,13,11,14,15,17,18,11]
,[datetime(2024,12,6),0,None,0,3,5,5,7,9,12,15,4]
,[datetime(2024,12,7),5,None,4,9,11,0,16,33,32,28,6]
,[datetime(2024,12,8),0,None,0,3,3,3,5,6,8,11,5]
,[datetime(2024,12,9),0,None,0,4,6,6,13,15,21,44,34]
,[datetime(2024,12,10),6,None,5,11,12,10,16,17,19,23,15]
,[datetime(2024,12,11),19,None,18,29,30,30,36,44,47,49,38]
,[datetime(2024,12,12),16,None,15,21,22,0,32,34,36,50,39]
,[datetime(2024,12,13),1,None,0,6,11,11,15,19,21,23,14]
,[datetime(2024,12,14),0,None,0,5,9,9,16,17,19,21,11]
,[datetime(2024,12,15),0,None,0,12,13,13,19,21,23,25,15]
,[datetime(2024,12,16),2,None,0,5,5,5,13,14,13,9,2]
,[datetime(2024,12,17),0,None,0,5,7,7,19,25,25,21,7]
,[datetime(2024,12,18),5,None,4,8,8,6,9,12,16,18,8]
,[datetime(2024,12,19),1,None,0,3,3,3,4,7,8,4,0]
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
        "start_date": "2023-12-21",
        "end_date": "2024-12-19",
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
df.to_csv("merged_train_weather_data.csv", index=False)
print("Data has been formatted and saved to 'merged_train_weather_data.csv'.")
