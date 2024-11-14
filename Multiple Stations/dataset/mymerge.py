import pandas as pd
import glob

# Specify the directory where your files are stored
file_directory = '/Users/dishanachar/Documents/scrap/dataset/'

# Get a list of all CSV files in the directory
file_paths = glob.glob(file_directory + "*.csv")

# Load and merge all datasets row-wise
merged_df_rowwise = pd.concat([pd.read_csv(file) for file in file_paths], axis=0).reset_index(drop=True)

# Identify and rename only the columns with station codes (e.g., containing "_Delay" in their name)
station_columns = [col for col in merged_df_rowwise.columns if '_Delay' in col]

# Create a dictionary to rename these columns sequentially as S1, S2, etc.
station_column_rename = {station: f"S{i+1}" for i, station in enumerate(station_columns)}
merged_df_rowwise.rename(columns=station_column_rename, inplace=True)

# Export the merged DataFrame to a new CSV file
output_file_path = '/Users/dishanachar/Documents/scrap/dataset/merged_data.csv'
merged_df_rowwise.to_csv(output_file_path, index=False)

print(f"Data successfully exported to {output_file_path}")