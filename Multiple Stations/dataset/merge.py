import pandas as pd
import glob

# Paths to your uploaded sample files
file_paths = ['train_12001_weather_data.csv', 'train_12677_weather_data.csv','train_12678_weather_data.csv','train_12702_weather_data.csv','train_12740_weather_data.csv','train_12841_weather_data.csv','train_12953_weather_data.csv','train_22453_weather_data.csv']

# Load all CSV files into a list of DataFrames, dropping the first row from each
dataframes = [pd.read_csv(file, skiprows=1) for file in file_paths]

# Drop the first column (date) from each DataFrame
dataframes = [df.drop(df.columns[0], axis=1) for df in dataframes]

# Separate each DataFrame into 70% and 30% portions
train_parts = [df.iloc[:int(0.7 * len(df)), :] for df in dataframes]
test_parts = [df.iloc[int(0.7 * len(df)):, :] for df in dataframes]

# Concatenate the 70% parts and the 30% parts, respectively
train_data = pd.concat(train_parts, ignore_index=True)
test_data = pd.concat(test_parts, ignore_index=True)

# Combine the 70% and 30% parts together
final_data = pd.concat([train_data, test_data], ignore_index=True)

# Save the final DataFrame to a new CSV file
final_data.to_csv('combined_weather_data.csv', index=False)

print("Final data shape:", final_data.shape)
print("File saved as 'combined_weather_data.csv'")