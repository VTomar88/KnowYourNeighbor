import requests
import csv
from api_key import *

def fetch_census_data(year, zip_code):
    api_key = my_key
    # List all the variables you're interested in
    # variables = ','.join(['B02001_001E', 'B02001_002E', 'B02001_003E', 'B02001_004E', 'B02001_005E', 'B02001_006E', 'B02001_007E', 'B02001_008E'])
    variables = ','.join(['DP05_0045E', 'DP05_0046E'])
    url = f"https://api.census.gov/data/{year}/acs/acs5/profile?get={variables}&for=zip%20code%20tabulation%20area:{zip_code}&key={api_key}"

    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

def save_to_csv(data, filename):
    if data:
        headers = data[0]  # First row is headers
        rows = data[1:]  # Remaining rows are data

        with open(filename, 'w', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(headers)  # Write the header
            csv_writer.writerows(rows)  # Write the data rows

def main():
    # Usage example
    year = '2022'
    zip_code = '20871'  # Replace with your desired ZIP Code
    data = fetch_census_data(year, zip_code)
    print(data)
    # if data:
    #     save_to_csv(data, 'race_data.csv')
    #     print("Data saved to 'race_data.csv'")
    # else:
    #     print("Failed to fetch or save data.")

if __name__ == "__main__":
    main()

