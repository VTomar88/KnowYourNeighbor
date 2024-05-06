import requests
import pandas as pd
from api_key import *

def fetch_census_data(year, variables, zip_code):
    api_key = my_key
    
    variables = ','.join(['DP05_0045E', 'DP05_0046E'])
    url = f"https://api.census.gov/data/{year}/acs/acs5/profile?get={variables}&for=zip%20code%20tabulation%20area:{zip_code}&key={api_key}"

    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

def main():
    # Usage example
    # Searched from census_variables.csv. It can be extended as per the use
    variables_dict = {'DP05_0045E': 'One race | Asian | Asian Indian', 'DP05_0046E': 'One race | Asian | Chinease'}
    variables = list(variables_dict.keys())
    year = '2022'
    zip_code = '20871'  # Replace with your desired ZIP Code
    data = fetch_census_data(year, variables, zip_code)
    print(data)

if __name__ == "__main__":
    main()

