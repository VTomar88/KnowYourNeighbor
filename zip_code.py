import requests
import pandas as pd
from api_key import *

def fetch_census_data(year, variables, zip_code):
    # Create your own api_key.py module and store your API key from US census 
    api_key = my_key
    # US Census url to fetch the data
    url = f"https://api.census.gov/data/{year}/acs/acs5/profile?get={variables}&for=zip%20code%20tabulation%20area:{zip_code}&key={api_key}"
    # get the data from url
    response = requests.get(url)
    # if the url works good, return the data
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

def main():
    # Searched from census_variables.csv, and can be extended to different codes
    variables_dict = {'DP05_0044E': 'One race | Asian', 'DP05_0045E': 'One race | Asian | Asian Indian', 'DP05_0046E': 'One race | Asian | Chinease'}
    variables = ','.join(list(variables_dict.keys()))
    year = '2022'
    zip_code = '20871'  # Replace with your desired ZIP Code
    data = fetch_census_data(year, variables, zip_code)
    print(data)

if __name__ == "__main__":
    main()

