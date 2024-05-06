from bs4 import BeautifulSoup
import requests
import pandas as pd

def scrap_variables():
    # census variable url
    census_variable_url = f'https://api.census.gov/data/2022/acs/acs5/profile/variables.html' 

    # HTTP GET requests
    page = requests.get(census_variable_url)

    # Checking if we successfully fetched the url

    if page.status_code == requests.codes.ok:
        # print("All is good!")
        bs = BeautifulSoup(page.text, 'lxml')
        # print(bs)
        # Find the <tbody> element
        tbody = bs.find('tbody')
        variables_dict = {}
        if tbody:
            # Iterate over each <tr> in the found <tbody>
            for tr in tbody.find_all('tr'):
                # For each <tr>, find all <td> elements
                tds = tr.find_all('td')
                # Extract text or other data from each <td>
                row = [td.text.strip() for td in tds]
                variables_dict[row[0]] = row[1]
        else:
            print("No <tbody> element found.")

    df = pd.DataFrame(list(variables_dict.items()), columns=["Name", "Label"])
    # Replace punctuation in the 'Label' column
    df['Label'] = df['Label'].str.replace('!!', ' | ')

    # Filter the dataframe for profile DP05
    filtered_df = df[df['Name'].str.startswith('DP05')]

    # Filter for label with 'Estimate'
    filtered_df = filtered_df[filtered_df['Label'].apply(lambda x: 'estimate' in str(x).lower())]

    # Re-index the filtered DataFrame
    filtered_df.reset_index(drop=True, inplace=True)

    write_data = filtered_df.to_csv('census_variables.csv', index=False)

    return write_data


def main():
    scrap_variables()

if __name__ == '__main__':
    main()
