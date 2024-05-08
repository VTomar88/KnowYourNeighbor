import pandas as pd
from uszipcode import SearchEngine


def get_zip_codes(city, state):
    """Fetch ZIP codes for a given city and state."""
    search = SearchEngine()
    # get zip codes with city and state
    res = search.by_city_and_state(city, state)
    # return all zip code, major state, and state
    return [item.zipcode for item in res], res[0].major_city, res[0].state_abbr


def main():
    # Get the cities for which zip codes are needed
    cities_MD = ['Clarksburg', 'Boyds', 'Rockville', 'Germantown', 'Gaitherburg', 'Frederick']
    cities_VA = ['Ashburn', 'Belmont', 'Sterling']

    city_state_zip_dict = {}
    # Get all the zip codes for cities
    for i in cities_MD:
        city_state_zip_dict[f"{get_zip_codes(i, 'MD')[1]}-{get_zip_codes(i, 'MD')[2]}"] = get_zip_codes(i, 'MD')[0]
    for i in cities_VA:
        city_state_zip_dict[f"{get_zip_codes(i, 'VA')[1]}-{get_zip_codes(i, 'VA')[2]}"] = get_zip_codes(i, 'VA')[0]
    
    # write a dataframe
    df = pd.DataFrame(list(city_state_zip_dict.items()), columns=["City-State", "Zip_Code"])

    return df.to_csv('city-state_zip-codes.csv', index=False)

if __name__ == "__main__":
    main()



