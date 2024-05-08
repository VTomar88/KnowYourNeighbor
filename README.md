# KnowYourNeighbor
I am performing the data analysis based on US Census Bureau API.

## Goal 
I am interested in knowing popuation of races with zip code. 

## Steps to achieve the goal
These are the steps I preformed to achieve the goal

### Step 1. Get info from  US census
I created a function 'fetch_census_data' in 'zip_code.py'. This function requires year, races, and zip code.
This function is enough to proivde the required info but need more work to obtain information on multiple zip codes.

### Step 2. What if we need more information on different race, enthinicity, age, gender etc. ?
I limited my search for just three races. I needed a code for each race to make the step 1 work. 
So, I decided to web-scrap US census page to compile all the variable codes with their explanation in csv format. 
The web scrapping script is 'census_variables_scrap.py' and the results are stored in 'census_variables.csv'. There are 
a total of 92 variables, from which I used only three variables for this project.

### Step 3. Get the zip codes and their respective city and state.
Next was to collect zip codes. I created script 'get_zip_codes.py' and stored the results in 'city-state_zip-codes.csv'.

### Step 4. Final analysis
Now we have all ingredients to perform the final analysis. Please find the analysis and results in 'main_data_analysis.ipynb'.

## Conclusion
The current work can be extended to any varaible and can be helpful to build an app like "KnowYourNeighborhood". Any interest please
feel free to connect.

