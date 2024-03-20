# Team 24 - Project A: Chicago Crimes Big Data Analysis

## Team members by task

- **Task 1: Omar Nosseir**
  - SID: 862396706
  - NetID: onoss001
  - Email: onoss001@ucr.edu 
  - Task description:
    - Prepare the original dataset using Beast and Spark SQL
    - Ensure columns are in the correct format for column-based (Parquet) transformation
    - Introduce a ZIPCode column representing the location of each crime
    - Return a compressed .parquet file to be used in following tasks


- **Task 2: Afraaz Mohammed:** amoha120
  - SID: 862393696
  - NetID: amoha120
  - Email: amoha120@ucr.edu
  - Task description:
    - Run group aggregate queries to computes the total number of crimes per ZIP Code
    - Load ZIP Code geometries and merge it with the previous query output
    - Generate a Shapefile for the query described above.
    - Import the generated shapefile into QGIS and plot a choropleth map

- **Task 3: Jeralson Paredes:**
  - SID: 862381979
  - NetID: jpare033
  - Email: jpare033@ucr.edu
  - Task description:
    - Count the number of crimes for each crime type, given start and end dates
    - Load the dataset created from task 1 (i.e. 10k) as an argument to aggregate the number of crimes for each type
    - Generate a CSV file that contains the results (i.e. number of crimes for each crime type)
    - Load the CSV file into a spreadsheet program to create desired bar chart

- **Task 4: James Glassford:**
  - SID: 862379637
  - NetID: aglas012
  - Email: aglas012@ucr.edu
  - Task description: 
    - Utilize a predictive model to try and determine whether or not future calls will result in an arrest
    - Prune the data to remove any arrest data that is inconclusive (not true or false)
    - Feed the data to the predictive model and make the prediction based on sentiment analysis
      
