# Aircraft Data Exploration
This directory contains a Jupyter Notebook that performs exploratory data analysis on an aircraft dataset. The notebook walks through inspecting, data imputation, transforming, and visualizing the data, structured into five main tasks.

## Files

- hw1.ipynb – Jupyter Notebook containing all analysis and plots
- README.md – Project overview and instructions
- hw1_answers – Project narrative and answers to questions listed in the project description


## Requirements

To run the notebook, install Python and the following packages:

```bash
pip install pandas numpy matplotlib seaborn scipy miceforest plotnine

```

## Task 1 Data Inspection and Imputation
- Loaded the dataset considering NAs not as empty strings
- Displayed column names, data types, and basic dataset info
- Imputed data in columns we are interested in (CARRIER, CARRIER_NAME, MANUFACTURE_YEAR, NUMBER_OF_SEATS, CAPACITY_IN_POUNDS, and AIRLINE_ID)
- Choices of imputation are listed in hw1_answers.pdf
- Outputs: printed summary tables of null values of variables we are interested to impute 

## Task 2 Data Standarization and Transformation
- Checked unique values across columns interested in (MANUFACTURER, MODEL, AIRCRAFT_STATUS, and OPERATING_STATUS) and inspected potential errors
- Standarization and Transformation choices are described in hw1_answers.pdf
- Outputs: summary tables of unique values across columns interested 

## Task 3 Remaining Data
- Outputs: printed remaining and original number of rows of data before and after cleaning

## Task 4 Transformation and derivative variables
- Checked the skewness of NUMBER_OF_SEATS and CAPACITY_IN_POUNDS
- Plotted histograms for each
- Applied Box-Cox transformation to normalize NUMBER_OF_SEATS and CAPACITY_IN_POUNDS
- Added two new columns: NUMBER_OF_SEATS_BOXCOX, CAPACITY_IN_POUNDS_BOXCOX
- Plotted histograms after transformation
- Outputs: Skewness values printed, 2 histograms displaying original distributions, and 2 histograms displaying the transformed distributions

## Task 5 Feature engineering
- Created a new categorical column SIZE based on quartiles of NUMBER_OF_SEATS
- Grouped by SIZE and analyzed: Proportions of aircrafts by OPERATING_STATUS, proportions by AIRCRAFT_STATUS
- Plotted side-by-side bar plots for both groupings
- Output: 2 bar plots comparing distributions across size categories



