- Let’s assume that the dataset updates everyday at 7:00 AM (It seems like the update frequency is sporadic)
    - After monitoring the API for past week I can confirm that the updates are not constant, there is not fixed schedule
    - Therefore the best option would be to run the pipeline everyday once a day

What am I going to do with this data?
- I want to create a web app to display this data
    - Which airlines have the most grievances in the last
    - Which airport registers the least number of grievances

How am I going to model the data?
- The raw dataset has 26 columns, from which only 4 of them are dimensions rest are metrics
- As such there is no need to model the data as it can be used directly for reports
- However there are certain things that will be done while loading the data itself
    - Standardising column naming convention
    - Partitioning the data based on date

Which tools am I going to use?
- Python and dlt for ingestion
- BigQuery as a data warehouse (duckdb for testing) (https://docs.streamlit.io/develop/tutorials/databases/bigquery)
- Github for code repo
- Github action to trigger the workflow


