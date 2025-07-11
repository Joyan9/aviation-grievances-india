## July 8th, 2025
- Created a basic dlt pipeline for the Aviation Grievances API endpoint

### Issues faced
- loaded data column names are not consistent

### Next Steps
- Try out dlt' in built capability to transform data before loading
    - add a date column
    - create a column mapping 

## July 9th, 2025
- fixed the issues encountered yesterday
    - column name case standardising
    - adding updated at and inserted at columns


### Issues faced
- In the API response, the updated_date was part of the meta data and accessing it via the rest_api source was quite difficult therefore switched to creating a dlt resource and yielding each record along with the api metadata

### Next Steps
- Need to check how to avoid deduplication


## July 10th, 2025
- Nothing major accomplished

### Issues faced
- Getting an error on partitioning

### Next Steps
- Need to read dlt docs on partitioning and try out different approaches

## July 11th, 2025
- Figured out how to partition with dlt
- Created the streamlit based dashboard but purely using claude

### Issues faced
- Nothing major

### Next Steps
- I'm planning to create a materialised view with like filters and column naming because the current dashboard numbers do not make sense, so basically need to include a sanity check on the data
- Check how to deploy dlt via CRON, do I need to dockerise?