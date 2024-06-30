import pandas as pd
from store_to_db import store_data_to_mongodb
from sales_data_pipeline import etl_flow

def main():
    # Call the function to store data into MongoDB
    store_data_to_mongodb('data_files_config.json')
    # Prompt the user for input
    comparison_type = input("Enter the comparison type ('daily', 'weekly', or 'monthly'): ")
    start_date_1 = pd.to_datetime(input("Enter the first start date (e.g., '2019-04-01'): "))
    start_date_2 = pd.to_datetime(input("Enter the second start date (e.g., '2019-04-08'): "))

    # Call the ETL process function
    etl_flow(start_date_1, start_date_2, comparison_type)

if __name__ == "__main__":
    main()
