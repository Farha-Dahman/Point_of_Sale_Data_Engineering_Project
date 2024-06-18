from store_to_db import store_data_to_mongodb

def main():
    # Call the function to store data into MongoDB
    store_data_to_mongodb('data_files_config.json')

if __name__ == "__main__":
    main()
