import os
from typing import List
import pandas as pd
from pymongo import MongoClient
from fastapi import HTTPException
from DB.connect_db import get_mongo_connection

# MongoDB connection setup
client, db, sales_collection = get_mongo_connection()

# Total sales for each store on a daily basis
def calculate_line_item_amount(df, column_name):
    # Assuming there is a function that calculates line item amounts
    return df.groupby([column_name, 'sales_outlet_id']).agg({'line_item_amount': 'sum'}).reset_index()

def daily_sales_by_store(df):
    column_name = 'transaction_date'
    daily_sales = calculate_line_item_amount(df, column_name)
    daily_sales.columns = [column_name, 'sales_outlet_id', 'daily_sales']
    return daily_sales

def get_daily_sales(store_id: int):
    try:
        # Fetch data from MongoDB
        data = list(sales_collection.find({"sales_outlet_id": store_id}))
        df = pd.DataFrame(data)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="Store not found")

        daily_sales = daily_sales_by_store(df)
        outlet_data = daily_sales[daily_sales['sales_outlet_id'] == store_id]
        result = outlet_data.to_dict(orient="records")
        return {"store_id": store_id, "daily_sales": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Total sales for each store on a weekly basis
def weekly_sales_by_store(df):
    column_name = 'transaction_date'
    df[column_name] = pd.to_datetime(df[column_name])
    df['week'] = df[column_name].dt.isocalendar().week
    df['year'] = df[column_name].dt.isocalendar().year
    weekly_sales = df.groupby(['year', 'week', 'sales_outlet_id']).agg({'line_item_amount': 'sum'}).reset_index()
    weekly_sales.columns = ['year', 'week', 'sales_outlet_id', 'weekly_sales']
    return weekly_sales

def get_weekly_sales(store_id: int):
    try:
        # Fetch data from MongoDB
        data = list(sales_collection.find({"sales_outlet_id": store_id}))
        df = pd.DataFrame(data)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="Store not found")

        weekly_sales = weekly_sales_by_store(df)
        outlet_data = weekly_sales[weekly_sales['sales_outlet_id'] == store_id]
        result = outlet_data.to_dict(orient="records")
        return {"store_id": store_id, "weekly_sales": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Total sales for each store on a monthly basis
def monthly_sales_by_store(df):
    column_name = 'transaction_date'
    df[column_name] = pd.to_datetime(df[column_name])
    df['month'] = df[column_name].dt.month
    df['year'] = df[column_name].dt.year
    monthly_sales = df.groupby(['year', 'month', 'sales_outlet_id']).agg({'line_item_amount': 'sum'}).reset_index()
    monthly_sales.columns = ['year', 'month', 'sales_outlet_id', 'monthly_sales']
    return monthly_sales

def get_monthly_sales(store_id: int):
    try:
        # Fetch data from MongoDB
        data = list(sales_collection.find({"sales_outlet_id": store_id}))
        df = pd.DataFrame(data)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="Store not found")

        monthly_sales = monthly_sales_by_store(df)
        outlet_data = monthly_sales[monthly_sales['sales_outlet_id'] == store_id]
        result = outlet_data.to_dict(orient="records")
        return {"store_id": store_id, "monthly_sales": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Peak hours for each store
def plot_peak_hours_for_store(df, store_id):
    df['transaction_time'] = pd.to_datetime(df['transaction_time'])
    df['hour'] = df['transaction_time'].dt.hour
    column_name = 'hour'
    hourly_sales = calculate_line_item_amount(df[df['sales_outlet_id'] == store_id], column_name)

    # Find the peak hour for the store
    if hourly_sales.empty:
        return pd.DataFrame()
    peak_hour = hourly_sales.loc[hourly_sales['line_item_amount'].idxmax()]
    return peak_hour[['sales_outlet_id', 'hour', 'line_item_amount']]

def get_peak_hours_for_store(store_id: int):
    try:
        # Fetch data from MongoDB
        sales_data = list(sales_collection.find({"sales_outlet_id": store_id}))

        if not sales_data:
            raise HTTPException(status_code=404, detail="Sales data not found for the store")

        sales_df = pd.DataFrame(sales_data)
        if sales_df.empty:
            raise HTTPException(status_code=404, detail="Sales data not found for the store")

        # Calculate peak hours
        peak_hour = plot_peak_hours_for_store(sales_df, store_id)
        if peak_hour.empty:
            raise HTTPException(status_code=404, detail="No peak hour data found for the store")
        
        result = peak_hour.to_dict()
        return {"peak_hour": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# sales by customer type
def sales_by_customer_type(df):
    # Assuming 'customer_type' column in the dataframe with values 'guest' or 'loyal'
    sales_by_type = df.groupby(['Guest', 'sales_outlet_id']).agg({'line_item_amount': 'sum', 'transaction_id': 'count'}).reset_index()
    sales_by_type.columns = ['Guest', 'sales_outlet_id', 'total_sales', 'total_transactions']

    return sales_by_type

def get_sales_by_customer_type(store_id: int):
    try:
        # Fetch data from MongoDB
        data = list(sales_collection.find({"sales_outlet_id": store_id}))
        df = pd.DataFrame(data)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="Store not found")

        sales_data = sales_by_customer_type(df)
        outlet_data = sales_data[sales_data['sales_outlet_id'] == store_id]
        result = outlet_data.to_dict(orient="records")
        return {"store_id": store_id, "sales_by_customer_type": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Most Selling Item in each store
def most_selling_item_by_store(df):
    sales_count = df.groupby(['sales_outlet_id', 'product_id']).agg({'quantity': 'sum'}).reset_index()
    most_selling_items = sales_count.loc[sales_count.groupby('sales_outlet_id')['quantity'].idxmax()]
    most_selling_items.columns = ['sales_outlet_id', 'product_id', 'total_quantity']
    return most_selling_items

def get_most_selling_item(store_id: int):
    try:
        # Fetch data from MongoDB
        data = list(sales_collection.find({"sales_outlet_id": store_id}))
        df = pd.DataFrame(data)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="Store not found")

        most_selling_items = most_selling_item_by_store(df)
        outlet_data = most_selling_items[most_selling_items['sales_outlet_id'] == store_id]
        result = outlet_data.to_dict(orient="records")
        return {"store_id": store_id, "most_selling_item": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Comparison between actual sales and target sales for each sales outlet
def calculate_sales_differences(goal_data, actual_sales_data):
    goal = goal_data.copy()
    goal['total_goal'] = goal['beans_goal'] + goal['beverage_goal'] + goal['food_goal'] + goal['merchandise _goal']

    total_sales_by_outlet = actual_sales_data.groupby('sales_outlet_id')['line_item_amount'].sum().reset_index()

    comparison_df = pd.merge(goal, total_sales_by_outlet, on='sales_outlet_id')
    comparison_df.rename(columns={'line_item_amount': 'actual_sales'}, inplace=True)

    comparison_df['difference'] = comparison_df['actual_sales'] - comparison_df['total_goal']

    result_df = comparison_df[['sales_outlet_id', 'total_goal', 'actual_sales', 'difference']]
    return result_df

def get_sales_comparison():
    try:
        # Convert input data to DataFrames
        goal_df = pd.read_csv('data/sales targets.csv')
        actual_sales_df = pd.read_csv('data/201904 sales reciepts.csv')
        
        if goal_df.empty or actual_sales_df.empty:
            raise HTTPException(status_code=404, detail="Input data cannot be empty")

        # Calculate sales differences
        result_df = calculate_sales_differences(goal_df, actual_sales_df)
        result = result_df.to_dict(orient="records")
        return {"sales_differences": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
                            
# Number of customers who placed more than one order and only one order
def calculate_line_item_statistics(data):
    try:
        num_customers_line_item_id = []
        for i in range(1, 9):
            num_customers_line_item_id.append(int((data['line_item_id'] == i).sum()))

        line_item_counts = {f'line_item_id_{i}': count for i, count in enumerate(num_customers_line_item_id, start=1)}
        num_customers_1_line_item_id = int((data['line_item_id'] == 1).sum())
        num_customers_more_line_item_id = int((data['line_item_id'] > 1).sum())
        total_customers = int(data.shape[0])
        percentage_1_line_item_id = float((num_customers_1_line_item_id / total_customers) * 100)
        percentage_more_than_1_line_item_id = float((num_customers_more_line_item_id / total_customers) * 100)

        result = {
            "total_customers": total_customers,
            "num_customers_1_line_item_id": num_customers_1_line_item_id,
            "num_customers_more_line_item_id": num_customers_more_line_item_id,
            "percentage_1_line_item_id": percentage_1_line_item_id,
            "percentage_more_than_1_line_item_id": percentage_more_than_1_line_item_id
        }
        result.update(line_item_counts)
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
def get_line_item_statistics():
    try:
        # Fetch all data from MongoDB
        data = list(sales_collection.find({}))
        df = pd.DataFrame(data)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found")
        
        # Calculate line item statistics
        result = calculate_line_item_statistics(df)
        return {"line_item_statistics": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Distribution of In-Store vs. Online Transactions
def get_transaction_distribution():
    try:
        # MongoDB aggregation pipeline to calculate distribution
        pipeline = [
            {"$group": {"_id": "$instore_yn", "count": {"$sum": 1}}}
        ]
        result = list(sales_collection.aggregate(pipeline))

        distribution = {
            "In-Store" if item['_id'] else "Online": item['count']
            for item in result
        }
        return {"Distribution of In-Store vs. Online Transactions": distribution}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Which generation buys the most
def get_generation_counts(customer_df):
    try:
        generation_counts = customer_df['generation'].value_counts()
        return generation_counts.to_dict()
    except Exception as e:
        raise e

def get_generation_counts_endpoint():
    try:
        # Fetch data from MongoDB
        customer_collection = db['customer']
        data = list(customer_collection.find({}))
        if not data:
            raise HTTPException(status_code=404, detail="No generation data found")

        customer_df = pd.DataFrame(data)
        if customer_df.empty:
            raise HTTPException(status_code=404, detail="No generation data found")

        # Calculate generation counts
        generation_counts = get_generation_counts(customer_df)
        return {"generation_counts": generation_counts}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Daily sales per transaction for each day of the month
def get_daily_receipts(sales_df):
    # Ensure the 'transaction_date' column is in datetime format
    sales_df['transaction_date'] = pd.to_datetime(sales_df['transaction_date'])

    # Aggregate data to get daily receipts for each store
    daily_receipts = sales_df.groupby(['sales_outlet_id', sales_df['transaction_date'].dt.date]).size().reset_index(name='daily_receipts')
    
    return daily_receipts

def get_daily_receipts_for_store(store_id: int):
    try:
        # Fetch data from MongoDB for the specific store
        data = list(sales_collection.find({"sales_outlet_id": store_id}))
        sales_df = pd.DataFrame(data)
        
        if sales_df.empty:
            raise HTTPException(status_code=404, detail="No sales data found for the store")

        # Calculate daily receipts
        daily_receipts = get_daily_receipts(sales_df)
        result = daily_receipts.to_dict(orient="records")
        return {"store_id": store_id, "daily_receipts": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Best performing store for the month
def sales_for_month(df):
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df['month'] = df['transaction_date'].dt.to_period('M').astype(str)
    column_name = 'month'
    monthly_sales = calculate_line_item_amount(df, column_name)
    monthly_sales.columns = [column_name, 'sales_outlet_id', 'monthly_sales']
    monthly_sales['sales_outlet_id'] = monthly_sales['sales_outlet_id'].astype(str)
    return monthly_sales

def best_performing_store_for_month(df):
    monthly_sales = sales_for_month(df)
    best_store = monthly_sales.loc[monthly_sales.groupby('month')['monthly_sales'].idxmax()]
    return best_store

def get_best_performing_store_for_month():
    try:
        # Fetch data from MongoDB
        data = list(sales_collection.find({}))
        df = pd.DataFrame(data)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No sales data found")

        # Calculate the best performing store for each month
        best_store = best_performing_store_for_month(df)
        result = best_store.to_dict(orient="records")
        return {"best_performing_store_for_month": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Most sales city
def analyze_city_sales(df, outlet_city_df):
    try:
        # Merge sales data with city information
        merged_df = pd.merge(df, outlet_city_df, on='sales_outlet_id')
        
        # Group by city and calculate total sales amount
        city_sales = merged_df.groupby('store_city')['line_item_amount'].sum().reset_index()

        # Identify the city with the most sales
        most_sales_city = city_sales.loc[city_sales['line_item_amount'].idxmax()]

        return most_sales_city
    except Exception as e:
        raise e

def get_most_sales_city():
    try:
        # Fetch data from MongoDB
        sales_data = list(sales_collection.find({}))
        sales_outlet_collection = db['sales_outlet']
        outlet_city_data = list(sales_outlet_collection.find({}))

        if not sales_data or not outlet_city_data:
            raise HTTPException(status_code=404, detail="Required data not found")

        sales_df = pd.DataFrame(sales_data)
        outlet_city_df = pd.DataFrame(outlet_city_data)

        if sales_df.empty or outlet_city_df.empty:
            raise HTTPException(status_code=404, detail="Required data not found")

        # Analyze city sales
        most_sales_city = analyze_city_sales(sales_df, outlet_city_df)
        result = most_sales_city.to_dict()
        return {"most_sales_city": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# analyze tax status distribution
def analyze_tax_status_distribution(product_df):
    try:
        # Proportion of tax-exempt and taxable products
        tax_status_counts = product_df['tax_exempt_yn'].value_counts(normalize=True)
        return tax_status_counts
    except Exception as e:
        raise e

def get_tax_status_distribution():
    try:
        # Fetch data from MongoDB
        product_collection = db['product']
        product_data = list(product_collection.find({}))

        if not product_data:
            raise HTTPException(status_code=404, detail="Product data not found")

        product_df = pd.DataFrame(product_data)

        if product_df.empty:
            raise HTTPException(status_code=404, detail="Product data not found")

        # Analyze tax status distribution
        tax_status_counts = analyze_tax_status_distribution(product_df)
        result = tax_status_counts.to_dict()
        return {"tax_status_distribution": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Drink size distribution
def analyze_drink_size_distribution(product_df, sales_df):
    product_df['unit_of_measure'] = product_df['unit'].astype(str) + ' ' + product_df['measure']

    merged_df = pd.merge(sales_df, product_df, on='product_id')
    
    drinks_df = merged_df[merged_df['product_group'].str.contains('Beverages|Whole Bean/Teas', case=False, na=False)]

    drink_size_distribution = drinks_df.groupby('unit_of_measure')['quantity'].sum().sort_values(ascending=False)
    
    return drink_size_distribution

def get_drink_size_distribution():
    try:
        # Fetch data from MongoDB
        product_collection = db['product']
        product_data = list(product_collection.find({}))
        sales_data = list(sales_collection.find({}))

        if not product_data or not sales_data:
            raise HTTPException(status_code=404, detail="Required data not found")

        product_df = pd.DataFrame(product_data)
        sales_df = pd.DataFrame(sales_data)

        if product_df.empty or sales_df.empty:
            raise HTTPException(status_code=404, detail="Required data not found")

        # Analyze drink size distribution
        drink_size_distribution = analyze_drink_size_distribution(product_df, sales_df)
        result = drink_size_distribution.to_dict()
        return {"drink_size_distribution": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Most sold products
def most_sold_products(pastry_inventory_df, product_df, sales_df):
    try:
        common_product_ids = product_df[product_df['product_id'].isin(sales_df['product_id'])]['product_id']
        filtered_inventory = pastry_inventory_df[pastry_inventory_df['product_id'].isin(common_product_ids)]
        merged_df = filtered_inventory.merge(product_df, how='left', left_on='product_id', right_on='product_id')
        sorted_df = merged_df.sort_values(by='quantity_sold', ascending=False)
        return sorted_df.head(5)
    except Exception as e:
        raise e

def get_most_sold_products():
    try:
        # Fetch data from MongoDB
        pastry_inventory_collection = db['pastry_inventory']
        product_collection = db['product']
        pastry_inventory_data = list(pastry_inventory_collection.find({}))
        product_data = list(product_collection.find({}))
        sales_data = list(sales_collection.find({}))

        if not pastry_inventory_data or not product_data or not sales_data:
            raise HTTPException(status_code=404, detail="Required data not found")

        pastry_inventory_df = pd.DataFrame(pastry_inventory_data)
        product_df = pd.DataFrame(product_data)
        sales_df = pd.DataFrame(sales_data)

        if pastry_inventory_df.empty or product_df.empty or sales_df.empty:
            raise HTTPException(status_code=404, detail="Required data not found")

        # Calculate the most sold products
        top_5_sold_products = most_sold_products(pastry_inventory_df, product_df, sales_df)
        result = top_5_sold_products.to_dict(orient="records")
        return {"most_sold_products": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Average sales per transaction
def plot_average_sales_per_transaction_by_day_of_month(df, month):
    # Convert transaction_date to datetime if it is not already
    if not pd.api.types.is_datetime64_any_dtype(df['transaction_date']):
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    
    # Filter the DataFrame to include only the specified month
    df_filtered = df[df['transaction_date'].dt.month == month]
    
    # Group by transaction_date and calculate the average sales per transaction
    daily_sales_per_transac_by_month = df_filtered.groupby('transaction_date')['line_item_amount'].mean()
    return daily_sales_per_transac_by_month

def get_average_sales_per_transaction(month: int):
    try:
        # Fetch data from MongoDB
        sales_data = list(sales_collection.find({}))

        if not sales_data:
            raise HTTPException(status_code=404, detail="Sales data not found")

        sales_df = pd.DataFrame(sales_data)

        if sales_df.empty:
            raise HTTPException(status_code=404, detail="Sales data not found")

        # Calculate average sales per transaction by day of month
        average_sales = plot_average_sales_per_transaction_by_day_of_month(sales_df, month)
        result = average_sales.to_dict()
        return {"average_sales_per_transaction_by_day_of_month": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Daily sales per week
def get_daily_sales_per_week(sales_df, month):
    # Convert transaction_date to datetime if it is not already
    if not pd.api.types.is_datetime64_any_dtype(sales_df['transaction_date']):
        sales_df['transaction_date'] = pd.to_datetime(sales_df['transaction_date'])
    
    # Filter the DataFrame to include only the specified month
    sales_df = sales_df[sales_df['transaction_date'].dt.month == month]
    
    # Extract the day of the week (Monday=0, Sunday=6) and day name
    sales_df['day_of_week'] = sales_df['transaction_date'].dt.dayofweek
    sales_df['day_name'] = sales_df['transaction_date'].dt.day_name()
    
    # Group by store and day of week to calculate daily sales per week
    daily_sales_per_week = sales_df.groupby(['sales_outlet_id', 'day_of_week', 'day_name']).size().reset_index(name='daily_sales')
    
    # Sort by day_of_week to ensure the days are in order from Monday to Sunday
    daily_sales_per_week = daily_sales_per_week.sort_values(by='day_of_week')
    
    # Convert sales_outlet_id to string for plotting
    daily_sales_per_week['sales_outlet_id'] = daily_sales_per_week['sales_outlet_id'].astype(str)
 
    return daily_sales_per_week

def get_daily_sales_per_week_endpoint(month: int):
    try:
        # Fetch data from MongoDB
        sales_data = list(sales_collection.find({}))

        if not sales_data:
            raise HTTPException(status_code=404, detail="Sales data not found")

        sales_df = pd.DataFrame(sales_data)

        if sales_df.empty:
            raise HTTPException(status_code=404, detail="Sales data not found")

        # Calculate daily sales per week for the specified month
        daily_sales_per_week = get_daily_sales_per_week(sales_df, month)
        result = daily_sales_per_week.to_dict(orient="records")
        return {"daily_sales_per_week": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


    