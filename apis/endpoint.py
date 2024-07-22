from fastapi import FastAPI
from services import *

app = FastAPI()

@app.get("/daily_sales/{store_id}")
def daily_sales(store_id: int):
    return get_daily_sales(store_id)

@app.get("/weekly_sales/{store_id}")
def weekly_sales(store_id: int):
    return get_weekly_sales(store_id)

@app.get("/monthly_sales/{store_id}")
def monthly_sales(store_id: int):
    return  get_monthly_sales(store_id)

@app.get("/peak_hours/{store_id}")
def peak_hours(store_id: int):
    return get_peak_hours_for_store(store_id)

@app.get("/customer_type/{store_id}")
def customer_type(store_id: int):
    return get_sales_by_customer_type(store_id)

@app.get("/most_selling_item/{store_id}")
def most_selling_item(store_id: int):
    return get_most_selling_item(store_id)

@app.get("/sales_comparison")
def sales_comparison():
    return get_sales_comparison()

@app.get("/line_item_statistics")
def line_item_statistics():
    return get_line_item_statistics()

@app.get("/transaction_distribution")
def transaction_distribution():
    return get_transaction_distribution()

@app.get("/generation_counts")
def generation_counts():
    return get_generation_counts_endpoint()

@app.get("/daily_receipts/{store_id}")
def daily_receipts(store_id: int):
    return get_daily_receipts_for_store(store_id)

@app.get("/best_performing_store_for_month")
def best_performing_store_for_month():
    return get_best_performing_store_for_month()

@app.get("/most_sales_city")
async def most_sales_city_endpoint():
    return get_most_sales_city()

@app.get("/tax_status_distribution")
async def tax_status_distribution_endpoint():
    return get_tax_status_distribution()

@app.get("/drink_size_distribution")
async def drink_size_distribution_endpoint():
    return get_drink_size_distribution()

@app.get("/most_sold_products")
async def most_sold_products_endpoint():
    return get_most_sold_products()

@app.get("/average_sales_per_transaction/{month}")
async def average_sales_per_transaction_endpoint(month: int):
    return get_average_sales_per_transaction(month)

@app.get("/daily_sales_per_week/{month}")
async def daily_sales_per_week(month: int):
    return get_daily_sales_per_week_endpoint(month)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=5000)