from file_handler import read_sales_data, parse_transactions, validate_and_filter

# ------------------ Step 4: Daily Sales Trend ------------------
def daily_sales_trend(transactions):
    from collections import defaultdict
    from datetime import datetime

    daily_data = defaultdict(lambda: {'revenue': 0.0, 'transaction_count': 0, 'unique_customers': set()})

    for txn in transactions:
        date = txn['Date']
        revenue = txn['Quantity'] * txn['UnitPrice']
        customer = txn['CustomerID']

        daily_data[date]['revenue'] += revenue
        daily_data[date]['transaction_count'] += 1
        daily_data[date]['unique_customers'].add(customer)

    trend = {}
    for date_str in sorted(daily_data.keys(), key=lambda x: datetime.strptime(x, "%Y-%m-%d")):
        trend[date_str] = {
            'revenue': daily_data[date_str]['revenue'],
            'transaction_count': daily_data[date_str]['transaction_count'],
            'unique_customers': len(daily_data[date_str]['unique_customers'])
        }

    return trend

# ------------------ Step 5: Peak Sales Day ------------------
def find_peak_sales_day(transactions):
    from collections import defaultdict

    daily_data = defaultdict(lambda: {'revenue': 0.0, 'transaction_count': 0})

    for txn in transactions:
        date = txn['Date']
        revenue = txn['Quantity'] * txn['UnitPrice']
        daily_data[date]['revenue'] += revenue
        daily_data[date]['transaction_count'] += 1

    peak_date = None
    peak_revenue = 0.0
    peak_count = 0

    for date, stats in daily_data.items():
        if stats['revenue'] > peak_revenue:
            peak_date = date
            peak_revenue = stats['revenue']
            peak_count = stats['transaction_count']

    return peak_date, peak_revenue, peak_count

# ------------------ Step 6: Low Performing Products ------------------
def low_performing_products(transactions, threshold=10):
    from collections import defaultdict

    product_data = defaultdict(lambda: {'total_qty': 0, 'total_revenue': 0.0})

    for txn in transactions:
        product = txn['ProductName']
        qty = txn['Quantity']
        revenue = txn['Quantity'] * txn['UnitPrice']

        product_data[product]['total_qty'] += qty
        product_data[product]['total_revenue'] += revenue

    low_products = [
        (product, data['total_qty'], data['total_revenue'])
        for product, data in product_data.items()
        if data['total_qty'] < threshold
    ]

    low_products.sort(key=lambda x: x[1])
    return low_products

# ------------------ Step 7: Run full pipeline ------------------
# filename = "C:/Masai project/data/sales_data.txt"

# # 1️⃣ Read file
# raw_lines = read_sales_data(filename)

# # 2️⃣ Parse transactions
# transactions = parse_transactions(raw_lines)

# # 3️⃣ Validate and filter (example: region='North', min_amount=10000)
# valid_transactions, invalid_count, summary = validate_and_filter(
#     transactions, region='North', min_amount=10000
# )

# # 4️⃣ Print valid transactions
# print("\nFinal Valid Transactions:")
# for txn in valid_transactions:
#     print(txn)

# # 5️⃣ Print summary
# print("\nSummary:")
# print(summary)

# # 6️⃣ Daily sales trend
# trend = daily_sales_trend(valid_transactions)
# print("\nDaily Sales Trend:")
# for date, stats in trend.items():
#     print(f"{date}: {stats}")

# # 7️⃣ Peak sales day
# peak_date, peak_revenue, peak_txn_count = find_peak_sales_day(valid_transactions)
# print(f"\nPeak Sales Day: {peak_date}, Revenue: {peak_revenue}, Transactions: {peak_txn_count}")

# # 8️⃣ Low-performing products
# low_products = low_performing_products(valid_transactions, threshold=10)
# print("\nLow-Performing Products (Quantity < 10):")
# for product, qty, revenue in low_products:
#     print(f"{product}: Quantity={qty}, Revenue={revenue}")

# ------------------ Step 7: Run full pipeline ------------------
if __name__ == "__main__":

    filename = "C:/Masai project/data/sales_data.txt"

    raw_lines = read_sales_data(filename)
    transactions = parse_transactions(raw_lines)

    valid_transactions, invalid_count, summary = validate_and_filter(
        transactions, region='North', min_amount=10000
    )

    print("\nSummary:")
    print(summary)

    trend = daily_sales_trend(valid_transactions)
    print("\nDaily Sales Trend:")
    for date, stats in trend.items():
        print(f"{date}: {stats}")

    peak_date, peak_revenue, peak_txn_count = find_peak_sales_day(valid_transactions)
    print(f"\nPeak Sales Day: {peak_date}, Revenue: {peak_revenue}, Transactions: {peak_txn_count}")

    low_products = low_performing_products(valid_transactions, threshold=10)
    print("\nLow-Performing Products (Quantity < 10):")
    for product, qty, revenue in low_products:
        print(f"{product}: Quantity={qty}, Revenue={revenue}")
