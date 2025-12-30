# ------------------ Step 1: Read file ------------------
def read_sales_data(filename):
    encodings = ["utf-8", "cp1252", "latin-1"]

    for enc in encodings:
        try:
            with open(filename, "r", encoding=enc) as f:
                next(f, None)  # skip header
                raw_lines = [line.strip() for line in f if line.strip()]
                return raw_lines

        except UnicodeDecodeError:
            continue

        except FileNotFoundError:
            print(f"Error: File not found → {filename}")
            return []

    print("Failed to decode file with known encodings.")
    return []

# ------------------ Step 2: Parse transactions ------------------
def parse_transactions(raw_lines):
    transactions = []
    
    for line in raw_lines:
        parts = line.split("|")
        if len(parts) != 8:
            continue
        
        (txn_id, date, prod_id, prod_name,
         qty, price, cust_id, region) = parts
        
        # Replace commas in product names
        prod_name = prod_name.replace(",", " ")

        try:
            qty = int(qty.replace(",", ""))
            price = float(price.replace(",", ""))
        except ValueError:
            continue
        
        transactions.append({
            "TransactionID": txn_id,
            "Date": date,
            "ProductID": prod_id,
            "ProductName": prod_name.strip(),
            "Quantity": qty,
            "UnitPrice": price,
            "CustomerID": cust_id,
            "Region": region
        })
    
    return transactions

# ------------------ Step 3: Validate and filter ------------------
def validate_and_filter(transactions, region=None, min_amount=None, max_amount=None):
    required_fields = ['TransactionID', 'Date', 'ProductID', 'ProductName',
                       'Quantity', 'UnitPrice', 'CustomerID', 'Region']

    valid_transactions = []
    invalid_count = 0
    total_input = len(transactions)

    # Validation
    for txn in transactions:
        if not all(field in txn for field in required_fields):
            invalid_count += 1
            continue
        if txn['Quantity'] <= 0 or txn['UnitPrice'] <= 0:
            invalid_count += 1
            continue
        if not (str(txn['TransactionID']).startswith('T') and
                str(txn['ProductID']).startswith('P') and
                str(txn['CustomerID']).startswith('C')):
            invalid_count += 1
            continue
        valid_transactions.append(txn)

    print(f"Total input records: {total_input}")
    print(f"Invalid records found: {invalid_count}")
    print(f"Valid records after validation: {len(valid_transactions)}\n")

    filtered_by_region_count = 0
    filtered_by_amount_count = 0

    # Filter by region
    if region:
        available_regions = set(txn['Region'] for txn in valid_transactions)
        print(f"Available regions: {available_regions}")
        before_region_filter = len(valid_transactions)
        valid_transactions = [txn for txn in valid_transactions if txn['Region'] == region]
        filtered_by_region_count = before_region_filter - len(valid_transactions)
        print(f"Records after filtering by region '{region}': {len(valid_transactions)}")

    # Filter by amount
    if min_amount is not None or max_amount is not None:
        def within_amount(txn):
            amt = txn['Quantity'] * txn['UnitPrice']
            if min_amount is not None and amt < min_amount:
                return False
            if max_amount is not None and amt > max_amount:
                return False
            return True

        before_amount_filter = len(valid_transactions)
        valid_transactions = [txn for txn in valid_transactions if within_amount(txn)]
        filtered_by_amount_count = before_amount_filter - len(valid_transactions)
        print(f"Records after filtering by amount range: {len(valid_transactions)}\n")

    final_count = len(valid_transactions)

    filter_summary = {
        'total_input': total_input,
        'invalid': invalid_count,
        'filtered_by_region': filtered_by_region_count,
        'filtered_by_amount': filtered_by_amount_count,
        'final_count': final_count
    }

    return valid_transactions, invalid_count, filter_summary

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
filename = "C:/Masai project/data/sales_data.txt"

# 1️⃣ Read file
raw_lines = read_sales_data(filename)

# 2️⃣ Parse transactions
transactions = parse_transactions(raw_lines)

# 3️⃣ Validate and filter (example: region='North', min_amount=10000)
valid_transactions, invalid_count, summary = validate_and_filter(
    transactions, region='North', min_amount=10000
)

# 4️⃣ Print valid transactions
print("\nFinal Valid Transactions:")
for txn in valid_transactions:
    print(txn)

# 5️⃣ Print summary
print("\nSummary:")
print(summary)

# 6️⃣ Daily sales trend
trend = daily_sales_trend(valid_transactions)
print("\nDaily Sales Trend:")
for date, stats in trend.items():
    print(f"{date}: {stats}")

# 7️⃣ Peak sales day
peak_date, peak_revenue, peak_txn_count = find_peak_sales_day(valid_transactions)
print(f"\nPeak Sales Day: {peak_date}, Revenue: {peak_revenue}, Transactions: {peak_txn_count}")

# 8️⃣ Low-performing products
low_products = low_performing_products(valid_transactions, threshold=10)
print("\nLow-Performing Products (Quantity < 10):")
for product, qty, revenue in low_products:
    print(f"{product}: Quantity={qty}, Revenue={revenue}")
