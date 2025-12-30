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

# ------------------ Step 4: Run the full pipeline ------------------
filename = "C:/Masai project/data/sales_data.txt"

# Read file
raw_lines = read_sales_data(filename)

# Parse transactions
transactions = parse_transactions(raw_lines)

# Validate and filter (example: region='North', min_amount=10000)
valid_transactions, invalid_count, summary = validate_and_filter(
    transactions, region='North', min_amount=10000
)

# Print results
print("\nFinal Valid Transactions:")
for txn in valid_transactions:
    print(txn)

print("\nSummary:")
print(summary)
