# utils/file_handler.py
import os


# --------------------------------------------------
# Task 1.1: Read Sales Data
# --------------------------------------------------
def read_sales_data(filename):
    """
    Reads sales data from file handling encoding issues
    Returns: list of raw lines (strings)
    """
    encodings = ["utf-8", "latin-1", "cp1252"]

    for enc in encodings:
        try:
            with open(filename, "r", encoding=enc) as file:
                lines = file.readlines()

            # Skip header and remove empty lines
            raw_lines = [line.strip() for line in lines[1:] if line.strip()]
            return raw_lines

        except UnicodeDecodeError:
            continue
        except FileNotFoundError:
            print(f"❌ File not found: {filename}")
            return []

    print("❌ Unable to read file with supported encodings.")
    return []


# --------------------------------------------------
# Task 1.2: Parse and Clean Data
# --------------------------------------------------
def parse_transactions(raw_lines):
    """
    Parses raw lines into clean list of dictionaries
    """
    transactions = []

    for line in raw_lines:
        parts = line.split("|")

        if len(parts) != 8:
            continue

        txn_id, date, prod_id, prod_name, qty, price, cust_id, region = parts

        # Clean product name (remove commas)
        prod_name = prod_name.replace(",", "").strip()

        try:
            qty = int(qty.replace(",", ""))
            price = float(price.replace(",", ""))
        except ValueError:
            continue

        transactions.append({
            "TransactionID": txn_id.strip(),
            "Date": date.strip(),
            "ProductID": prod_id.strip(),
            "ProductName": prod_name,
            "Quantity": qty,
            "UnitPrice": price,
            "CustomerID": cust_id.strip(),
            "Region": region.strip()
        })

    return transactions


# --------------------------------------------------
# Task 1.3: Validate and Filter Data
# --------------------------------------------------
def validate_and_filter(transactions, region=None, min_amount=None, max_amount=None):
    """
    Validates transactions and applies optional filters
    Returns: (valid_transactions, invalid_count, filter_summary)
    """
    required_fields = [
        "TransactionID", "Date", "ProductID", "ProductName",
        "Quantity", "UnitPrice", "CustomerID", "Region"
    ]

    valid_transactions = []
    invalid_count = 0
    total_input = len(transactions)

    # ---------------- Validation ----------------
    for txn in transactions:
        if not all(field in txn for field in required_fields):
            invalid_count += 1
            continue

        if txn["Quantity"] <= 0 or txn["UnitPrice"] <= 0:
            invalid_count += 1
            continue

        if not (
            txn["TransactionID"].startswith("T") and
            txn["ProductID"].startswith("P") and
            txn["CustomerID"].startswith("C")
        ):
            invalid_count += 1
            continue

        valid_transactions.append(txn)

    print(f"Total records parsed: {total_input}")
    print(f"Invalid records removed: {invalid_count}")
    print(f"Valid records after validation: {len(valid_transactions)}")

    # ---------------- Region Filter ----------------
    filtered_by_region = 0
    if region:
        available_regions = sorted(set(txn["Region"] for txn in valid_transactions))
        print(f"Available regions: {available_regions}")

        before = len(valid_transactions)
        valid_transactions = [
            txn for txn in valid_transactions if txn["Region"] == region
        ]
        filtered_by_region = before - len(valid_transactions)
        print(f"Records after region filter ({region}): {len(valid_transactions)}")

    # ---------------- Amount Filter ----------------
    filtered_by_amount = 0
    if min_amount is not None or max_amount is not None:
        amounts = [txn["Quantity"] * txn["UnitPrice"] for txn in valid_transactions]
        if amounts:
            print(f"Available transaction amount range: min={min(amounts)}, max={max(amounts)}")

        before = len(valid_transactions)

        def within_amount(txn):
            amount = txn["Quantity"] * txn["UnitPrice"]
            if min_amount is not None and amount < min_amount:
                return False
            if max_amount is not None and amount > max_amount:
                return False
            return True

        valid_transactions = [
            txn for txn in valid_transactions if within_amount(txn)
        ]
        filtered_by_amount = before - len(valid_transactions)
        print(f"Records after amount filter: {len(valid_transactions)}")

    summary = {
        "total_input": total_input,
        "invalid": invalid_count,
        "filtered_by_region": filtered_by_region,
        "filtered_by_amount": filtered_by_amount,
        "final_count": len(valid_transactions)
    }

    return valid_transactions, invalid_count, summary
