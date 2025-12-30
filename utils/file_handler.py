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


filename = "C:/Masai project/data/sales_data.txt"
raw_lines = read_sales_data(filename)
data = parse_transactions(raw_lines)

print(data)
