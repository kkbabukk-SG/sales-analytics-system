# api_handler.py
import requests

BASE_URL = "https://dummyjson.com/products"


# --------------------------------------------------
# Task 3.1 (a): Fetch All Products
# --------------------------------------------------
def fetch_all_products():
    """
    Fetches all products from DummyJSON API
    Returns: list of product dictionaries
    """
    try:
        response = requests.get(BASE_URL, params={"limit": 100})
        response.raise_for_status()
        data = response.json()
        print("✅ Successfully fetched products from API")
        return data.get("products", [])
    except requests.RequestException as e:
        print("❌ Failed to fetch products:", e)
        return []


# --------------------------------------------------
# Task 3.1 (b): Create Product Mapping
# --------------------------------------------------
def create_product_mapping(api_products):
    """
    Creates a mapping of product IDs to product info
    Returns: dict {id: {title, category, brand, rating}}
    """
    product_mapping = {}

    for product in api_products:
        product_mapping[product["id"]] = {
            "title": product.get("title"),
            "category": product.get("category"),
            "brand": product.get("brand"),
            "rating": product.get("rating"),
        }

    return product_mapping


# --------------------------------------------------
# Task 3.2: Enrich Sales Data
# --------------------------------------------------
def enrich_sales_data(transactions, product_mapping):
    """
    Enriches transaction data using API product info
    Returns: list of enriched transaction dictionaries
    """
    enriched_transactions = []

    for tx in transactions:
        enriched_tx = tx.copy()

        # Extract numeric product ID (P101 -> 101)
        try:
            numeric_id = int(tx["ProductID"][1:])
        except (ValueError, KeyError):
            numeric_id = None

        product = product_mapping.get(numeric_id)

        if product:
            enriched_tx["API_Category"] = product["category"]
            enriched_tx["API_Brand"] = product["brand"]
            enriched_tx["API_Rating"] = product["rating"]
            enriched_tx["API_Match"] = True
        else:
            enriched_tx["API_Category"] = None
            enriched_tx["API_Brand"] = None
            enriched_tx["API_Rating"] = None
            enriched_tx["API_Match"] = False

        enriched_transactions.append(enriched_tx)

    return enriched_transactions


# --------------------------------------------------
# Helper Function: Save Enriched Data
# --------------------------------------------------
import os

def save_enriched_data(enriched_transactions, filename="enriched_sales_data.txt"):
    """
    Saves enriched transactions to data/enriched_sales_data.txt
    """

    base_dir = os.path.dirname(os.path.dirname(__file__))  # project root
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    file_path = os.path.join(data_dir, filename)

    headers = [
        "TransactionID",
        "Date",
        "ProductID",
        "ProductName",
        "Quantity",
        "UnitPrice",
        "CustomerID",
        "Region",
        "API_Category",
        "API_Brand",
        "API_Rating",
        "API_Match",
    ]

    with open(file_path, "w", encoding="utf-8") as file:
        file.write("|".join(headers) + "\n")

        for tx in enriched_transactions:
            row = [
                str(tx.get(col, "")) if tx.get(col) is not None else ""
                for col in headers
            ]
            file.write("|".join(row) + "\n")

    print(f"✅ Enriched data saved to {file_path}")



# --------------------------------------------------
# Example Usage (Optional for Testing)
# --------------------------------------------------
if __name__ == "__main__":
    # Step 1: Fetch products
    products = fetch_all_products()

    # Step 2: Create mapping
    product_map = create_product_mapping(products)

    # Sample transactions
    transactions = [
        {
            "TransactionID": "T001",
            "Date": "2024-12-01",
            "ProductID": "P1",
            "ProductName": "iPhone 9",
            "Quantity": 2,
            "UnitPrice": 549,
            "CustomerID": "C001",
            "Region": "North",
        },
        {
            "TransactionID": "T002",
            "Date": "2024-12-02",
            "ProductID": "P999",
            "ProductName": "Unknown",
            "Quantity": 1,
            "UnitPrice": 100,
            "CustomerID": "C002",
            "Region": "South",
        },
    ]

    # Step 3: Enrich sales data
    enriched = enrich_sales_data(transactions, product_map)

    # Step 4: Save file
    save_enriched_data(enriched)
