# utils/data_processor.py
import os
from collections import defaultdict
from datetime import datetime
from datetime import datetime
from collections import defaultdict


# --------------------------------------------------
# Task 2.1(a): Total Revenue
# --------------------------------------------------
def calculate_total_revenue(transactions):
    return sum(txn["Quantity"] * txn["UnitPrice"] for txn in transactions)


# --------------------------------------------------
# Task 2.1(b): Region-wise Sales
# --------------------------------------------------
def region_wise_sales(transactions):
    region_data = defaultdict(lambda: {"total_sales": 0, "transaction_count": 0})
    total_sales = 0

    for txn in transactions:
        revenue = txn["Quantity"] * txn["UnitPrice"]
        region = txn["Region"]

        region_data[region]["total_sales"] += revenue
        region_data[region]["transaction_count"] += 1
        total_sales += revenue

    result = {}
    for region, data in sorted(
        region_data.items(),
        key=lambda x: x[1]["total_sales"],
        reverse=True
    ):
        result[region] = {
            "total_sales": data["total_sales"],
            "transaction_count": data["transaction_count"],
            "percentage": round((data["total_sales"] / total_sales) * 100, 2)
        }

    return result


# --------------------------------------------------
# Task 2.1(c): Top Selling Products
# --------------------------------------------------
def top_selling_products(transactions, n=5):
    product_data = defaultdict(lambda: {"qty": 0, "revenue": 0})

    for txn in transactions:
        p = txn["ProductName"]
        product_data[p]["qty"] += txn["Quantity"]
        product_data[p]["revenue"] += txn["Quantity"] * txn["UnitPrice"]

    sorted_products = sorted(
        product_data.items(),
        key=lambda x: x[1]["qty"],
        reverse=True
    )

    return [
        (product, data["qty"], data["revenue"])
        for product, data in sorted_products[:n]
    ]


# --------------------------------------------------
# Task 2.1(d): Customer Purchase Analysis
# --------------------------------------------------
def customer_analysis(transactions):
    customer_data = defaultdict(lambda: {"total_spent": 0, "orders": 0, "products": set()})

    for txn in transactions:
        cid = txn["CustomerID"]
        amount = txn["Quantity"] * txn["UnitPrice"]

        customer_data[cid]["total_spent"] += amount
        customer_data[cid]["orders"] += 1
        customer_data[cid]["products"].add(txn["ProductName"])

    result = {}
    for cid, data in sorted(
        customer_data.items(),
        key=lambda x: x[1]["total_spent"],
        reverse=True
    ):
        result[cid] = {
            "total_spent": data["total_spent"],
            "avg_order_value": round(data["total_spent"] / data["orders"], 2),
            "products_bought": list(data["products"])
        }

    return result


# --------------------------------------------------
# Task 2.2(a): Daily Sales Trend
# --------------------------------------------------
def daily_sales_trend(transactions):
    daily_data = defaultdict(lambda: {
        "revenue": 0,
        "transaction_count": 0,
        "customers": set()
    })

    for txn in transactions:
        date = txn["Date"]
        revenue = txn["Quantity"] * txn["UnitPrice"]

        daily_data[date]["revenue"] += revenue
        daily_data[date]["transaction_count"] += 1
        daily_data[date]["customers"].add(txn["CustomerID"])

    result = {}
    for date in sorted(daily_data.keys(), key=lambda x: datetime.strptime(x, "%Y-%m-%d")):
        result[date] = {
            "revenue": daily_data[date]["revenue"],
            "transaction_count": daily_data[date]["transaction_count"],
            "unique_customers": len(daily_data[date]["customers"])
        }

    return result


# --------------------------------------------------
# Task 2.2(b): Peak Sales Day
# --------------------------------------------------
def find_peak_sales_day(transactions):
    daily = defaultdict(lambda: {"revenue": 0, "count": 0})

    for txn in transactions:
        date = txn["Date"]
        daily[date]["revenue"] += txn["Quantity"] * txn["UnitPrice"]
        daily[date]["count"] += 1

    peak_date, peak_data = max(
        daily.items(),
        key=lambda x: x[1]["revenue"]
    )

    return peak_date, peak_data["revenue"], peak_data["count"]


# --------------------------------------------------
# Task 2.3: Low Performing Products
# --------------------------------------------------
def low_performing_products(transactions, threshold=10):
    product_data = defaultdict(lambda: {"qty": 0, "revenue": 0})

    for txn in transactions:
        p = txn["ProductName"]
        product_data[p]["qty"] += txn["Quantity"]
        product_data[p]["revenue"] += txn["Quantity"] * txn["UnitPrice"]

    low_products = [
        (product, data["qty"], data["revenue"])
        for product, data in product_data.items()
        if data["qty"] < threshold
    ]

    return sorted(low_products, key=lambda x: x[1])

# --------------------------------------------------
# Task 4: Report Generation
# --------------------------------------------------

def generate_sales_report(transactions, enriched_transactions, output_file="output/sales_report.txt"):
    """
    Generates a comprehensive formatted sales report
    """

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    total_transactions = len(transactions)
    total_revenue = sum(t["Quantity"] * t["UnitPrice"] for t in transactions)
    avg_order_value = total_revenue / total_transactions if total_transactions else 0

    dates = [t["Date"] for t in transactions]
    date_range = f"{min(dates)} to {max(dates)}" if dates else "N/A"

    # -------- Region-wise Performance --------
    region_stats = defaultdict(lambda: {"sales": 0, "count": 0})

    for t in transactions:
        amount = t["Quantity"] * t["UnitPrice"]
        region_stats[t["Region"]]["sales"] += amount
        region_stats[t["Region"]]["count"] += 1

    # -------- Top Products --------
    product_stats = defaultdict(lambda: {"qty": 0, "rev": 0})
    for t in transactions:
        product_stats[t["ProductName"]]["qty"] += t["Quantity"]
        product_stats[t["ProductName"]]["rev"] += t["Quantity"] * t["UnitPrice"]

    top_products = sorted(
        product_stats.items(),
        key=lambda x: x[1]["rev"],
        reverse=True
    )[:5]

    # -------- Top Customers --------
    customer_stats = defaultdict(lambda: {"spent": 0, "count": 0})
    for t in transactions:
        customer_stats[t["CustomerID"]]["spent"] += t["Quantity"] * t["UnitPrice"]
        customer_stats[t["CustomerID"]]["count"] += 1

    top_customers = sorted(
        customer_stats.items(),
        key=lambda x: x[1]["spent"],
        reverse=True
    )[:5]

    # -------- Daily Sales Trend --------
    daily_stats = defaultdict(lambda: {"rev": 0, "tx": set(), "cust": set()})
    for t in transactions:
        daily_stats[t["Date"]]["rev"] += t["Quantity"] * t["UnitPrice"]
        daily_stats[t["Date"]]["tx"].add(t["TransactionID"])
        daily_stats[t["Date"]]["cust"].add(t["CustomerID"])

    # -------- API Enrichment Summary --------
    enriched_count = sum(1 for t in enriched_transactions if t["API_Match"])
    failed_products = sorted(
        set(t["ProductName"] for t in enriched_transactions if not t["API_Match"])
    )
    success_rate = (enriched_count / len(enriched_transactions)) * 100 if enriched_transactions else 0

    # -------- Write Report --------
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("=" * 40 + "\n")
        f.write("SALES ANALYTICS REPORT\n")
        f.write(f"Generated: {datetime.now()}\n")
        f.write(f"Records Processed: {total_transactions}\n")
        f.write("=" * 40 + "\n\n")

        f.write("OVERALL SUMMARY\n")
        f.write("-" * 40 + "\n")
        f.write(f"Total Revenue: ₹{total_revenue:,.2f}\n")
        f.write(f"Total Transactions: {total_transactions}\n")
        f.write(f"Average Order Value: ₹{avg_order_value:,.2f}\n")
        f.write(f"Date Range: {date_range}\n\n")

        f.write("REGION-WISE PERFORMANCE\n")
        f.write("-" * 40 + "\n")
        f.write("Region | Sales | % of Total | Transactions\n")
        for region, data in sorted(region_stats.items(), key=lambda x: x[1]["sales"], reverse=True):
            percent = (data["sales"] / total_revenue) * 100 if total_revenue else 0
            f.write(f"{region} | ₹{data['sales']:,.0f} | {percent:.2f}% | {data['count']}\n")
        f.write("\n")

        f.write("TOP 5 PRODUCTS\n")
        f.write("-" * 40 + "\n")
        for i, (prod, stats) in enumerate(top_products, 1):
            f.write(f"{i}. {prod} | Qty: {stats['qty']} | Revenue: ₹{stats['rev']:,.0f}\n")
        f.write("\n")

        f.write("TOP 5 CUSTOMERS\n")
        f.write("-" * 40 + "\n")
        for i, (cust, stats) in enumerate(top_customers, 1):
            f.write(f"{i}. {cust} | Spent: ₹{stats['spent']:,.0f} | Orders: {stats['count']}\n")
        f.write("\n")

        f.write("DAILY SALES TREND\n")
        f.write("-" * 40 + "\n")
        for date, d in sorted(daily_stats.items()):
            f.write(f"{date} | Revenue: ₹{d['rev']:,.0f} | Transactions: {len(d['tx'])} | Customers: {len(d['cust'])}\n")
        f.write("\n")

        f.write("API ENRICHMENT SUMMARY\n")
        f.write("-" * 40 + "\n")
        f.write(f"Total Products Enriched: {enriched_count}\n")
        f.write(f"Success Rate: {success_rate:.2f}%\n")
        if failed_products:
            f.write("Products Not Enriched:\n")
            for p in failed_products:
                f.write(f"- {p}\n")

    return output_file