import os
from utils.file_handler import read_sales_data, parse_transactions, validate_and_filter
from utils.data_processor import (
    calculate_total_revenue,
    region_wise_sales,
    top_selling_products,
    customer_analysis,
    daily_sales_trend,
    find_peak_sales_day,
    low_performing_products,
    generate_sales_report
)
from utils.api_handler import (
    fetch_all_products,
    create_product_mapping,
    enrich_sales_data,
    save_enriched_data
)


def main():
    try:
        print("=" * 40)
        print("SALES ANALYTICS SYSTEM")
        print("=" * 40)

        base_dir = os.path.dirname(__file__)
        sales_file = os.path.join(base_dir, "data", "sales_data.txt")

        print("\n[1/10] Reading sales data...")
        lines = read_sales_data(sales_file)
        print(f"✓ Successfully read {len(lines)} transactions")

        print("\n[2/10] Parsing and cleaning data...")
        transactions = parse_transactions(lines)
        print(f"✓ Parsed {len(transactions)} records")

        regions = sorted(set(t["Region"] for t in transactions))
        amounts = [t["Quantity"] * t["UnitPrice"] for t in transactions]

        print("\n[3/10] Filter Options Available:")
        print(f"Regions: {', '.join(regions)}")
        print(f"Amount Range: ₹{min(amounts):,.0f} - ₹{max(amounts):,.0f}")

        apply_filter = input("\nDo you want to filter data? (y/n): ").lower()

        region = min_amt = max_amt = None
        if apply_filter == "y":
            region = input("Enter region (or press Enter to skip): ") or None
            min_amt = input("Enter minimum amount (or press Enter): ")
            max_amt = input("Enter maximum amount (or press Enter): ")

            min_amt = float(min_amt) if min_amt else None
            max_amt = float(max_amt) if max_amt else None

        print("\n[4/10] Validating transactions...")
        valid_txns, invalid_count, summary = validate_and_filter(
            transactions, region, min_amt, max_amt
        )
        print(summary)

        print("\n[5/10] Analyzing sales data...")
        calculate_total_revenue(valid_txns)
        region_wise_sales(valid_txns)
        top_selling_products(valid_txns)
        customer_analysis(valid_txns)
        daily_sales_trend(valid_txns)
        find_peak_sales_day(valid_txns)
        low_performing_products(valid_txns)
        print("✓ Analysis complete")

        print("\n[6/10] Fetching product data from API...")
        products = fetch_all_products()
        product_map = create_product_mapping(products)
        print(f"✓ Fetched {len(product_map)} products")

        print("\n[7/10] Enriching sales data...")
        enriched = enrich_sales_data(valid_txns, product_map)
        success = sum(1 for t in enriched if t["API_Match"])
        print(f"✓ Enriched {success}/{len(enriched)} transactions ({(success/len(enriched))*100:.1f}%)")

        print("\n[8/10] Saving enriched data...")
        save_enriched_data(enriched)
        print("✓ Saved to data/enriched_sales_data.txt")

        print("\n[9/10] Generating report...")
        report_path = generate_sales_report(valid_txns, enriched)
        print(f"✓ Report saved to {report_path}")

        print("\n[10/10] Process Complete!")
        print("=" * 40)

    except Exception as e:
        print("\n❌ ERROR:", e)
        print("Process terminated safely.")


if __name__ == "__main__":
    main()
