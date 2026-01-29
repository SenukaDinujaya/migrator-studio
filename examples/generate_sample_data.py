"""
Generate sample synthetic ERP data for development/testing.

Creates interconnected tables as feather files:
- DAT-00000001.feather: Customers (100 rows)
- DAT-00000002.feather: Products (50 rows)
- DAT-00000003.feather: Sales Orders (100 rows)
- DAT-00000004.feather: Order Lines (~250 rows)
- DAT-00000005.feather: Region Lookup (10 rows)
- DAT-00000006.feather: Status Lookup (6 rows)

Includes data quality issues for transformation demos:
- Null values, empty strings, whitespace
- Invalid dates (9999-12-31)
- Various status codes
"""

import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

# Seed for reproducibility
random.seed(42)

OUTPUT_DIR = Path(__file__).parent


def generate_region_lookup() -> pd.DataFrame:
    """DAT-00000005: Region lookup table."""
    regions = [
        ("NA", "North America"),
        ("EU", "Europe"),
        ("APAC", "Asia Pacific"),
        ("LATAM", "Latin America"),
        ("MEA", "Middle East & Africa"),
        ("ANZ", "Australia & New Zealand"),
        ("UK", "United Kingdom"),
        ("DACH", "Germany/Austria/Switzerland"),
        ("NORDIC", "Nordic Countries"),
        ("SEA", "Southeast Asia"),
    ]
    return pd.DataFrame(regions, columns=["RegionCode", "RegionName"])


def generate_status_lookup() -> pd.DataFrame:
    """DAT-00000006: Status lookup table."""
    statuses = [
        ("A", "Active"),
        ("I", "Inactive"),
        ("P", "Pending"),
        ("S", "Suspended"),
        ("C", "Closed"),
        ("D", "Deleted"),
    ]
    return pd.DataFrame(statuses, columns=["StatusCode", "StatusDesc"])


def generate_products() -> pd.DataFrame:
    """DAT-00000002: Products table with 50 items."""
    categories = ["Electronics", "Office Supplies", "Furniture", "Software", "Hardware"]

    products = []
    for i in range(1, 51):
        category = random.choice(categories)

        # Generate item code with some variations for data quality issues
        if i == 15:
            item_code = "  ITEM-015  "  # Whitespace issue
        elif i == 30:
            item_code = ""  # Empty string
        else:
            item_code = f"ITEM-{i:03d}"

        # Generate item name with some issues
        if i == 22:
            item_name = None  # Null value
        elif i == 45:
            item_name = "   "  # Whitespace only
        else:
            item_name = f"{category} Product {i}"

        # Price with some null values
        if i == 8:
            unit_price = None
        else:
            unit_price = round(random.uniform(10.0, 500.0), 2)

        products.append({
            "ProductID": i,
            "ItemCode": item_code,
            "ItemName": item_name,
            "Category": category,
            "UnitPrice": unit_price,
        })

    return pd.DataFrame(products)


def generate_customers(regions: pd.DataFrame, statuses: pd.DataFrame) -> pd.DataFrame:
    """DAT-00000001: Customers table with 100 customers."""
    first_names = ["John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Lisa", "William", "Jennifer"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
    domains = ["gmail.com", "yahoo.com", "outlook.com", "company.com", "business.net"]

    region_codes = regions["RegionCode"].tolist()
    status_codes = statuses["StatusCode"].tolist()

    customers = []
    for i in range(1, 101):
        first = random.choice(first_names)
        last = random.choice(last_names)

        # Name with some data quality issues
        if i == 5:
            name = None  # Null
        elif i == 25:
            name = ""  # Empty string
        elif i == 50:
            name = "  John Doe  "  # Extra whitespace
        else:
            name = f"{first} {last}"

        # Email with issues
        if i == 10:
            email = None
        elif i == 35:
            email = "invalid-email"  # Missing @ and domain
        elif i == 60:
            email = "   "  # Whitespace only
        else:
            email = f"{first.lower()}.{last.lower()}{i}@{random.choice(domains)}"

        # Phone with issues
        if i == 15:
            phone = None
        elif i == 40:
            phone = "INVALID"  # Non-numeric
        else:
            phone = f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

        # Region - some null values
        if i == 20:
            region = None
        elif i == 55:
            region = "INVALID"  # Code not in lookup
        else:
            region = random.choice(region_codes)

        # Status
        status = random.choice(status_codes)

        # Address with some issues
        if i == 30:
            address = None
        elif i == 70:
            address = ""
        else:
            address = f"{random.randint(100,9999)} {random.choice(['Main', 'Oak', 'Maple', 'Cedar', 'Pine'])} St"

        customers.append({
            "CustomerID": i,
            "Name": name,
            "Status": status,
            "Region": region,
            "Email": email,
            "Phone": phone,
            "Address": address,
        })

    return pd.DataFrame(customers)


def generate_sales_orders(customers: pd.DataFrame, statuses: pd.DataFrame) -> pd.DataFrame:
    """DAT-00000003: Sales Orders table with 100 orders."""
    customer_ids = customers["CustomerID"].tolist()
    order_statuses = ["OPEN", "CLOSED", "PENDING", "SHIPPED", "CANCELLED"]

    orders = []
    base_date = date(2024, 1, 1)

    for i in range(1, 101):
        order_num = f"SO-{i:06d}"
        customer_id = random.choice(customer_ids)

        # Order date with some data quality issues
        if i == 12:
            order_date = date(9999, 12, 31)  # Invalid far-future date
        elif i == 28:
            order_date = None  # Null date
        elif i == 45:
            order_date = date(1900, 1, 1)  # Very old date
        else:
            order_date = base_date + timedelta(days=random.randint(0, 365))

        # Status with some variations
        if i == 18:
            status = None
        elif i == 65:
            status = "UNKNOWN"  # Invalid status
        elif i == 80:
            status = "  OPEN  "  # Whitespace
        else:
            status = random.choice(order_statuses)

        # Total amount (will be calculated from lines, but some with issues)
        if i == 33:
            total_amount = None
        elif i == 55:
            total_amount = -100.00  # Negative amount
        else:
            total_amount = round(random.uniform(100.0, 5000.0), 2)

        orders.append({
            "OrderNum": order_num,
            "CustomerID": customer_id,
            "OrderDate": order_date,
            "Status": status,
            "TotalAmount": total_amount,
        })

    return pd.DataFrame(orders)


def generate_order_lines(orders: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    """DAT-00000004: Order Lines table with ~250 lines."""
    order_nums = orders["OrderNum"].tolist()
    item_codes = products["ItemCode"].tolist()
    # Filter out empty/whitespace item codes for most lines
    valid_item_codes = [c for c in item_codes if c and str(c).strip()]

    lines = []
    line_id = 0

    for order_num in order_nums:
        # Random number of lines per order (1-5)
        num_lines = random.randint(1, 5)

        for line_num in range(1, num_lines + 1):
            line_id += 1

            # Item code with some issues
            if line_id == 50:
                item_code = None
            elif line_id == 100:
                item_code = "INVALID-ITEM"  # Not in products
            elif line_id == 150:
                item_code = ""
            else:
                item_code = random.choice(valid_item_codes)

            # Quantity with issues
            if line_id == 75:
                quantity = None
            elif line_id == 125:
                quantity = 0  # Zero quantity
            elif line_id == 175:
                quantity = -5  # Negative
            else:
                quantity = random.randint(1, 20)

            # Amount with issues
            if line_id == 60:
                amount = None
            elif line_id == 110:
                amount = 0.0
            else:
                amount = round(random.uniform(50.0, 1000.0), 2)

            lines.append({
                "OrderNum": order_num,
                "LineNum": line_num,
                "ItemCode": item_code,
                "Quantity": quantity,
                "Amount": amount,
            })

    return pd.DataFrame(lines)


def main():
    """Generate all sample data files."""
    print("Generating sample ERP data...")

    # Generate lookup tables first
    print("  - Generating Region Lookup (DAT-00000005)...")
    regions = generate_region_lookup()
    regions.to_feather(OUTPUT_DIR / "DAT-00000005.feather")
    print(f"    Created: {len(regions)} regions")

    print("  - Generating Status Lookup (DAT-00000006)...")
    statuses = generate_status_lookup()
    statuses.to_feather(OUTPUT_DIR / "DAT-00000006.feather")
    print(f"    Created: {len(statuses)} statuses")

    # Generate products
    print("  - Generating Products (DAT-00000002)...")
    products = generate_products()
    products.to_feather(OUTPUT_DIR / "DAT-00000002.feather")
    print(f"    Created: {len(products)} products")

    # Generate customers (depends on regions, statuses)
    print("  - Generating Customers (DAT-00000001)...")
    customers = generate_customers(regions, statuses)
    customers.to_feather(OUTPUT_DIR / "DAT-00000001.feather")
    print(f"    Created: {len(customers)} customers")

    # Generate sales orders (depends on customers)
    print("  - Generating Sales Orders (DAT-00000003)...")
    orders = generate_sales_orders(customers, statuses)
    orders.to_feather(OUTPUT_DIR / "DAT-00000003.feather")
    print(f"    Created: {len(orders)} orders")

    # Generate order lines (depends on orders, products)
    print("  - Generating Order Lines (DAT-00000004)...")
    lines = generate_order_lines(orders, products)
    lines.to_feather(OUTPUT_DIR / "DAT-00000004.feather")
    print(f"    Created: {len(lines)} order lines")

    print("\nDone! Generated files:")
    for f in sorted(OUTPUT_DIR.glob("DAT-*.feather")):
        print(f"  - {f.name}")

    print("\nData quality issues included for transformation demos:")
    print("  - Null values in various columns")
    print("  - Empty strings and whitespace-only values")
    print("  - Invalid dates (9999-12-31, 1900-01-01)")
    print("  - Invalid codes not in lookup tables")
    print("  - Negative and zero values where unexpected")


if __name__ == "__main__":
    main()
