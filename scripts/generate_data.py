import pandas as pd
import random
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np

# Path to the bronze layer folder
BRONZE_PATH = Path("data/bronze")

# Available products
PRODUCTS = [
    "Notebook", "Monitor", "Keyboard", "Mouse", "Headset",
    "Webcam", "SSD", "RAM", "GPU", "CPU"
]

# Product to category mapping
CATEGORIES = {
    "Notebook": "Computers",
    "Monitor": "Displays",
    "Keyboard": "Peripherals",
    "Mouse": "Peripherals",
    "Headset": "Audio",
    "Webcam": "Peripherals",
    "SSD": "Storage",
    "RAM": "Memory",
    "GPU": "Components",
    "CPU": "Components"
}

# Available sales regions
REGIONS = ["South", "Southeast", "Northeast", "North", "Midwest"]

# Simulated customer names
CUSTOMER_NAMES = [
    "Ana Silva", "Bruno Santos", "Carla Oliveira", "Diego Souza", "Fernanda Lima",
    "Gabriel Costa", "Helena Alves", "Igor Ferreira", "Julia Rodrigues", "Lucas Pereira",
    "Mariana Nascimento", "Nicolas Barbosa", "Patricia Gomes", "Rafael Martins", "Sofia Carvalho"
]

# Valid order statuses
VALID_STATUSES = ["completed", "pending", "processing"]

# Base price per product
UNIT_PRICES = {
    "Notebook": 3500.00,
    "Monitor": 1200.00,
    "Keyboard": 250.00,
    "Mouse": 150.00,
    "Headset": 400.00,
    "Webcam": 300.00,
    "SSD": 450.00,
    "RAM": 350.00,
    "GPU": 4000.00,
    "CPU": 2500.00
}


def generate(num_orders: int) -> list[dict]:
    """
    Generate a list of simulated sales orders with intentional data quality issues.

    Args:
        num_orders: Number of orders to generate

    Returns:
        List of order dictionaries
    """
    orders_list = []

    for i in range(num_orders):
        # Generate a random date within the last 90 days
        days_ago = random.randint(0, 90)
        order_date = datetime.now() - timedelta(days=days_ago)

        # Pick a random product for this order
        product = random.choice(PRODUCTS)

        # Intentional error: 5% chance of invalid status
        status = random.choice(VALID_STATUSES)
        if random.random() < 0.05:
            status = "cancelled_invalid"

        # Intentional error: 8% chance of null unit_price
        unit_price = UNIT_PRICES[product]
        if random.random() < 0.08:
            unit_price = np.nan

        # Intentional error: 5% chance of negative quantity
        quantity = random.randint(1, 10)
        if random.random() < 0.05:
            quantity = -quantity

        order = {
            "order_id":      i,
            "customer_name": random.choice(CUSTOMER_NAMES),
            "product":       product,
            "category":      CATEGORIES[product],
            "quantity":      quantity,
            "unit_price":    unit_price,
            "region":        random.choice(REGIONS),
            "status":        status,
            "order_date":    order_date,
        }

        orders_list.append(order)

    # Intentional error: duplicate last 3 orders to simulate real-world duplicates
    orders_list += orders_list[-3:]

    return orders_list


def bronze_save(orders_list: list) -> Path:
    # Ensure the bronze folder exists before saving
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)

    # Convert list of dicts to DataFrame
    df = pd.DataFrame(orders_list)

    # Build filename with today's date for traceability
    post_date = datetime.now().strftime("%Y-%m-%d")
    file_name = f"sales_raw_{post_date}.csv"

    # Build the full file path using pathlib
    path = BRONZE_PATH / file_name

    # Save as CSV without the pandas index column
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"Bronze CSV saved: {path}")
    return path


def run():
    # Entry point — generates data and saves to bronze layer
    print("Generating sales data...")
    orders_list = generate(500)
    bronze_save(orders_list)
    print("Bronze layer complete!")


if __name__ == "__main__":
    run()