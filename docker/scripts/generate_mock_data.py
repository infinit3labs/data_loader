#!/usr/bin/env python3
"""
Mock data generator for testing the Databricks Data Loader.

This script generates realistic test data in various formats (Parquet, JSON, CSV)
to simulate different data loading scenarios including SCD2 and append patterns.
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
import random
from faker import Faker

fake = Faker()
Faker.seed(42)  # For reproducible data
np.random.seed(42)
random.seed(42)


class MockDataGenerator:
    """Generates mock data for testing data loading strategies."""
    
    def __init__(self, data_root: str = "/app/data"):
        self.data_root = Path(data_root)
        self.raw_path = self.data_root / "raw"
        self.processed_path = self.data_root / "processed"
        
        # Create directories
        self.raw_path.mkdir(parents=True, exist_ok=True)
        self.processed_path.mkdir(parents=True, exist_ok=True)
        
    def generate_customer_data(self, num_records: int = 1000, num_batches: int = 3) -> None:
        """Generate customer data for SCD2 testing."""
        print(f"Generating customer data: {num_records} records in {num_batches} batches")
        
        customer_dir = self.raw_path / "customers"
        customer_dir.mkdir(exist_ok=True)
        
        base_customers = []
        
        # Generate initial customer data
        for i in range(num_records):
            customer = {
                'customer_id': f'CUST_{i:06d}',
                'name': fake.name(),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'address': fake.address().replace('\n', ', '),
                'city': fake.city(),
                'state': fake.state_abbr(),
                'zip_code': fake.zipcode(),
                'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=80),
                'customer_type': random.choice(['PREMIUM', 'STANDARD', 'BASIC']),
                'created_date': fake.date_between(start_date='-2y', end_date='today'),
                'last_updated': datetime.now(),
                'date_partition': datetime.now().strftime('%Y%m%d')
            }
            base_customers.append(customer)
        
        # Save initial batch
        df_initial = pd.DataFrame(base_customers)
        initial_path = customer_dir / f"customers_batch_001_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df_initial.to_parquet(initial_path, index=False)
        print(f"Saved initial customer batch: {initial_path}")
        
        # Generate subsequent batches with updates (SCD2 scenario)
        for batch in range(2, num_batches + 1):
            updated_customers = []
            
            # Update random subset of existing customers
            update_count = random.randint(50, 200)
            customers_to_update = random.sample(base_customers, min(update_count, len(base_customers)))
            
            for customer in customers_to_update:
                updated_customer = customer.copy()
                
                # Randomly update some fields (tracking columns)
                if random.choice([True, False]):
                    updated_customer['email'] = fake.email()
                if random.choice([True, False]):
                    updated_customer['address'] = fake.address().replace('\n', ', ')
                if random.choice([True, False]):
                    updated_customer['phone'] = fake.phone_number()
                if random.choice([True, False]):
                    updated_customer['customer_type'] = random.choice(['PREMIUM', 'STANDARD', 'BASIC'])
                
                updated_customer['last_updated'] = datetime.now() + timedelta(days=batch)
                updated_customer['date_partition'] = (datetime.now() + timedelta(days=batch)).strftime('%Y%m%d')
                updated_customers.append(updated_customer)
            
            # Add some new customers
            new_customer_count = random.randint(20, 100)
            for i in range(new_customer_count):
                customer_id = f'CUST_{len(base_customers) + i:06d}'
                new_customer = {
                    'customer_id': customer_id,
                    'name': fake.name(),
                    'email': fake.email(),
                    'phone': fake.phone_number(),
                    'address': fake.address().replace('\n', ', '),
                    'city': fake.city(),
                    'state': fake.state_abbr(),
                    'zip_code': fake.zipcode(),
                    'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=80),
                    'customer_type': random.choice(['PREMIUM', 'STANDARD', 'BASIC']),
                    'created_date': datetime.now() + timedelta(days=batch),
                    'last_updated': datetime.now() + timedelta(days=batch),
                    'date_partition': (datetime.now() + timedelta(days=batch)).strftime('%Y%m%d')
                }
                updated_customers.append(new_customer)
                base_customers.append(new_customer)
            
            # Save batch
            if updated_customers:
                df_batch = pd.DataFrame(updated_customers)
                batch_path = customer_dir / f"customers_batch_{batch:03d}_{(datetime.now() + timedelta(days=batch)).strftime('%Y%m%d_%H%M%S')}.parquet"
                df_batch.to_parquet(batch_path, index=False)
                print(f"Saved customer batch {batch}: {batch_path} ({len(updated_customers)} records)")
    
    def generate_transaction_data(self, num_records: int = 5000, num_files: int = 5) -> None:
        """Generate transaction data for append strategy testing."""
        print(f"Generating transaction data: {num_records} records in {num_files} files")
        
        transaction_dir = self.raw_path / "transactions"
        transaction_dir.mkdir(exist_ok=True)
        
        records_per_file = num_records // num_files
        
        # Generate base customer IDs for transactions
        customer_ids = [f'CUST_{i:06d}' for i in range(1000)]
        
        for file_num in range(num_files):
            transactions = []
            file_date = datetime.now() - timedelta(days=num_files - file_num)
            
            for i in range(records_per_file):
                transaction = {
                    'transaction_id': f'TXN_{file_num:02d}_{i:06d}',
                    'customer_id': random.choice(customer_ids),
                    'transaction_date': file_date + timedelta(
                        hours=random.randint(0, 23),
                        minutes=random.randint(0, 59),
                        seconds=random.randint(0, 59)
                    ),
                    'amount': round(random.uniform(10.0, 1000.0), 2),
                    'currency': 'USD',
                    'transaction_type': random.choice(['PURCHASE', 'REFUND', 'TRANSFER', 'PAYMENT']),
                    'status': random.choice(['COMPLETED', 'PENDING', 'FAILED']),
                    'merchant_id': f'MERCH_{random.randint(1, 100):03d}',
                    'category': random.choice(['GROCERY', 'RESTAURANT', 'GAS', 'RETAIL', 'ONLINE']),
                    'created_timestamp': datetime.now(),
                    'updated_timestamp': datetime.now()
                }
                transactions.append(transaction)
            
            # Save file
            df = pd.DataFrame(transactions)
            file_path = transaction_dir / f"transactions_{file_date.strftime('%Y%m%d')}_{file_num:02d}.parquet"
            df.to_parquet(file_path, index=False)
            print(f"Saved transaction file: {file_path} ({len(transactions)} records)")
    
    def generate_product_data(self, num_records: int = 500) -> None:
        """Generate product reference data."""
        print(f"Generating product data: {num_records} records")
        
        product_dir = self.raw_path / "products"
        product_dir.mkdir(exist_ok=True)
        
        products = []
        categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Food & Beverage']
        
        for i in range(num_records):
            product = {
                'product_id': f'PROD_{i:06d}',
                'product_name': fake.catch_phrase(),
                'category': random.choice(categories),
                'brand': fake.company(),
                'price': round(random.uniform(5.0, 500.0), 2),
                'description': fake.text(max_nb_chars=200),
                'sku': fake.bothify(text='SKU-####-????'),
                'weight_kg': round(random.uniform(0.1, 10.0), 2),
                'dimensions': f"{random.randint(10, 100)}x{random.randint(10, 100)}x{random.randint(5, 50)} cm",
                'in_stock': random.choice([True, False]),
                'stock_quantity': random.randint(0, 1000),
                'created_date': fake.date_between(start_date='-1y', end_date='today'),
                'last_updated': datetime.now()
            }
            products.append(product)
        
        df = pd.DataFrame(products)
        file_path = product_dir / f"products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.to_parquet(file_path, index=False)
        print(f"Saved product file: {file_path}")
    
    def generate_error_data(self) -> None:
        """Generate some files with data quality issues for testing error handling."""
        print("Generating error data for testing...")
        
        error_dir = self.raw_path / "error_test"
        error_dir.mkdir(exist_ok=True)
        
        # Empty file
        empty_path = error_dir / "empty_file.parquet"
        pd.DataFrame().to_parquet(empty_path, index=False)
        print(f"Created empty file: {empty_path}")
        
        # File with schema mismatch
        schema_mismatch = [
            {'id': 1, 'name': 'test', 'extra_column': 'should_not_exist'},
            {'id': 2, 'name': 'test2', 'extra_column': 'also_should_not_exist'}
        ]
        mismatch_path = error_dir / "schema_mismatch.parquet"
        pd.DataFrame(schema_mismatch).to_parquet(mismatch_path, index=False)
        print(f"Created schema mismatch file: {mismatch_path}")
        
        # File with null values in critical columns
        null_data = [
            {'customer_id': None, 'name': 'John Doe', 'email': 'john@example.com'},
            {'customer_id': 'CUST_001', 'name': None, 'email': 'jane@example.com'},
            {'customer_id': 'CUST_002', 'name': 'Bob Smith', 'email': None}
        ]
        null_path = error_dir / "null_values.parquet"
        pd.DataFrame(null_data).to_parquet(null_path, index=False)
        print(f"Created null values file: {null_path}")


def main():
    """Generate all test data."""
    print("Starting mock data generation...")
    
    generator = MockDataGenerator()
    
    # Generate different types of test data
    generator.generate_customer_data(num_records=1000, num_batches=3)
    generator.generate_transaction_data(num_records=5000, num_files=5)
    generator.generate_product_data(num_records=500)
    generator.generate_error_data()
    
    print("Mock data generation completed!")
    print(f"Data saved to: {generator.data_root}")


if __name__ == "__main__":
    main()
