#!/usr/bin/env python3
"""
Sample Data Generator for AML Entity Resolution
Generates realistic sample data with proper entity relationships and overlaps
"""

import csv
import random
import datetime
from faker import Faker
import pandas as pd

# Initialize Faker with multiple locales for global data
fake = Faker(['en_US', 'en_GB', 'de_DE', 'fr_FR', 'es_ES', 'it_IT', 'pt_BR', 'ja_JP', 'zh_CN', 'ru_RU'])

# Global configuration
CUSTOMER_COUNT = 500000
ORBIS_COUNT = 100000
WORLDCHECK_COUNT = 100000
TRANSACTION_COUNT = 1000000

# Overlap percentages
CUSTOMER_ORBIS_OVERLAP = 0.40  # 40% of customers in Orbis
CUSTOMER_WORLDCHECK_OVERLAP = 0.10  # 10% of customers in WorldCheck
SUSPICIOUS_TRANSACTION_PERCENT = 0.05  # 5% suspicious transactions

# Country codes for global distribution
COUNTRIES = ['US', 'GB', 'DE', 'FR', 'ES', 'IT', 'CA', 'AU', 'JP', 'CN', 'BR', 'MX', 'IN', 'RU', 'KR', 'NL', 'SE', 'CH', 'NO', 'DK']

# Transaction types
TRANSACTION_TYPES = ['Wire Transfer', 'ACH Transfer', 'SWIFT Transfer', 'SEPA Transfer', 'CHAPS Transfer']

# Business categories
BUSINESS_CATEGORIES = ['Technology', 'Manufacturing', 'Financial Services', 'Healthcare', 'Retail', 'Real Estate', 'Consulting', 'Transportation', 'Energy', 'Media']

# Risk categories for WorldCheck
RISK_CATEGORIES = ['High', 'Medium', 'Low']
LIST_TYPES = ['Sanctions', 'PEP', 'Adverse Media', 'Reputational Risk']

def generate_name_variations(base_name):
    """Generate name variations for entity resolution testing"""
    variations = [base_name]
    
    # Split name into parts
    parts = base_name.split()
    if len(parts) >= 2:
        # Initial + last name
        variations.append(f"{parts[0][0]}. {parts[-1]}")
        # First + middle initial + last
        if len(parts) > 2:
            variations.append(f"{parts[0]} {parts[1][0]}. {parts[-1]}")
    
    return variations

def generate_address_variations(base_address):
    """Generate address variations"""
    variations = [base_address]
    
    # Street vs St
    if " Street" in base_address:
        variations.append(base_address.replace(" Street", " St"))
    elif " St" in base_address:
        variations.append(base_address.replace(" St", " Street"))
    
    # Avenue vs Ave
    if " Avenue" in base_address:
        variations.append(base_address.replace(" Avenue", " Ave"))
    elif " Ave" in base_address:
        variations.append(base_address.replace(" Ave", " Avenue"))
    
    return variations

def generate_customers():
    """Generate 500K customer records"""
    print("Generating customer data...")
    
    customers = []
    customer_names = set()
    
    for i in range(CUSTOMER_COUNT):
        # Generate base customer
        if random.random() < 0.7:  # 70% individuals, 30% companies
            # Individual
            first_name = fake.first_name()
            last_name = fake.last_name()
            base_name = f"{first_name} {last_name}"
            
            # Create variations
            name_variations = generate_name_variations(base_name)
            name = random.choice(name_variations)
            
            email = fake.email()
            phone = fake.phone_number()
            address = fake.address()
            
        else:
            # Company
            company_suffixes = ['Inc', 'Corp', 'Ltd', 'LLC', 'Company', 'Corporation', 'Limited']
            base_name = fake.company()
            suffix = random.choice(company_suffixes)
            name = f"{base_name} {suffix}"
            
            email = fake.company_email()
            phone = fake.phone_number()
            address = fake.address()
        
        # Ensure unique names
        if name in customer_names:
            name = f"{name} {random.randint(1, 999)}"
        customer_names.add(name)
        
        customer = {
            'id': i + 1,
            'name': name,
            'email': email,
            'phone': phone,
            'address': address,
            'source_system': f'system_{random.randint(1, 5)}'
        }
        customers.append(customer)
    
    return customers

def generate_orbis(customers):
    """Generate 100K Orbis records with 40% overlap with customers"""
    print("Generating Orbis data...")
    
    orbis_records = []
    customer_overlap_count = int(ORBIS_COUNT * CUSTOMER_ORBIS_OVERLAP)
    
    # Select customers for overlap
    overlap_customers = random.sample(customers, customer_overlap_count)
    
    # Generate Orbis records with customer overlap
    for i in range(ORBIS_COUNT):
        if i < customer_overlap_count:
            # Use customer data with variations
            customer = overlap_customers[i]
            name_variations = generate_name_variations(customer['name'])
            name = random.choice(name_variations)
            
            # Add company suffixes if it's an individual
            if ' ' in name and not any(suffix in name for suffix in ['Inc', 'Corp', 'Ltd', 'LLC', 'Company']):
                suffixes = ['Inc', 'Corp', 'Ltd', 'LLC']
                name = f"{name} {random.choice(suffixes)}"
        else:
            # Generate new company
            name = fake.company()
            suffixes = ['Inc', 'Corp', 'Ltd', 'LLC', 'Company', 'Corporation', 'Limited']
            name = f"{name} {random.choice(suffixes)}"
        
        country = random.choice(COUNTRIES)
        
        orbis_record = {
            'orbis_id': f'ORB{i+1:06d}',
            'company_name': name,
            'legal_name': name,
            'country_code': country,
            'country_name': fake.country(),
            'incorporation_date': fake.date_between(start_date='-20y', end_date='today').strftime('%Y-%m-%d'),
            'status': 'Active',
            'industry_code': random.randint(1000, 9999),
            'industry_name': random.choice(BUSINESS_CATEGORIES),
            'ultimate_parent_name': fake.company() if random.random() < 0.3 else 'None',
            'ultimate_parent_country': country,
            'beneficial_owners': f"{fake.name()} ({random.randint(20, 80)}%)",
            'share_capital': random.randint(100000, 50000000),
            'revenue': random.randint(1000000, 100000000),
            'employees': random.randint(10, 10000),
            'risk_score': random.choice(['Low', 'Medium', 'High']),
            'last_updated': fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d')
        }
        orbis_records.append(orbis_record)
    
    return orbis_records

def generate_worldcheck(customers):
    """Generate 100K WorldCheck records with 10% overlap with customers"""
    print("Generating WorldCheck data...")
    
    worldcheck_records = []
    customer_overlap_count = int(WORLDCHECK_COUNT * CUSTOMER_WORLDCHECK_OVERLAP)
    
    # Select customers for overlap
    overlap_customers = random.sample(customers, customer_overlap_count)
    
    # Generate WorldCheck records with customer overlap
    for i in range(WORLDCHECK_COUNT):
        if i < customer_overlap_count:
            # Use customer data with variations
            customer = overlap_customers[i]
            name_variations = generate_name_variations(customer['name'])
            name = random.choice(name_variations)
        else:
            # Generate new entity
            if random.random() < 0.8:  # 80% individuals, 20% entities
                name = fake.name()
            else:
                name = fake.company()
        
        country = random.choice(COUNTRIES)
        risk_category = random.choice(RISK_CATEGORIES)
        list_type = random.choice(LIST_TYPES)
        
        worldcheck_record = {
            'wc_id': f'WC{i+1:06d}',
            'full_name': name,
            'aliases': f"{name.split()[0]}, {name.split()[-1]}" if ' ' in name else name,
            'date_of_birth': fake.date_between(start_date='-80y', end_date='-18y').strftime('%Y-%m-%d') if random.random() < 0.8 else '',
            'place_of_birth': fake.city(),
            'nationality': country,
            'passport_numbers': f"{country}{random.randint(100000000, 999999999)}",
            'id_numbers': f"ID: {random.randint(100000000, 999999999)}",
            'entity_type': 'Individual' if random.random() < 0.8 else 'Entity',
            'risk_category': risk_category,
            'list_type': list_type,
            'list_name': f"{list_type} List",
            'list_country': country,
            'reason_for_listing': fake.sentence(),
            'listing_date': fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d'),
            'last_updated': fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
            'status': 'Active',
            'notes': fake.text(max_nb_chars=100)
        }
        worldcheck_records.append(worldcheck_record)
    
    return worldcheck_records

def generate_suspicious_patterns():
    """Generate suspicious transaction patterns"""
    patterns = []
    
    # Structuring pattern: multiple transactions just under $10,000
    for i in range(100):
        base_time = fake.date_time_between(start_date='-30d', end_date='today')
        for j in range(random.randint(3, 8)):
            patterns.append({
                'amount': random.randint(9000, 9999),
                'time': base_time + datetime.timedelta(hours=j),
                'pattern_type': 'structuring'
            })
    
    # Round amounts pattern
    for i in range(200):
        patterns.append({
            'amount': random.choice([50000, 100000, 250000, 500000, 1000000]),
            'time': fake.date_time_between(start_date='-30d', end_date='today'),
            'pattern_type': 'round_amount'
        })
    
    # High-frequency pattern
    for i in range(150):
        base_time = fake.date_time_between(start_date='-7d', end_date='today')
        for j in range(random.randint(5, 15)):
            patterns.append({
                'amount': random.randint(100, 1000),
                'time': base_time + datetime.timedelta(minutes=j*30),
                'pattern_type': 'high_frequency'
            })
    
    return patterns

def generate_transactions(customers, orbis_records, worldcheck_records):
    """Generate 1M transactions with all customers appearing"""
    print("Generating transaction data...")
    
    transactions = []
    suspicious_patterns = generate_suspicious_patterns()
    pattern_index = 0
    
    # Ensure all customers appear in transactions
    customer_transaction_count = len(customers)
    remaining_transactions = TRANSACTION_COUNT - customer_transaction_count
    
    # Generate transactions for all customers
    for i, customer in enumerate(customers):
        # Generate 1-3 transactions per customer
        num_transactions = random.randint(1, 3)
        for j in range(num_transactions):
            transaction = generate_single_transaction(customer, i, j, suspicious_patterns, pattern_index)
            transactions.append(transaction)
            pattern_index += 1
    
    # Generate remaining transactions
    for i in range(remaining_transactions):
        customer = random.choice(customers)
        transaction = generate_single_transaction(customer, len(transactions), 0, suspicious_patterns, pattern_index)
        transactions.append(transaction)
        pattern_index += 1
    
    return transactions

def generate_single_transaction(customer, transaction_num, sub_num, suspicious_patterns, pattern_index):
    """Generate a single transaction"""
    
    # Determine if this should be a suspicious transaction
    is_suspicious = pattern_index < len(suspicious_patterns)
    
    if is_suspicious:
        pattern = suspicious_patterns[pattern_index]
        amount = pattern['amount']
        transaction_time = pattern['time']
    else:
        amount = random.randint(5, 8000000)
        transaction_time = fake.date_time_between(start_date='-1y', end_date='today')
    
    # Generate transaction data
    transaction = {
        'transaction_id': f'TXN{transaction_num+1:07d}',
        'record_id': random.randint(100000, 999999),
        'type': random.choice(['XA', 'IN']),
        'party_type': random.choice(['Originator', 'Beneficiary']),
        'transaction_reference': f'REF{transaction_time.strftime("%Y%m%d")}{random.randint(1, 9999):04d}',
        'currency': random.choice(['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD']),
        'transaction_type': random.choice(TRANSACTION_TYPES),
        'transaction_status': random.choice(['Completed', 'Pending', 'Failed']),
        'transaction_amount': amount,
        'transaction_description': fake.sentence(),
        'transaction_category': random.choice(BUSINESS_CATEGORIES),
        'exchange_rate': round(random.uniform(0.5, 2.0), 2),
        'fees': round(amount * random.uniform(0.001, 0.01), 2),
        'originator_name': customer['name'],
        'originator_account': f"{random.choice(COUNTRIES)}{random.randint(100000000, 999999999)}",
        'originator_institution': fake.company(),
        'originator_country': random.choice(COUNTRIES),
        'originator_address': customer['address'],
        'beneficiary_name': fake.name() if random.random() < 0.7 else fake.company(),
        'beneficiary_account': f"{random.choice(COUNTRIES)}{random.randint(100000000, 999999999)}",
        'beneficiary_institution': fake.company(),
        'beneficiary_country': random.choice(COUNTRIES),
        'beneficiary_address': fake.address(),
        'transaction_date': transaction_time.strftime('%Y-%m-%d %H:%M:%S'),
        'credit_debit': 'CREDIT' if random.random() < 0.5 else 'DEBIT',
        'originator_institute': fake.company() if random.random() < 0.7 else '',
        'beneficiary_institute': fake.company() if random.random() < 0.7 else '',
        'TP_originator': fake.name() if random.random() < 0.3 else '',
        'TP_beneficiary': fake.name() if random.random() < 0.3 else ''
    }
    
    return transaction

def save_to_csv(data, filename, fieldnames):
    """Save data to CSV file"""
    print(f"Saving {len(data)} records to {filename}...")
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def main():
    """Main function to generate all sample data"""
    print("Starting sample data generation...")
    
    # Generate customers first
    customers = generate_customers()
    save_to_csv(customers, 'data/sample_customer_large.csv', 
                ['id', 'name', 'email', 'phone', 'address', 'source_system'])
    
    # Generate Orbis with customer overlap
    orbis_records = generate_orbis(customers)
    save_to_csv(orbis_records, 'data/sample_orbis_large.csv',
                ['orbis_id', 'company_name', 'legal_name', 'country_code', 'country_name', 
                 'incorporation_date', 'status', 'industry_code', 'industry_name', 
                 'ultimate_parent_name', 'ultimate_parent_country', 'beneficial_owners',
                 'share_capital', 'revenue', 'employees', 'risk_score', 'last_updated'])
    
    # Generate WorldCheck with customer overlap
    worldcheck_records = generate_worldcheck(customers)
    save_to_csv(worldcheck_records, 'data/sample_wc_large.csv',
                ['wc_id', 'full_name', 'aliases', 'date_of_birth', 'place_of_birth', 
                 'nationality', 'passport_numbers', 'id_numbers', 'entity_type', 
                 'risk_category', 'list_type', 'list_name', 'list_country', 
                 'reason_for_listing', 'listing_date', 'last_updated', 'status', 'notes'])
    
    # Generate transactions
    transactions = generate_transactions(customers, orbis_records, worldcheck_records)
    save_to_csv(transactions, 'data/sample_trnx_large.csv',
                ['transaction_id', 'record_id', 'type', 'party_type', 'transaction_reference',
                 'currency', 'transaction_type', 'transaction_status', 'transaction_amount',
                 'transaction_description', 'transaction_category', 'exchange_rate', 'fees',
                 'originator_name', 'originator_account', 'originator_institution', 
                 'originator_country', 'originator_address', 'beneficiary_name', 
                 'beneficiary_account', 'beneficiary_institution', 'beneficiary_country',
                 'beneficiary_address', 'transaction_date', 'credit_debit', 
                 'originator_institute', 'beneficiary_institute', 'TP_originator', 'TP_beneficiary'])
    
    print("Sample data generation completed!")
    print(f"Generated {len(customers)} customers")
    print(f"Generated {len(orbis_records)} Orbis records")
    print(f"Generated {len(worldcheck_records)} WorldCheck records")
    print(f"Generated {len(transactions)} transactions")

if __name__ == "__main__":
    main() 