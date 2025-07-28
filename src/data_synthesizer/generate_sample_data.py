#!/usr/bin/env python3
"""
Sample Data Generator for AML Entity Resolution
Generates realistic sample data for the three source systems
"""

import csv
import random
import datetime
from faker import Faker
import pandas as pd

# Initialize Faker with multiple locales for global data
fake = Faker(['en_US', 'en_GB', 'de_DE', 'fr_FR', 'es_ES', 'it_IT', 'pt_BR', 'ja_JP', 'zh_CN', 'ru_RU'])

# Global configuration
ORBIS_COUNT = 100000
WORLDCHECK_COUNT = 100000
TRANSACTION_COUNT = 1000000

# Overlap percentages
ORBIS_WORLDCHECK_OVERLAP = 0.15  # 15% overlap between Orbis and WorldCheck

def generate_optional_contact_info():
    """Generate optional contact info with 50% probability of being empty"""
    has_info = random.choice([True, False])
    
    if has_info:
        # Randomly choose which fields to populate
        fields = ['email', 'phone', 'address']
        populated_fields = random.sample(fields, random.randint(1, len(fields)))
        
        email = fake.email() if 'email' in populated_fields else ""
        phone = fake.phone_number() if 'phone' in populated_fields else ""
        address = fake.address() if 'address' in populated_fields else ""
        
        return email, phone, address
    else:
        return "", "", ""

def generate_orbis_data():
    """Generate Orbis company data"""
    print("Generating Orbis data...")
    
    orbis_records = []
    
    for i in range(ORBIS_COUNT):
        # Generate optional contact info
        email, phone, address = generate_optional_contact_info()
        
        record = {
            'company_id': f"ORB{i+1:06d}",
            'company_name': fake.company(),
            'legal_form': random.choice(['Ltd', 'LLC', 'Corp', 'Inc', 'GmbH', 'SA', 'NV']),
            'incorporation_date': fake.date_between(start_date='-20y', end_date='-1y').strftime('%Y-%m-%d'),
            'country_name': fake.country(),
            'city': fake.city(),
            'address': address,
            'email': email,
            'phone': phone,
            'industry': random.choice(['Technology', 'Finance', 'Manufacturing', 'Healthcare', 'Retail', 'Energy']),
            'employees': random.randint(1, 10000),
            'revenue': random.randint(100000, 1000000000),
            'status': random.choice(['Active', 'Inactive', 'Dissolved']),
            'registration_number': fake.uuid4(),
            'tax_id': fake.uuid4(),
            'website': fake.url(),
            'parent_company': fake.company() if random.choice([True, False]) else "",
            'subsidiaries': random.randint(0, 5),
            'directors': fake.name() + ", " + fake.name() if random.choice([True, False]) else "",
            'shareholders': fake.name() + ", " + fake.name() if random.choice([True, False]) else ""
        }
        orbis_records.append(record)
    
    return orbis_records

def generate_worldcheck_data():
    """Generate WorldCheck individual/entity data"""
    print("Generating WorldCheck data...")
    
    worldcheck_records = []
    
    for i in range(WORLDCHECK_COUNT):
        # Generate optional contact info
        email, phone, address = generate_optional_contact_info()
        
        record = {
            'wc_id': f"WC{i+1:06d}",
            'full_name': fake.name(),
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d'),
            'nationality': fake.country(),
            'passport_number': fake.uuid4(),
            'address': address,
            'email': email,
            'phone': phone,
            'risk_level': random.choice(['Low', 'Medium', 'High']),
            'category': random.choice(['PEP', 'Sanctions', 'Adverse Media', 'Regulatory']),
            'source': random.choice(['UN', 'EU', 'US', 'UK', 'Media']),
            'listing_date': fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d'),
            'status': random.choice(['Active', 'Inactive', 'Removed']),
            'aliases': fake.name() + ", " + fake.name() if random.choice([True, False]) else "",
            'citizenship': fake.country(),
            'occupation': fake.job(),
            'employer': fake.company(),
            'reason': fake.text(max_nb_chars=200),
            'last_updated': fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
            'source_url': fake.url()
        }
        worldcheck_records.append(record)
    
    return worldcheck_records

def generate_transaction_data():
    """Generate transaction data with correct schema"""
    print(f"Generating {TRANSACTION_COUNT} transaction records...")
    
    transactions = []
    
    for i in range(TRANSACTION_COUNT):
        # Generate optional contact info for originator
        orig_email, orig_phone, orig_address = generate_optional_contact_info()
        
        # Generate optional contact info for beneficiary  
        ben_email, ben_phone, ben_address = generate_optional_contact_info()
        
        # Generate optional contact info for TP_originator
        tp_orig_email, tp_orig_phone, tp_orig_address = generate_optional_contact_info()
        
        # Generate optional contact info for TP_beneficiary
        tp_ben_email, tp_ben_phone, tp_ben_address = generate_optional_contact_info()
        
        transaction = {
            'transaction_id': f"TXN{i+1:06d}",
            'record_id': f"REC{i+1:06d}",
            'type': random.choice(['SWIFT', 'SEPA', 'ACH', 'WIRE']),
            'party_type': random.choice(['INDIVIDUAL', 'CORPORATE', 'FINANCIAL_INSTITUTION']),
            'transaction_reference': fake.uuid4(),
            'currency': random.choice(['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD']),
            'transaction_date': fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
            'transaction_type': random.choice(['TRANSFER', 'PAYMENT', 'DEPOSIT', 'WITHDRAWAL']),
            'transaction_status': random.choice(['COMPLETED', 'PENDING', 'FAILED', 'CANCELLED']),
            'transaction_amount': round(random.uniform(100, 1000000), 2),
            'transaction_description': fake.text(max_nb_chars=100),
            'transaction_category': random.choice(['BUSINESS', 'PERSONAL', 'INVESTMENT', 'CHARITY']),
            'exchange_rate': round(random.uniform(0.5, 2.0), 4),
            'fees': round(random.uniform(0, 100), 2),
            'credit_debit': random.choice(['CREDIT', 'DEBIT']),
            'originator_account': fake.bban(),
            'originator_address': orig_address,
            'originator_country': fake.country_code(),
            'originator_email': orig_email,
            'originator_institute': fake.company(),
            'originator_institution': fake.company(),
            'originator_name': fake.name(),
            'originator_phone': orig_phone,
            'beneficiary_account': fake.bban(),
            'beneficiary_address': ben_address,
            'beneficiary_country': fake.country_code(),
            'beneficiary_email': ben_email,
            'beneficiary_institute': fake.company(),
            'beneficiary_institution': fake.company(),
            'beneficiary_name': fake.name(),
            'beneficiary_phone': ben_phone,
            'TP_originator_account': fake.bban() if random.choice([True, False]) else "",
            'TP_originator_email': tp_orig_email,
            'TP_originator_name': fake.name() if random.choice([True, False]) else "",
            'TP_originator_phone': tp_orig_phone,
            'TP_originator_address': tp_orig_address,
            'TP_originator_country': fake.country_code() if random.choice([True, False]) else "",
            'TP_beneficiary_account': fake.bban() if random.choice([True, False]) else "",
            'TP_beneficiary_address': tp_ben_address,
            'TP_beneficiary_country': fake.country_code() if random.choice([True, False]) else "",
            'TP_beneficiary_name': fake.name() if random.choice([True, False]) else "",
            'TP_beneficiary_email': tp_ben_email,
            'TP_beneficiary_phone': tp_ben_phone
        }
        
        transactions.append(transaction)
        
        if (i + 1) % 100000 == 0:
            print(f"Generated {i + 1} transaction records...")
    
    return transactions

def main():
    """Main function to generate all sample data"""
    print("Starting sample data generation...")
    
    # Generate Orbis data
    orbis_records = generate_orbis_data()
    df_orbis = pd.DataFrame(orbis_records)
    df_orbis.to_csv('data/sample_orbis_large.csv', index=False)
    print(f"Saving {len(orbis_records)} records to data/sample_orbis_large.csv...")
    
    # Generate WorldCheck data
    worldcheck_records = generate_worldcheck_data()
    df_worldcheck = pd.DataFrame(worldcheck_records)
    df_worldcheck.to_csv('data/sample_wc_large.csv', index=False)
    print(f"Saving {len(worldcheck_records)} records to data/sample_wc_large.csv...")
    
    # Generate transaction data
    transactions = generate_transaction_data()
    df_transactions = pd.DataFrame(transactions)
    df_transactions.to_csv('data/sample_trnx_large.csv', index=False)
    print(f"Saving {len(transactions)} transaction records to data/sample_trnx_large.csv...")
    
    print("Sample data generation completed!")

if __name__ == "__main__":
    main() 