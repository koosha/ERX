#!/usr/bin/env python3
"""
Test Entity Resolution on Small Sample Files
Runs entity resolution on sample_customer.csv and sample_trnx.csv
"""

import sys
import os
import logging
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from core.entity_resolution import EntityResolver

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_entity_resolution():
    """Test entity resolution on small sample files"""
    
    # File paths
    customer_data_path = "data/sample_customer.csv"
    transaction_data_path = "data/sample_trnx.csv"
    
    # Check if files exist
    if not os.path.exists(customer_data_path):
        logger.error(f"Customer data not found at {customer_data_path}")
        return
    
    if not os.path.exists(transaction_data_path):
        logger.error(f"Transaction data not found at {transaction_data_path}")
        return
    
    try:
        # Load customer data
        logger.info("Loading customer data...")
        customer_df = pd.read_csv(customer_data_path)
        customers = customer_df.to_dict('records')
        logger.info(f"Loaded {len(customers)} customer records")
        
        # Display sample customer data
        print("\n" + "="*60)
        print("SAMPLE CUSTOMER DATA")
        print("="*60)
        print(customer_df.to_string(index=False))
        
        # Load transaction data
        logger.info("Loading transaction data...")
        transaction_df = pd.read_csv(transaction_data_path)
        transactions = transaction_df.to_dict('records')
        logger.info(f"Loaded {len(transactions)} transaction records")
        
        # Display sample transaction data
        print("\n" + "="*60)
        print("SAMPLE TRANSACTION DATA")
        print("="*60)
        # Show key columns for readability
        key_columns = ['transaction_id', 'originator_name', 'beneficiary_name', 'transaction_amount', 'currency']
        print(transaction_df[key_columns].to_string(index=False))
        
        # Initialize entity resolver
        logger.info("Initializing Entity Resolver...")
        resolver = EntityResolver()
        
        # Run entity resolution
        logger.info("Running entity resolution...")
        entities = resolver.resolve_entities(customers)
        
        # Display results
        print("\n" + "="*60)
        print("ENTITY RESOLUTION RESULTS")
        print("="*60)
        
        for i, entity in enumerate(entities, 1):
            print(f"\nEntity {i}: {entity['entity_id']}")
            print(f"  Type: {entity['entity_type']}")
            print(f"  Primary Name: {entity['primary_name']}")
            print(f"  Primary Email: {entity['primary_email']}")
            print(f"  Primary Phone: {entity['primary_phone']}")
            print(f"  PEP Status: {entity['pep_ind']}")
            print(f"  Confidence: {entity['confidence']:.3f}")
            print(f"  Record Count: {entity['record_count']}")
            print(f"  Sources: {', '.join(entity['sources'])}")
            print(f"  Records:")
            for j, record in enumerate(entity['records'], 1):
                print(f"    {j}. {record['name']} | {record['email']} | {record['phone']} | {record['source_system']}")
        
        # Get and display summary
        summary = resolver.get_entity_summary()
        print("\n" + "="*60)
        print("ENTITY RESOLUTION SUMMARY")
        print("="*60)
        print(f"Total entities: {summary['total_entities']}")
        print(f"Individual entities: {summary['individual_entities']}")
        print(f"Business entities: {summary['business_entities']}")
        print(f"PEP entities: {summary['pep_entities']}")
        print(f"Average confidence: {summary['avg_confidence']:.3f}")
        print(f"Average records per entity: {summary['avg_records_per_entity']:.2f}")
        
        # Create entity mapping for transactions
        print("\n" + "="*60)
        print("ENTITY MAPPING FOR TRANSACTIONS")
        print("="*60)
        
        entity_mapping = {}
        for entity in entities:
            for record in entity['records']:
                customer_name = record.get('name', '')
                if customer_name:
                    entity_mapping[customer_name] = entity['entity_id']
        
        print("Customer Name -> Entity ID Mapping:")
        for customer_name, entity_id in entity_mapping.items():
            print(f"  {customer_name} -> {entity_id}")
        
        # Analyze transaction-entity relationships
        print("\n" + "="*60)
        print("TRANSACTION-ENTITY ANALYSIS")
        print("="*60)
        
        transaction_analysis = []
        for txn in transactions:
            originator_name = txn['originator_name']
            beneficiary_name = txn['beneficiary_name']
            
            originator_entity = entity_mapping.get(originator_name, 'Unknown')
            beneficiary_entity = entity_mapping.get(beneficiary_name, 'Unknown')
            
            transaction_analysis.append({
                'transaction_id': txn['transaction_id'],
                'originator_name': originator_name,
                'originator_entity': originator_entity,
                'beneficiary_name': beneficiary_name,
                'beneficiary_entity': beneficiary_entity,
                'amount': txn['transaction_amount'],
                'currency': txn['currency']
            })
        
        # Display transaction analysis
        analysis_df = pd.DataFrame(transaction_analysis)
        print(analysis_df.to_string(index=False))
        
        # Save results
        logger.info("Saving results...")
        os.makedirs('output', exist_ok=True)
        
        # Save entities
        resolver.save_entities_to_csv('output/test_entities.csv')
        
        # Save entity mapping
        import json
        with open('output/test_entity_mapping.json', 'w') as f:
            json.dump(entity_mapping, f, indent=2)
        
        # Save transaction analysis
        analysis_df.to_csv('output/test_transaction_analysis.csv', index=False)
        
        print("\n" + "="*60)
        print("OUTPUT FILES SAVED")
        print("="*60)
        print("  - output/test_entities.csv: Resolved entities")
        print("  - output/test_entity_mapping.json: Entity mapping")
        print("  - output/test_transaction_analysis.csv: Transaction analysis")
        
        # Show expected entity groupings
        print("\n" + "="*60)
        print("EXPECTED ENTITY GROUPINGS")
        print("="*60)
        print("Based on the data, we expect these groupings:")
        print("1. John Smith + J. Smith (same person, different name formats)")
        print("2. Mary Johnson + Mary J Johnson (same person, different name formats)")
        print("3. Bob Williams + Robert Williams (same person, different name formats)")
        print("4. Mike Johnson + Michael Johnson (same person, different name formats)")
        print("5. Individual entities for: Susan Davis, Lisa Brown")
        
        logger.info("Entity resolution test completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during entity resolution test: {e}")
        raise

if __name__ == "__main__":
    test_entity_resolution() 