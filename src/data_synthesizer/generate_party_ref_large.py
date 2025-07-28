#!/usr/bin/env python3
"""
Large Party Reference Generator
Generates party_ref_large.csv from the three large source files
"""

import pandas as pd
import json
from typing import List, Dict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_large_source_data():
    """Load data from the large source files"""
    print("Loading large source data...")
    
    # Load transaction data
    trnx_df = pd.read_csv('data/sample_trnx_large.csv')
    print(f"Loaded {len(trnx_df)} transaction records")
    
    # Load Orbis data
    orbis_df = pd.read_csv('data/sample_orbis_large.csv')
    print(f"Loaded {len(orbis_df)} Orbis records")
    
    # Load WorldCheck data
    wc_df = pd.read_csv('data/sample_wc_large.csv')
    print(f"Loaded {len(wc_df)} WorldCheck records")
    
    return trnx_df, orbis_df, wc_df

def extract_parties_from_transactions(trnx_df):
    """Extract parties from transaction data"""
    print("Extracting parties from transaction data...")
    
    parties = []
    
    # Extract originators
    for idx, row in trnx_df.iterrows():
        if pd.notna(row['originator_name']) and row['originator_name'].strip():
            party = {
                'name': row['originator_name'],
                'email': row['originator_email'] if pd.notna(row['originator_email']) else "",
                'phone': row['originator_phone'] if pd.notna(row['originator_phone']) else "",
                'address': row['originator_address'] if pd.notna(row['originator_address']) else "",
                'country': row['originator_country'] if pd.notna(row['originator_country']) else "",
                'accounts_list': [row['originator_account']] if pd.notna(row['originator_account']) else [],
                'source_system': 'trnx',
                'source_index_list': [row['transaction_id']]
            }
            parties.append(party)
    
    # Extract beneficiaries
    for idx, row in trnx_df.iterrows():
        if pd.notna(row['beneficiary_name']) and row['beneficiary_name'].strip():
            party = {
                'name': row['beneficiary_name'],
                'email': row['beneficiary_email'] if pd.notna(row['beneficiary_email']) else "",
                'phone': row['beneficiary_phone'] if pd.notna(row['beneficiary_phone']) else "",
                'address': row['beneficiary_address'] if pd.notna(row['beneficiary_address']) else "",
                'country': row['beneficiary_country'] if pd.notna(row['beneficiary_country']) else "",
                'accounts_list': [row['beneficiary_account']] if pd.notna(row['beneficiary_account']) else [],
                'source_system': 'trnx',
                'source_index_list': [row['transaction_id']]
            }
            parties.append(party)
    
    # Extract TP_originators
    for idx, row in trnx_df.iterrows():
        if pd.notna(row['TP_originator_name']) and row['TP_originator_name'].strip():
            party = {
                'name': row['TP_originator_name'],
                'email': row['TP_originator_email'] if pd.notna(row['TP_originator_email']) else "",
                'phone': row['TP_originator_phone'] if pd.notna(row['TP_originator_phone']) else "",
                'address': row['TP_originator_address'] if pd.notna(row['TP_originator_address']) else "",
                'country': row['TP_originator_country'] if pd.notna(row['TP_originator_country']) else "",
                'accounts_list': [row['TP_originator_account']] if pd.notna(row['TP_originator_account']) else [],
                'source_system': 'trnx',
                'source_index_list': [row['transaction_id']]
            }
            parties.append(party)
    
    # Extract TP_beneficiaries
    for idx, row in trnx_df.iterrows():
        if pd.notna(row['TP_beneficiary_name']) and row['TP_beneficiary_name'].strip():
            party = {
                'name': row['TP_beneficiary_name'],
                'email': row['TP_beneficiary_email'] if pd.notna(row['TP_beneficiary_email']) else "",
                'phone': row['TP_beneficiary_phone'] if pd.notna(row['TP_beneficiary_phone']) else "",
                'address': row['TP_beneficiary_address'] if pd.notna(row['TP_beneficiary_address']) else "",
                'country': row['TP_beneficiary_country'] if pd.notna(row['TP_beneficiary_country']) else "",
                'accounts_list': [row['TP_beneficiary_account']] if pd.notna(row['TP_beneficiary_account']) else [],
                'source_system': 'trnx',
                'source_index_list': [row['transaction_id']]
            }
            parties.append(party)
    
    print(f"Extracted {len(parties)} party records from transactions")
    return parties

def extract_parties_from_orbis(orbis_df):
    """Extract parties from Orbis data"""
    print("Extracting parties from Orbis data...")
    
    parties = []
    
    for idx, row in orbis_df.iterrows():
        if pd.notna(row['company_name']) and row['company_name'].strip():
            party = {
                'name': row['company_name'],
                'email': row['email'] if pd.notna(row['email']) else "",
                'phone': row['phone'] if pd.notna(row['phone']) else "",
                'address': row['address'] if pd.notna(row['address']) else "",
                'country': row['country_name'] if pd.notna(row['country_name']) else "",
                'accounts_list': [],  # No accounts for Orbis
                'source_system': 'orbis',
                'source_index_list': [row['company_id']]
            }
            parties.append(party)
    
    print(f"Extracted {len(parties)} party records from Orbis")
    return parties

def extract_parties_from_worldcheck(wc_df):
    """Extract parties from WorldCheck data"""
    print("Extracting parties from WorldCheck data...")
    
    parties = []
    
    for idx, row in wc_df.iterrows():
        if pd.notna(row['full_name']) and row['full_name'].strip():
            party = {
                'name': row['full_name'],
                'email': row['email'] if pd.notna(row['email']) else "",
                'phone': row['phone'] if pd.notna(row['phone']) else "",
                'address': row['address'] if pd.notna(row['address']) else "",
                'country': row['nationality'] if pd.notna(row['nationality']) else "",
                'accounts_list': [],  # No accounts for WorldCheck
                'source_system': 'WC',
                'source_index_list': [row['wc_id']]
            }
            parties.append(party)
    
    print(f"Extracted {len(parties)} party records from WorldCheck")
    return parties

def consolidate_parties(parties):
    """Consolidate parties by name and source system"""
    print("Consolidating parties...")
    
    # Group by name and source system
    party_groups = {}
    
    for party in parties:
        key = (party['name'].lower().strip(), party['source_system'])
        
        if key not in party_groups:
            party_groups[key] = party
        else:
            # Merge with existing party
            existing = party_groups[key]
            
            # Merge source indices
            existing['source_index_list'].extend(party['source_index_list'])
            
            # Merge accounts (only for trnx)
            if party['source_system'] == 'trnx':
                existing['accounts_list'].extend(party['accounts_list'])
            
            # Use non-empty values for contact info
            if not existing['email'] and party['email']:
                existing['email'] = party['email']
            if not existing['phone'] and party['phone']:
                existing['phone'] = party['phone']
            if not existing['address'] and party['address']:
                existing['address'] = party['address']
            if not existing['country'] and party['country']:
                existing['country'] = party['country']
    
    consolidated = list(party_groups.values())
    print(f"Consolidated to {len(consolidated)} unique parties")
    return consolidated

def create_party_ref_dataframe(consolidated_parties):
    """Create the final party_ref dataframe"""
    print("Creating party_ref dataframe...")
    
    party_ref_data = []
    
    for i, party in enumerate(consolidated_parties):
        # Remove duplicates from lists
        unique_accounts = list(set(party['accounts_list'])) if party['accounts_list'] else []
        unique_indices = list(set(party['source_index_list']))
        
        party_ref_record = {
            'party_id': i + 1,
            'name': party['name'],
            'email': party['email'],
            'phone': party['phone'],
            'address': party['address'],
            'country': party['country'],
            'accounts_list': json.dumps(unique_accounts),
            'source_system': party['source_system'],
            'source_index_list': json.dumps(unique_indices)
        }
        party_ref_data.append(party_ref_record)
    
    return pd.DataFrame(party_ref_data)

def main():
    """Main function to generate party_ref_large.csv"""
    print("Starting party_ref generation from large source files...")
    
    # Load source data
    trnx_df, orbis_df, wc_df = load_large_source_data()
    
    # Extract parties from each source
    trnx_parties = extract_parties_from_transactions(trnx_df)
    orbis_parties = extract_parties_from_orbis(orbis_df)
    wc_parties = extract_parties_from_worldcheck(wc_df)
    
    # Combine all parties
    all_parties = trnx_parties + orbis_parties + wc_parties
    print(f"Total extracted parties: {len(all_parties)}")
    
    # Consolidate parties
    consolidated_parties = consolidate_parties(all_parties)
    
    # Create final dataframe
    party_ref_df = create_party_ref_dataframe(consolidated_parties)
    
    # Save to file
    output_file = 'data/party_ref_large.csv'
    party_ref_df.to_csv(output_file, index=False)
    print(f"Saved {len(party_ref_df)} party records to {output_file}")
    
    # Print summary
    print("\nSummary:")
    print(f"Total unique parties: {len(party_ref_df)}")
    print(f"Parties from trnx: {len(party_ref_df[party_ref_df['source_system'] == 'trnx'])}")
    print(f"Parties from orbis: {len(party_ref_df[party_ref_df['source_system'] == 'orbis'])}")
    print(f"Parties from WC: {len(party_ref_df[party_ref_df['source_system'] == 'WC'])}")
    
    print("Party reference generation completed!")

if __name__ == "__main__":
    main() 