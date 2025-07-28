#!/usr/bin/env python3
"""
Large Party Reference Generator
Generates party_ref_large.csv from the large source files
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

def safe_string(value):
    """Convert value to safe string, handling NaN values"""
    if pd.isna(value) or value == '':
        return ''
    return str(value)

def extract_parties_from_transactions(trnx_df: pd.DataFrame) -> List[Dict]:
    """Extract parties from transaction data"""
    print("Extracting parties from transactions...")
    
    parties = []
    party_id = 1
    
    # Track unique parties within trnx source
    trnx_parties = {}
    
    # Extract originators
    for _, row in trnx_df.iterrows():
        name = safe_string(row['originator_name'])
        email = safe_string(row['email_originator'])
        
        # Create key for deduplication within trnx
        key = (name.lower(), email.lower())
        
        if key not in trnx_parties:
            # New party in trnx
            party = {
                'party_id': party_id,
                'name': name,
                'email': email,
                'phone': safe_string(row['phone_originator']),
                'address': safe_string(row['originator_address']),
                'country': safe_string(row['originator_country']),
                'accounts_list': json.dumps([safe_string(row['originator_account'])]),
                'source_system': 'trnx',
                'source_index_list': json.dumps([safe_string(row['transaction_id'])])
            }
            parties.append(party)
            trnx_parties[key] = party_id
            party_id += 1
        else:
            # Existing party in trnx - update accounts and indices
            existing_party = parties[trnx_parties[key] - 1]
            existing_party['accounts_list'] = json.dumps(
                json.loads(existing_party['accounts_list']) + [safe_string(row['originator_account'])]
            )
            existing_party['source_index_list'] = json.dumps(
                json.loads(existing_party['source_index_list']) + [safe_string(row['transaction_id'])]
            )
    
    # Extract beneficiaries
    for _, row in trnx_df.iterrows():
        name = safe_string(row['beneficiary_name'])
        email = safe_string(row['email_beneficiary'])
        
        # Create key for deduplication within trnx
        key = (name.lower(), email.lower())
        
        if key not in trnx_parties:
            # New party in trnx
            party = {
                'party_id': party_id,
                'name': name,
                'email': email,
                'phone': safe_string(row['phone_beneficiary']),
                'address': safe_string(row['beneficiary_address']),
                'country': safe_string(row['beneficiary_country']),
                'accounts_list': json.dumps([safe_string(row['beneficiary_account'])]),
                'source_system': 'trnx',
                'source_index_list': json.dumps([safe_string(row['transaction_id'])])
            }
            parties.append(party)
            trnx_parties[key] = party_id
            party_id += 1
        else:
            # Existing party in trnx - update accounts and indices
            existing_party = parties[trnx_parties[key] - 1]
            existing_party['accounts_list'] = json.dumps(
                json.loads(existing_party['accounts_list']) + [safe_string(row['beneficiary_account'])]
            )
            existing_party['source_index_list'] = json.dumps(
                json.loads(existing_party['source_index_list']) + [safe_string(row['transaction_id'])]
            )
    
    # Extract third parties if they exist
    for _, row in trnx_df.iterrows():
        if row['TP_originator'] and pd.notna(row['TP_originator']):
            name = safe_string(row['TP_originator'])
            email = safe_string(row['email_TP_originator'])
            
            key = (name.lower(), email.lower())
            
            if key not in trnx_parties:
                party = {
                    'party_id': party_id,
                    'name': name,
                    'email': email,
                    'phone': safe_string(row['phone_TP_originator']),
                    'address': '',  # Not available in transaction data
                    'country': '',  # Not available in transaction data
                    'accounts_list': json.dumps([]),
                    'source_system': 'trnx',
                    'source_index_list': json.dumps([safe_string(row['transaction_id'])])
                }
                parties.append(party)
                trnx_parties[key] = party_id
                party_id += 1
            else:
                # Update existing party
                existing_party = parties[trnx_parties[key] - 1]
                existing_party['source_index_list'] = json.dumps(
                    json.loads(existing_party['source_index_list']) + [safe_string(row['transaction_id'])]
                )
        
        if row['TP_beneficiary'] and pd.notna(row['TP_beneficiary']):
            name = safe_string(row['TP_beneficiary'])
            email = safe_string(row['email_TP_beneficiary'])
            
            key = (name.lower(), email.lower())
            
            if key not in trnx_parties:
                party = {
                    'party_id': party_id,
                    'name': name,
                    'email': email,
                    'phone': safe_string(row['phone_TP_beneficiary']),
                    'address': '',  # Not available in transaction data
                    'country': '',  # Not available in transaction data
                    'accounts_list': json.dumps([]),
                    'source_system': 'trnx',
                    'source_index_list': json.dumps([safe_string(row['transaction_id'])])
                }
                parties.append(party)
                trnx_parties[key] = party_id
                party_id += 1
            else:
                # Update existing party
                existing_party = parties[trnx_parties[key] - 1]
                existing_party['source_index_list'] = json.dumps(
                    json.loads(existing_party['source_index_list']) + [safe_string(row['transaction_id'])]
                )
    
    print(f"Extracted {len(parties)} parties from transactions")
    return parties, party_id

def extract_parties_from_orbis(orbis_df: pd.DataFrame, start_party_id: int) -> List[Dict]:
    """Extract parties from Orbis data"""
    print("Extracting parties from Orbis...")
    
    parties = []
    party_id = start_party_id
    
    for _, row in orbis_df.iterrows():
        party = {
            'party_id': party_id,
            'name': safe_string(row['company_name']),
            'email': safe_string(row['email']),
            'phone': safe_string(row['phone']),
            'address': safe_string(row['address']),
            'country': safe_string(row['country_name']),
            'accounts_list': json.dumps([]),  # No account info in Orbis
            'source_system': 'orbis',
            'source_index_list': json.dumps([safe_string(row['orbis_id'])])
        }
        parties.append(party)
        party_id += 1
    
    print(f"Extracted {len(parties)} parties from Orbis")
    return parties

def extract_parties_from_worldcheck(wc_df: pd.DataFrame, start_party_id: int) -> List[Dict]:
    """Extract parties from WorldCheck data"""
    print("Extracting parties from WorldCheck...")
    
    parties = []
    party_id = start_party_id
    
    for _, row in wc_df.iterrows():
        party = {
            'party_id': party_id,
            'name': safe_string(row['full_name']),
            'email': safe_string(row['email']),
            'phone': safe_string(row['phone']),
            'address': safe_string(row['address']),
            'country': safe_string(row['nationality']),  # Use nationality as country
            'accounts_list': json.dumps([]),  # No account info in WorldCheck
            'source_system': 'WC',
            'source_index_list': json.dumps([safe_string(row['wc_id'])])
        }
        parties.append(party)
        party_id += 1
    
    print(f"Extracted {len(parties)} parties from WorldCheck")
    return parties

def save_party_ref_large(parties: List[Dict], filename: str = 'data/party_ref_large.csv'):
    """Save party reference data to CSV"""
    print(f"Saving {len(parties)} parties to {filename}...")
    
    df = pd.DataFrame(parties)
    df.to_csv(filename, index=False)
    
    print(f"Party reference data saved to {filename}")

def main():
    """Main function to generate large party reference data"""
    print("Starting large party reference generation...")
    
    # Load large source data
    trnx_df, orbis_df, wc_df = load_large_source_data()
    
    # Extract parties from each source (no cross-source deduplication)
    trnx_parties, next_party_id = extract_parties_from_transactions(trnx_df)
    orbis_parties = extract_parties_from_orbis(orbis_df, next_party_id)
    wc_parties = extract_parties_from_worldcheck(wc_df, next_party_id + len(orbis_parties))
    
    # Combine all parties (no deduplication across sources)
    all_parties = trnx_parties + orbis_parties + wc_parties
    
    # Save large party reference data
    save_party_ref_large(all_parties)
    
    print("\nLarge party reference generation completed!")
    print(f"Generated {len(all_parties)} total parties")
    print("Check data/party_ref_large.csv for the complete dataset")

if __name__ == "__main__":
    main() 