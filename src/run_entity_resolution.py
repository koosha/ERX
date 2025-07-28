#!/usr/bin/env python3
"""
Run Entity Resolution on party_ref_large.csv to generate entity.csv
"""

import pandas as pd
import json
import logging
import re
from typing import List, Dict, Any
from fuzzywuzzy import fuzz
import numpy as np
from collections import defaultdict, Counter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PartyEntityResolver:
    """Entity resolver specifically for party reference data"""
    
    def __init__(self):
        self.entities = []
        self.entity_counter = 0
        
    def preprocess_party(self, party: Dict) -> Dict:
        """Preprocess party data for matching"""
        processed = party.copy()
        
        # Normalize name
        if 'name' in processed and processed['name'] and pd.notna(processed['name']):
            name = str(processed['name'])
            # Remove extra spaces and punctuation
            name = re.sub(r'[^\w\s]', ' ', name)
            name = re.sub(r'\s+', ' ', name).strip().lower()
            processed['name_normalized'] = name
        else:
            processed['name_normalized'] = ''
        
        # Normalize email
        if 'email' in processed and processed['email'] and pd.notna(processed['email']):
            processed['email_normalized'] = str(processed['email']).lower()
        else:
            processed['email_normalized'] = ''
        
        # Normalize phone
        if 'phone' in processed and processed['phone'] and pd.notna(processed['phone']):
            # Remove all non-digit characters
            processed['phone_normalized'] = re.sub(r'\D', '', str(processed['phone']))
        else:
            processed['phone_normalized'] = ''
        
        # Normalize address
        if 'address' in processed and processed['address'] and pd.notna(processed['address']):
            address = str(processed['address'])
            address = re.sub(r'\s+', ' ', address).strip().lower()
            processed['address_normalized'] = address
        else:
            processed['address_normalized'] = ''
        
        return processed
    
    def calculate_similarity(self, party1: Dict, party2: Dict) -> float:
        """Calculate similarity between two parties"""
        similarities = {}
        
        # Name similarity (highest weight)
        if party1['name_normalized'] and party2['name_normalized']:
            name_sim = fuzz.ratio(party1['name_normalized'], party2['name_normalized']) / 100.0
            similarities['name'] = name_sim * 0.4  # 40% weight
        
        # Email similarity (very high weight if exact match)
        if party1['email_normalized'] and party2['email_normalized']:
            if party1['email_normalized'] == party2['email_normalized']:
                similarities['email'] = 1.0 * 0.3  # 30% weight for exact match
            else:
                email_sim = fuzz.ratio(party1['email_normalized'], party2['email_normalized']) / 100.0
                similarities['email'] = email_sim * 0.3
        
        # Phone similarity (high weight if exact match)
        if party1['phone_normalized'] and party2['phone_normalized']:
            if party1['phone_normalized'] == party2['phone_normalized']:
                similarities['phone'] = 1.0 * 0.2  # 20% weight for exact match
            else:
                phone_sim = fuzz.ratio(party1['phone_normalized'], party2['phone_normalized']) / 100.0
                similarities['phone'] = phone_sim * 0.2
        
        # Address similarity (medium weight)
        if party1['address_normalized'] and party2['address_normalized']:
            addr_sim = fuzz.ratio(party1['address_normalized'], party2['address_normalized']) / 100.0
            similarities['address'] = addr_sim * 0.1  # 10% weight
        
        # Calculate weighted average
        if similarities:
            total_weight = sum(similarities.values())
            return total_weight
        else:
            return 0.0
    
    def resolve_entities(self, parties: List[Dict]) -> List[Dict]:
        """Resolve entities from party data"""
        logger.info(f"Starting entity resolution for {len(parties)} parties")
        
        # Preprocess all parties
        processed_parties = [self.preprocess_party(party) for party in parties]
        
        # Find similar parties and group them
        entity_groups = self._find_entity_groups(processed_parties)
        
        # Create entities from groups
        entities = []
        for group in entity_groups:
            entity = self._create_entity(group)
            entities.append(entity)
            self.entity_counter += 1
        
        logger.info(f"Resolved {len(entities)} entities from {len(parties)} parties")
        return entities
    
    def _find_entity_groups(self, parties: List[Dict]) -> List[List[Dict]]:
        """Find groups of similar parties"""
        groups = []
        used = set()
        
        for i, party1 in enumerate(parties):
            if i in used:
                continue
                
            group = [party1]
            used.add(i)
            
            for j, party2 in enumerate(parties[i+1:], i+1):
                if j in used:
                    continue
                    
                similarity = self.calculate_similarity(party1, party2)
                if similarity >= 0.7:  # 70% similarity threshold
                    group.append(party2)
                    used.add(j)
            
            groups.append(group)
        
        return groups
    
    def _create_entity(self, party_group: List[Dict]) -> Dict:
        """Create an entity from a group of similar parties"""
        # Get party IDs
        party_ids = [party['party_id'] for party in party_group]
        
        # Get source systems
        source_systems = list(set([party['source_system'] for party in party_group]))
        
        # Calculate confidence score
        confidence = self._calculate_confidence(party_group)
        
        # Get resolved fields
        resolved_name = self._get_resolved_name(party_group)
        resolved_email = self._get_resolved_email(party_group)
        resolved_phone = self._get_resolved_phone(party_group)
        resolved_address = self._get_resolved_address(party_group)
        resolved_country = self._get_resolved_country(party_group)
        
        # Create records list
        records = []
        for party in party_group:
            record = {
                'party_id': party['party_id'],
                'name': party['name'],
                'email': party['email'],
                'phone': party['phone'],
                'address': party['address'],
                'country': party['country'],
                'accounts_list': party['accounts_list'],
                'source_system': party['source_system'],
                'source_index_list': party['source_index_list']
            }
            records.append(record)
        
        entity = {
            'entity_id': f"E{self.entity_counter + 1:06d}",
            'party_ids': json.dumps(party_ids),
            'confidence_score': confidence,
            'resolved_name': resolved_name,
            'resolved_address': resolved_address,
            'resolved_phone': resolved_phone,
            'resolved_email': resolved_email,
            'resolved_country': resolved_country,
            'source_systems': json.dumps(source_systems),
            'records': json.dumps(records)
        }
        
        return entity
    
    def _calculate_confidence(self, party_group: List[Dict]) -> float:
        """Calculate confidence score for entity resolution"""
        if len(party_group) == 1:
            return 0.7
        
        # Calculate average similarity within group
        similarities = []
        for i, party1 in enumerate(party_group):
            for j, party2 in enumerate(party_group[i+1:], i+1):
                sim = self.calculate_similarity(party1, party2)
                similarities.append(sim)
        
        if similarities:
            avg_similarity = np.mean(similarities)
            size_boost = min(len(party_group) * 0.05, 0.2)
            return min(avg_similarity + size_boost, 1.0)
        
        return 0.8
    
    def _get_resolved_name(self, party_group: List[Dict]) -> str:
        """Get the most representative name"""
        names = [str(party['name']) for party in party_group if party['name'] and pd.notna(party['name'])]
        if not names:
            return ''
        return max(names, key=len)
    
    def _get_resolved_email(self, party_group: List[Dict]) -> str:
        """Get the most representative email"""
        emails = [str(party['email']) for party in party_group if party['email'] and pd.notna(party['email'])]
        if not emails:
            return ''
        return emails[0]
    
    def _get_resolved_phone(self, party_group: List[Dict]) -> str:
        """Get the most representative phone"""
        phones = [str(party['phone']) for party in party_group if party['phone'] and pd.notna(party['phone'])]
        if not phones:
            return ''
        return phones[0]
    
    def _get_resolved_address(self, party_group: List[Dict]) -> str:
        """Get the most representative address"""
        addresses = [str(party['address']) for party in party_group if party['address'] and pd.notna(party['address'])]
        if not addresses:
            return ''
        return max(addresses, key=len)
    
    def _get_resolved_country(self, party_group: List[Dict]) -> str:
        """Get the most representative country"""
        countries = [str(party['country']) for party in party_group if party['country'] and pd.notna(party['country'])]
        if not countries:
            return ''
        # Return most common country
        return Counter(countries).most_common(1)[0][0]

def main():
    """Main function to run entity resolution"""
    logger.info("Loading party reference data...")
    
    # Load party reference data
    party_df = pd.read_csv('data/party_ref_large.csv')
    logger.info(f"Loaded {len(party_df)} party records")
    
    # Convert to list of dictionaries
    parties = party_df.to_dict('records')
    
    # Initialize resolver
    resolver = PartyEntityResolver()
    
    # Run entity resolution
    logger.info("Running entity resolution...")
    entities = resolver.resolve_entities(parties)
    
    # Save to CSV
    logger.info("Saving entities to CSV...")
    entity_df = pd.DataFrame(entities)
    entity_df.to_csv('data/entity.csv', index=False)
    
    # Print summary
    logger.info(f"Entity resolution completed!")
    logger.info(f"Input parties: {len(parties)}")
    logger.info(f"Resolved entities: {len(entities)}")
    logger.info(f"Reduction ratio: {len(entities)/len(parties)*100:.1f}%")
    
    # Print some statistics
    confidence_scores = [entity['confidence_score'] for entity in entities]
    logger.info(f"Average confidence: {np.mean(confidence_scores):.3f}")
    logger.info(f"Min confidence: {min(confidence_scores):.3f}")
    logger.info(f"Max confidence: {max(confidence_scores):.3f}")

if __name__ == "__main__":
    main() 