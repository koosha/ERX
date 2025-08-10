#!/usr/bin/env python3
"""
Fast Entity Resolution - Highly optimized version
"""

import pandas as pd
import json
import logging
import re
from typing import List, Dict, Any, Set
from fuzzywuzzy import fuzz
import numpy as np
from collections import defaultdict, Counter
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FastEntityResolver:
    def __init__(self):
        self.entities = []
        self.entity_counter = 0
        self.start_time = time.time()
        self.last_progress_time = time.time()
        
    def print_progress(self, message: str, force_print: bool = False):
        """Print progress with timing info"""
        current_time = time.time()
        if force_print or (current_time - self.last_progress_time) > 30:
            elapsed = current_time - self.start_time
            print(f"[{elapsed:.1f}s] {message}")
            self.last_progress_time = current_time
        
    def preprocess_party(self, party: Dict) -> Dict:
        """Preprocess party data for matching"""
        processed = party.copy()
        
        # Normalize name
        if 'name' in processed and processed['name'] and pd.notna(processed['name']):
            name = str(processed['name'])
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
            processed['phone_normalized'] = re.sub(r'\D', '', str(processed['phone']))
        else:
            processed['phone_normalized'] = ''
        
        return processed
    
    def find_exact_matches(self, parties: List[Dict]) -> List[List[Dict]]:
        """Find exact matches first (very fast)"""
        print("Finding exact matches...")
        
        exact_groups = []
        used = set()
        
        # Group by exact email match
        email_groups = defaultdict(list)
        for party in parties:
            if party['email_normalized']:
                email_groups[party['email_normalized']].append(party)
        
        # Create groups for exact email matches
        for email, group in email_groups.items():
            if len(group) > 1 and id(group[0]) not in used:
                exact_groups.append(group)
                for p in group:
                    used.add(id(p))
        
        # Group by exact phone match
        phone_groups = defaultdict(list)
        for party in parties:
            if party['phone_normalized'] and id(party) not in used:
                phone_groups[party['phone_normalized']].append(party)
        
        # Create groups for exact phone matches
        for phone, group in phone_groups.items():
            if len(group) > 1:
                exact_groups.append(group)
                for p in group:
                    used.add(id(p))
        
        print(f"Found {len(exact_groups)} exact match groups")
        return exact_groups, used
    
    def create_smart_blocks(self, parties: List[Dict]) -> Dict[str, List[Dict]]:
        """Create smart blocking keys to minimize block sizes"""
        blocks = defaultdict(list)
        
        print(f"Creating smart blocks for {len(parties)} parties...")
        
        for i, party in enumerate(parties):
            name = party['name_normalized']
            email = party['email_normalized']
            phone = party['phone_normalized']
            
            # More specific blocking keys
            if name:
                # Use first 5 characters instead of 3
                if len(name) >= 5:
                    block_key = f"name_{name[:5]}"
                    blocks[block_key].append(party)
                
                # Use first word if it's longer
                first_word = name.split()[0] if name.split() else ""
                if len(first_word) >= 4:
                    block_key = f"word_{first_word[:4]}"
                    blocks[block_key].append(party)
            
            # Email domain blocking
            if email and '@' in email:
                domain = email.split('@')[1]
                if len(domain) >= 3:
                    block_key = f"email_{domain[:6]}"
                    blocks[block_key].append(party)
            
            # Phone prefix blocking (more specific)
            if phone and len(phone) >= 6:
                phone_prefix = phone[:6]
                block_key = f"phone_{phone_prefix}"
                blocks[block_key].append(party)
            
            if i % 50000 == 0 and i > 0:
                self.print_progress(f"Created blocks for {i}/{len(parties)} parties")
        
        # Filter out blocks that are too large
        filtered_blocks = {}
        for key, block_parties in blocks.items():
            if len(block_parties) <= 1000:  # Skip blocks larger than 1000
                filtered_blocks[key] = block_parties
        
        # Print block statistics
        block_sizes = [len(block_parties) for block_parties in filtered_blocks.values()]
        large_blocks = [size for size in block_sizes if size > 100]
        
        print(f"Smart block statistics:")
        print(f"  Total blocks: {len(filtered_blocks)}")
        print(f"  Average block size: {np.mean(block_sizes):.1f}")
        print(f"  Large blocks (>100 parties): {len(large_blocks)}")
        if large_blocks:
            print(f"  Largest block: {max(block_sizes)} parties")
        
        return filtered_blocks
    
    def calculate_similarity_fast(self, party1: Dict, party2: Dict) -> float:
        """Fast similarity calculation with early termination"""
        score = 0.0
        
        # Name similarity (highest weight)
        if party1['name_normalized'] and party2['name_normalized']:
            name_sim = fuzz.ratio(party1['name_normalized'], party2['name_normalized']) / 100.0
            score += name_sim * 0.4
            
            # Early termination if name similarity is very low
            if name_sim < 0.3:
                return score
        
        # Email exact match (very high weight)
        if party1['email_normalized'] and party2['email_normalized']:
            if party1['email_normalized'] == party2['email_normalized']:
                score += 0.3
                return score  # Email match is very strong indicator
        
        # Phone exact match
        if party1['phone_normalized'] and party2['phone_normalized']:
            if party1['phone_normalized'] == party2['phone_normalized']:
                score += 0.2
                return score  # Phone match is strong indicator
        
        return score
    
    def resolve_entities_fast(self, parties: List[Dict]) -> List[Dict]:
        """Fast entity resolution using exact matches and smart blocking"""
        total_parties = len(parties)
        logger.info(f"Starting fast entity resolution for {total_parties} parties")
        
        # Stage 1: Preprocessing
        print(f"Stage 1/4: Preprocessing {total_parties} parties...")
        processed_parties = []
        for i, party in enumerate(parties):
            processed_parties.append(self.preprocess_party(party))
            if i % 100000 == 0 and i > 0:
                self.print_progress(f"Preprocessed {i}/{total_parties} parties ({i/total_parties*100:.1f}%)")
        
        self.print_progress(f"Stage 1 complete: Preprocessed {len(processed_parties)} parties", force_print=True)
        
        # Stage 2: Find exact matches first
        print(f"Stage 2/4: Finding exact matches...")
        exact_groups, used_parties = self.find_exact_matches(processed_parties)
        
        # Get remaining parties for fuzzy matching
        remaining_parties = [p for p in processed_parties if id(p) not in used_parties]
        print(f"Remaining parties for fuzzy matching: {len(remaining_parties)}")
        
        self.print_progress(f"Stage 2 complete: Found {len(exact_groups)} exact match groups", force_print=True)
        
        # Stage 3: Smart blocking for remaining parties
        print(f"Stage 3/4: Creating smart blocks for fuzzy matching...")
        blocked_parties = self.create_smart_blocks(remaining_parties)
        fuzzy_groups = self._find_entity_groups_blocked(blocked_parties)
        
        self.print_progress(f"Stage 3 complete: Found {len(fuzzy_groups)} fuzzy match groups", force_print=True)
        
        # Stage 4: Combine and create entities
        print(f"Stage 4/4: Creating entities...")
        all_groups = exact_groups + fuzzy_groups
        
        # Add single parties as individual entities
        all_used = set()
        for group in all_groups:
            for party in group:
                all_used.add(id(party))
        
        single_parties = [p for p in processed_parties if id(p) not in all_used]
        for party in single_parties:
            all_groups.append([party])
        
        entities = []
        for i, group in enumerate(all_groups):
            entity = self._create_entity(group)
            entities.append(entity)
            self.entity_counter += 1
            
            if i % 1000 == 0 and i > 0:
                self.print_progress(f"Created {i}/{len(all_groups)} entities ({i/len(all_groups)*100:.1f}%)")
        
        self.print_progress(f"Stage 4 complete: Created {len(entities)} entities", force_print=True)
        
        logger.info(f"Resolved {len(entities)} entities from {len(parties)} parties")
        return entities
    
    def _find_entity_groups_blocked(self, blocked_parties: Dict[str, List[Dict]]) -> List[List[Dict]]:
        """Find entity groups using blocking strategy"""
        groups = []
        used = set()
        total_blocks = len(blocked_parties)
        
        print(f"  Processing {total_blocks} blocks for entity grouping...")
        
        # Process each block
        for block_idx, (block_key, block_parties) in enumerate(blocked_parties.items()):
            if len(block_parties) <= 1:
                continue
            
            # Print progress for large blocks
            if len(block_parties) > 100 or block_idx % 500 == 0:
                self.print_progress(f"  Block {block_idx+1}/{total_blocks}: '{block_key}' ({len(block_parties)} parties)")
            
            # Only compare parties within the same block
            for i, party1 in enumerate(block_parties):
                if id(party1) in used:
                    continue
                    
                group = [party1]
                used.add(id(party1))
                
                for j, party2 in enumerate(block_parties[i+1:], i+1):
                    if id(party2) in used:
                        continue
                        
                    similarity = self.calculate_similarity_fast(party1, party2)
                    if similarity >= 0.7:
                        group.append(party2)
                        used.add(id(party2))
                
                if group:
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
        
        # Simplified confidence calculation
        similarities = []
        for i, party1 in enumerate(party_group):
            for j, party2 in enumerate(party_group[i+1:], i+1):
                sim = self.calculate_similarity_fast(party1, party2)
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
        return Counter(countries).most_common(1)[0][0]

def main():
    """Main function to run fast entity resolution"""
    start_time = time.time()
    
    print("=" * 60)
    print("FAST ENTITY RESOLUTION - HIGHLY OPTIMIZED")
    print("=" * 60)
    
    logger.info("Loading party reference data...")
    
    # Load party reference data
    party_df = pd.read_csv('data/party_ref_large.csv')
    total_parties = len(party_df)
    logger.info(f"Loaded {total_parties} party records")
    
    # Convert to list of dictionaries
    parties = party_df.to_dict('records')
    
    # Initialize resolver
    resolver = FastEntityResolver()
    
    # Run entity resolution
    logger.info("Running fast entity resolution...")
    entities = resolver.resolve_entities_fast(parties)
    
    # Save to CSV
    logger.info("Saving entities to CSV...")
    entity_df = pd.DataFrame(entities)
    entity_df.to_csv('data/entity.csv', index=False)
    
    # Print comprehensive summary
    end_time = time.time()
    duration = end_time - start_time
    
    print("\n" + "=" * 60)
    print("FAST ENTITY RESOLUTION COMPLETED!")
    print("=" * 60)
    print(f"Total processing time: {duration:.2f} seconds ({duration/60:.1f} minutes)")
    print(f"Input parties: {total_parties:,}")
    print(f"Resolved entities: {len(entities):,}")
    print(f"Reduction ratio: {len(entities)/total_parties*100:.1f}%")
    
    # Print confidence statistics
    confidence_scores = [entity['confidence_score'] for entity in entities]
    print(f"\nConfidence Statistics:")
    print(f"  Average confidence: {np.mean(confidence_scores):.3f}")
    print(f"  Min confidence: {min(confidence_scores):.3f}")
    print(f"  Max confidence: {max(confidence_scores):.3f}")
    
    # Print source system distribution
    source_system_counts = defaultdict(int)
    for entity in entities:
        source_systems = json.loads(entity['source_systems'])
        for system in source_systems:
            source_system_counts[system] += 1
    
    print(f"\nSource System Distribution:")
    for system, count in source_system_counts.items():
        print(f"  {system}: {count:,} entities")
    
    print("=" * 60)

if __name__ == "__main__":
    main() 