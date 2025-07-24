#!/usr/bin/env python3
"""
Entity Resolution Module
Identifies and groups similar records into entities with proper classification
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
import yaml
import logging
from datetime import datetime
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import re
from collections import defaultdict
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EntityResolver:
    """
    Entity Resolution engine that groups similar records into entities
    """
    
    def __init__(self, config_path: str = "config/resolution_config.yaml"):
        """Initialize the entity resolver with configuration"""
        self.config = self._load_config(config_path)
        self.entities = []
        self.record_to_entity = {}
        self.entity_counter = 0
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            return config
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            raise
    
    def preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess a record for better matching"""
        processed = record.copy()
        
        # Normalize name
        if 'name' in processed:
            name = processed['name']
            if self.config['resolution']['preprocessing']['normalize_names']:
                # Remove extra spaces and punctuation
                name = re.sub(r'[^\w\s]', ' ', name)
                name = re.sub(r'\s+', ' ', name).strip()
                if self.config['resolution']['preprocessing']['lowercase']:
                    name = name.lower()
            processed['name_normalized'] = name
        
        # Normalize email
        if 'email' in processed:
            email = processed['email']
            if self.config['resolution']['preprocessing']['lowercase']:
                email = email.lower()
            processed['email_normalized'] = email
        
        # Normalize phone
        if 'phone' in processed:
            phone = processed['phone']
            if self.config['resolution']['preprocessing']['standardize_phone']:
                # Remove all non-digit characters
                phone = re.sub(r'\D', '', phone)
            processed['phone_normalized'] = phone
        
        # Normalize address
        if 'address' in processed:
            address = processed['address']
            if self.config['resolution']['preprocessing']['extract_address_components']:
                # Basic address normalization
                address = re.sub(r'\s+', ' ', address).strip()
                if self.config['resolution']['preprocessing']['lowercase']:
                    address = address.lower()
            processed['address_normalized'] = address
        
        return processed
    
    def calculate_similarity(self, record1: Dict, record2: Dict) -> float:
        """Calculate overall similarity between two records"""
        similarities = {}
        
        # Name similarity
        if 'name_normalized' in record1 and 'name_normalized' in record2:
            name_sim = self._calculate_name_similarity(
                record1['name_normalized'], 
                record2['name_normalized']
            )
            similarities['name'] = name_sim
        
        # Email similarity
        if 'email_normalized' in record1 and 'email_normalized' in record2:
            email_sim = self._calculate_email_similarity(
                record1['email_normalized'], 
                record2['email_normalized']
            )
            similarities['email'] = email_sim
        
        # Phone similarity
        if 'phone_normalized' in record1 and 'phone_normalized' in record2:
            phone_sim = self._calculate_phone_similarity(
                record1['phone_normalized'], 
                record2['phone_normalized']
            )
            similarities['phone'] = phone_sim
        
        # Address similarity
        if 'address_normalized' in record1 and 'address_normalized' in record2:
            address_sim = self._calculate_address_similarity(
                record1['address_normalized'], 
                record2['address_normalized']
            )
            similarities['address'] = address_sim
        
        # Calculate weighted average
        if not similarities:
            return 0.0
        
        weights = {
            'name': 0.4,
            'email': 0.3,
            'phone': 0.2,
            'address': 0.1
        }
        
        weighted_sum = sum(similarities[field] * weights[field] for field in similarities)
        total_weight = sum(weights[field] for field in similarities)
        
        return weighted_sum / total_weight if total_weight > 0 else 0.0
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate name similarity using multiple algorithms"""
        if not name1 or not name2:
            return 0.0
        
        algorithms = self.config['resolution']['algorithms']['name']
        scores = []
        
        for algo in algorithms:
            if algo['type'] == 'fuzzy':
                if algo['method'] == 'token_sort_ratio':
                    score = fuzz.token_sort_ratio(name1, name2)
                elif algo['method'] == 'partial_ratio':
                    score = fuzz.partial_ratio(name1, name2)
                else:
                    score = fuzz.ratio(name1, name2)
            elif algo['type'] == 'levenshtein':
                score = fuzz.ratio(name1, name2)
            else:
                score = 1.0 if name1 == name2 else 0.0
            
            scores.append(score * algo['weight'])
        
        return sum(scores) / 100.0  # Normalize to 0-1
    
    def _calculate_email_similarity(self, email1: str, email2: str) -> float:
        """Calculate email similarity"""
        if not email1 or not email2:
            return 0.0
        
        if email1 == email2:
            return 1.0
        
        # Extract domain and local part
        try:
            local1, domain1 = email1.split('@')
            local2, domain2 = email2.split('@')
            
            # Domain exact match gets high weight
            domain_sim = 1.0 if domain1 == domain2 else 0.0
            local_sim = fuzz.ratio(local1, local2) / 100.0
            
            return 0.3 * local_sim + 0.7 * domain_sim
        except:
            return fuzz.ratio(email1, email2) / 100.0
    
    def _calculate_phone_similarity(self, phone1: str, phone2: str) -> float:
        """Calculate phone similarity"""
        if not phone1 or not phone2:
            return 0.0
        
        if phone1 == phone2:
            return 1.0
        
        # Check if they're the same after normalization
        if len(phone1) >= 10 and len(phone2) >= 10:
            # Compare last 10 digits
            if phone1[-10:] == phone2[-10:]:
                return 0.9
        
        return fuzz.ratio(phone1, phone2) / 100.0
    
    def _calculate_address_similarity(self, addr1: str, addr2: str) -> float:
        """Calculate address similarity"""
        if not addr1 or not addr2:
            return 0.0
        
        if addr1 == addr2:
            return 1.0
        
        algorithms = self.config['resolution']['algorithms']['address']
        scores = []
        
        for algo in algorithms:
            if algo['method'] == 'token_set_ratio':
                score = fuzz.token_set_ratio(addr1, addr2)
            elif algo['method'] == 'partial_ratio':
                score = fuzz.partial_ratio(addr1, addr2)
            else:
                score = fuzz.ratio(addr1, addr2)
            
            scores.append(score * algo['weight'])
        
        return sum(scores) / 100.0
    
    def determine_entity_type(self, records: List[Dict]) -> str:
        """Determine if entity is individual or business"""
        business_indicators = ['inc', 'corp', 'ltd', 'llc', 'company', 'corporation', 'limited', 'co']
        
        for record in records:
            name = record.get('name', '').lower()
            if any(indicator in name for indicator in business_indicators):
                return 'biz'
        
        # Check if most records have company-like patterns
        business_count = 0
        for record in records:
            name = record.get('name', '')
            # Simple heuristic: if name has no spaces, likely business
            if ' ' not in name or len(name.split()) == 1:
                business_count += 1
        
        if business_count > len(records) * 0.5:
            return 'biz'
        
        return 'ind'
    
    def determine_pep_status(self, records: List[Dict]) -> bool:
        """Determine if entity is a PEP (Politically Exposed Person)"""
        # This would typically check against PEP databases
        # For now, using a simple heuristic based on name patterns
        pep_indicators = ['senator', 'congress', 'minister', 'president', 'governor', 'mayor']
        
        for record in records:
            name = record.get('name', '').lower()
            if any(indicator in name for indicator in pep_indicators):
                return True
        
        return False
    
    def resolve_entities(self, records: List[Dict]) -> List[Dict]:
        """Main entity resolution method"""
        logger.info(f"Starting entity resolution for {len(records)} records")
        
        # Preprocess all records
        processed_records = [self.preprocess_record(record) for record in records]
        
        # Initialize clustering
        clusters = []
        record_to_cluster = {}
        
        # First pass: exact matches
        exact_matches = self._find_exact_matches(processed_records)
        for match_group in exact_matches:
            cluster_id = len(clusters)
            clusters.append(match_group)
            for record_id in match_group:
                record_to_cluster[record_id] = cluster_id
        
        # Second pass: fuzzy matching for remaining records
        remaining_records = [i for i, record in enumerate(processed_records) 
                           if i not in record_to_cluster]
        
        for record_id in remaining_records:
            record = processed_records[record_id]
            best_cluster = None
            best_similarity = 0
            
            # Check against existing clusters
            for cluster_id, cluster in enumerate(clusters):
                cluster_similarities = []
                for cluster_record_id in cluster:
                    cluster_record = processed_records[cluster_record_id]
                    similarity = self.calculate_similarity(record, cluster_record)
                    cluster_similarities.append(similarity)
                
                avg_similarity = np.mean(cluster_similarities)
                if avg_similarity > self.config['resolution']['thresholds']['overall_threshold']:
                    if avg_similarity > best_similarity:
                        best_similarity = avg_similarity
                        best_cluster = cluster_id
            
            if best_cluster is not None:
                clusters[best_cluster].append(record_id)
                record_to_cluster[record_id] = best_cluster
            else:
                # Create new cluster
                new_cluster_id = len(clusters)
                clusters.append([record_id])
                record_to_cluster[record_id] = new_cluster_id
        
        # Convert clusters to entities
        entities = []
        for cluster_id, cluster in enumerate(clusters):
            cluster_records = [processed_records[record_id] for record_id in cluster]
            
            entity = {
                'entity_id': f"ENT{cluster_id:06d}",
                'entity_type': self.determine_entity_type(cluster_records),
                'records': cluster_records,
                'pep_ind': self.determine_pep_status(cluster_records),
                'confidence': self._calculate_entity_confidence(cluster_records),
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat(),
                'record_count': len(cluster_records),
                'sources': list(set(record.get('source_system', 'unknown') for record in cluster_records)),
                'primary_name': self._get_primary_name(cluster_records),
                'primary_email': self._get_primary_email(cluster_records),
                'primary_phone': self._get_primary_phone(cluster_records),
                'primary_address': self._get_primary_address(cluster_records)
            }
            
            entities.append(entity)
        
        self.entities = entities
        logger.info(f"Entity resolution completed. Created {len(entities)} entities")
        
        return entities
    
    def _find_exact_matches(self, records: List[Dict]) -> List[List[int]]:
        """Find records with exact matches on key fields"""
        exact_groups = []
        processed = set()
        
        for i, record1 in enumerate(records):
            if i in processed:
                continue
            
            group = [i]
            processed.add(i)
            
            for j, record2 in enumerate(records[i+1:], i+1):
                if j in processed:
                    continue
                
                # Check for exact matches on key fields
                if (record1.get('email_normalized') and 
                    record1['email_normalized'] == record2.get('email_normalized')):
                    group.append(j)
                    processed.add(j)
                elif (record1.get('phone_normalized') and 
                      record1['phone_normalized'] == record2.get('phone_normalized')):
                    group.append(j)
                    processed.add(j)
            
            if len(group) > 1:
                exact_groups.append(group)
        
        return exact_groups
    
    def _calculate_entity_confidence(self, records: List[Dict]) -> float:
        """Calculate confidence score for entity"""
        if len(records) == 1:
            return 0.7  # Single record has moderate confidence
        
        # Calculate average similarity within cluster
        similarities = []
        for i, record1 in enumerate(records):
            for j, record2 in enumerate(records[i+1:], i+1):
                sim = self.calculate_similarity(record1, record2)
                similarities.append(sim)
        
        if similarities:
            avg_similarity = np.mean(similarities)
            # Boost confidence for larger clusters with high similarity
            size_boost = min(len(records) * 0.05, 0.2)
            return min(avg_similarity + size_boost, 1.0)
        
        return 0.8
    
    def _get_primary_name(self, records: List[Dict]) -> str:
        """Get the most representative name for the entity"""
        names = [record.get('name', '') for record in records if record.get('name')]
        if not names:
            return ''
        
        # Return the longest name as primary
        return max(names, key=len)
    
    def _get_primary_email(self, records: List[Dict]) -> str:
        """Get the most representative email for the entity"""
        emails = [record.get('email', '') for record in records if record.get('email')]
        if not emails:
            return ''
        
        # Return the first valid email
        return emails[0]
    
    def _get_primary_phone(self, records: List[Dict]) -> str:
        """Get the most representative phone for the entity"""
        phones = [record.get('phone', '') for record in records if record.get('phone')]
        if not phones:
            return ''
        
        # Return the first valid phone
        return phones[0]
    
    def _get_primary_address(self, records: List[Dict]) -> str:
        """Get the most representative address for the entity"""
        addresses = [record.get('address', '') for record in records if record.get('address')]
        if not addresses:
            return ''
        
        # Return the longest address as primary
        return max(addresses, key=len)
    
    def save_entities_to_csv(self, filename: str):
        """Save entities to CSV file"""
        if not self.entities:
            logger.warning("No entities to save")
            return
        
        # Flatten entities for CSV
        flattened_entities = []
        for entity in self.entities:
            flat_entity = {
                'entity_id': entity['entity_id'],
                'entity_type': entity['entity_type'],
                'pep_ind': entity['pep_ind'],
                'confidence': entity['confidence'],
                'created_at': entity['created_at'],
                'updated_at': entity['updated_at'],
                'record_count': entity['record_count'],
                'sources': ';'.join(entity['sources']),
                'primary_name': entity['primary_name'],
                'primary_email': entity['primary_email'],
                'primary_phone': entity['primary_phone'],
                'primary_address': entity['primary_address'],
                'records_json': json.dumps(entity['records'])
            }
            flattened_entities.append(flat_entity)
        
        df = pd.DataFrame(flattened_entities)
        df.to_csv(filename, index=False)
        logger.info(f"Saved {len(flattened_entities)} entities to {filename}")
    
    def get_entity_summary(self) -> Dict:
        """Get summary statistics of entity resolution"""
        if not self.entities:
            return {}
        
        entity_types = [entity['entity_type'] for entity in self.entities]
        pep_entities = [entity for entity in self.entities if entity['pep_ind']]
        
        return {
            'total_entities': len(self.entities),
            'individual_entities': entity_types.count('ind'),
            'business_entities': entity_types.count('biz'),
            'pep_entities': len(pep_entities),
            'avg_confidence': np.mean([entity['confidence'] for entity in self.entities]),
            'avg_records_per_entity': np.mean([entity['record_count'] for entity in self.entities])
        } 