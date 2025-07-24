#!/usr/bin/env python3
"""
Feature Generation Module
Generates graph-based features for entities using TigerGraph
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime
import json
from collections import defaultdict

from .tigergraph_client import TigerGraphClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FeatureGenerator:
    """
    Feature generator that computes graph-based features for entities
    """
    
    def __init__(self, tigergraph_client: TigerGraphClient):
        """Initialize feature generator with TigerGraph client"""
        self.tg_client = tigergraph_client
        self.features_cache = {}
        
    def generate_all_features(self, entities: List[Dict]) -> pd.DataFrame:
        """Generate all available features for entities"""
        logger.info("Generating all features for entities")
        
        feature_data = []
        
        for entity in entities:
            entity_id = entity['entity_id']
            features = {
                'entity_id': entity_id,
                'entity_type': entity['entity_type'],
                'pep_ind': entity['pep_ind'],
                'risk_score': entity['risk_score']
            }
            
            # Generate graph-based features
            graph_features = self._generate_graph_features(entity_id)
            features.update(graph_features)
            
            # Generate transaction-based features
            transaction_features = self._generate_transaction_features(entity_id)
            features.update(transaction_features)
            
            # Generate network-based features
            network_features = self._generate_network_features(entity_id)
            features.update(network_features)
            
            # Generate temporal features
            temporal_features = self._generate_temporal_features(entity_id)
            features.update(temporal_features)
            
            feature_data.append(features)
        
        df = pd.DataFrame(feature_data)
        logger.info(f"Generated features for {len(df)} entities")
        
        return df
    
    def _generate_graph_features(self, entity_id: str) -> Dict[str, Any]:
        """Generate graph-based features for an entity"""
        features = {}
        
        try:
            # PageRank score
            page_rank_scores = self.tg_client.run_page_rank()
            features['page_rank_score'] = page_rank_scores.get(entity_id, 0.0)
            
            # Connected component ID
            component_mapping = self.tg_client.run_connected_components()
            features['connected_component_id'] = component_mapping.get(entity_id, -1)
            
            # Degree centrality (simplified)
            neighbors = self.tg_client.get_entity_neighbors(entity_id, max_depth=1)
            features['degree_centrality'] = len(neighbors.get('results', [])) if neighbors else 0
            
        except Exception as e:
            logger.warning(f"Error generating graph features for {entity_id}: {e}")
            features.update({
                'page_rank_score': 0.0,
                'connected_component_id': -1,
                'degree_centrality': 0
            })
        
        return features
    
    def _generate_transaction_features(self, entity_id: str) -> Dict[str, Any]:
        """Generate transaction-based features for an entity"""
        features = {}
        
        try:
            # Get transaction statistics
            query = f"""
            CREATE QUERY getTransactionStats(STRING entityId) FOR GRAPH {self.tg_client.config['tigergraph']['graph_name']} {{
                SumAccum<DOUBLE> @@total_amount = 0;
                SumAccum<INT> @@transaction_count = 0;
                SumAccum<DOUBLE> @@avg_amount = 0;
                MaxAccum<DOUBLE> @@max_amount = 0;
                MinAccum<DOUBLE> @@min_amount = 999999999;
                SetAccum<STRING> @@currencies;
                SetAccum<STRING> @@countries;
                
                Start = {{Entity.*}};
                Start = SELECT s FROM Start:s WHERE s.id == entityId;
                
                Transactions = SELECT t FROM Start:s -(HAS_TRANSACTION:e)-> Transaction:t
                ACCUM @@total_amount += t.amount,
                      @@transaction_count += 1,
                      @@max_amount += t.amount,
                      @@min_amount += t.amount,
                      @@currencies += t.currency,
                      @@countries += t.originator_country,
                      @@countries += t.beneficiary_country;
                
                IF @@transaction_count > 0 THEN
                    @@avg_amount = @@total_amount / @@transaction_count;
                END;
                
                PRINT @@total_amount, @@transaction_count, @@avg_amount, @@max_amount, @@min_amount, @@currencies.size(), @@countries.size();
            }}
            """
            
            result = self.tg_client._execute_gsql(query)
            
            if result and 'results' in result and result['results']:
                stats = result['results'][0]
                features.update({
                    'total_transaction_amount': stats[0],
                    'transaction_count': stats[1],
                    'avg_transaction_amount': stats[2],
                    'max_transaction_amount': stats[3],
                    'min_transaction_amount': stats[4],
                    'unique_currencies': stats[5],
                    'unique_countries': stats[6]
                })
            else:
                features.update({
                    'total_transaction_amount': 0.0,
                    'transaction_count': 0,
                    'avg_transaction_amount': 0.0,
                    'max_transaction_amount': 0.0,
                    'min_transaction_amount': 0.0,
                    'unique_currencies': 0,
                    'unique_countries': 0
                })
            
            # Calculate additional transaction features
            if features['transaction_count'] > 0:
                features['transaction_frequency'] = features['transaction_count'] / 30  # per month
                features['amount_variance'] = self._calculate_amount_variance(entity_id)
                features['suspicious_pattern_score'] = self._calculate_suspicious_pattern_score(entity_id)
            else:
                features['transaction_frequency'] = 0.0
                features['amount_variance'] = 0.0
                features['suspicious_pattern_score'] = 0.0
                
        except Exception as e:
            logger.warning(f"Error generating transaction features for {entity_id}: {e}")
            features.update({
                'total_transaction_amount': 0.0,
                'transaction_count': 0,
                'avg_transaction_amount': 0.0,
                'max_transaction_amount': 0.0,
                'min_transaction_amount': 0.0,
                'unique_currencies': 0,
                'unique_countries': 0,
                'transaction_frequency': 0.0,
                'amount_variance': 0.0,
                'suspicious_pattern_score': 0.0
            })
        
        return features
    
    def _generate_network_features(self, entity_id: str) -> Dict[str, Any]:
        """Generate network-based features for an entity"""
        features = {}
        
        try:
            # Get network statistics
            query = f"""
            CREATE QUERY getNetworkStats(STRING entityId) FOR GRAPH {self.tg_client.config['tigergraph']['graph_name']} {{
                SumAccum<INT> @@direct_connections = 0;
                SumAccum<INT> @@indirect_connections = 0;
                SumAccum<INT> @@pep_connections = 0;
                SumAccum<INT> @@high_risk_connections = 0;
                SetAccum<VERTEX> @@visited;
                SetAccum<VERTEX> @@direct_neighbors;
                
                Start = {{Entity.*}};
                Start = SELECT s FROM Start:s WHERE s.id == entityId;
                
                # Direct connections
                Direct = SELECT t FROM Start:s -(SIMILAR_TO:e)-> Entity:t
                ACCUM @@direct_connections += 1,
                      @@direct_neighbors += t;
                
                # Indirect connections (2-hop)
                Indirect = SELECT t FROM Direct:s -(SIMILAR_TO:e)-> Entity:t
                WHERE t NOT IN @@direct_neighbors AND t.id != entityId
                ACCUM @@indirect_connections += 1;
                
                # PEP and high-risk connections
                PEP = SELECT t FROM Start:s -(SIMILAR_TO:e)-> Entity:t
                WHERE t.pep_ind == true
                ACCUM @@pep_connections += 1;
                
                HighRisk = SELECT t FROM Start:s -(SIMILAR_TO:e)-> Entity:t
                WHERE t.risk_score > 0.7
                ACCUM @@high_risk_connections += 1;
                
                PRINT @@direct_connections, @@indirect_connections, @@pep_connections, @@high_risk_connections;
            }}
            """
            
            result = self.tg_client._execute_gsql(query)
            
            if result and 'results' in result and result['results']:
                stats = result['results'][0]
                features.update({
                    'direct_connections': stats[0],
                    'indirect_connections': stats[1],
                    'pep_connections': stats[2],
                    'high_risk_connections': stats[3],
                    'total_network_size': stats[0] + stats[1],
                    'network_density': self._calculate_network_density(entity_id)
                })
            else:
                features.update({
                    'direct_connections': 0,
                    'indirect_connections': 0,
                    'pep_connections': 0,
                    'high_risk_connections': 0,
                    'total_network_size': 0,
                    'network_density': 0.0
                })
                
        except Exception as e:
            logger.warning(f"Error generating network features for {entity_id}: {e}")
            features.update({
                'direct_connections': 0,
                'indirect_connections': 0,
                'pep_connections': 0,
                'high_risk_connections': 0,
                'total_network_size': 0,
                'network_density': 0.0
            })
        
        return features
    
    def _generate_temporal_features(self, entity_id: str) -> Dict[str, Any]:
        """Generate temporal features for an entity"""
        features = {}
        
        try:
            # Get temporal transaction patterns
            query = f"""
            CREATE QUERY getTemporalStats(STRING entityId) FOR GRAPH {self.tg_client.config['tigergraph']['graph_name']} {{
                SumAccum<INT> @@recent_transactions = 0;
                SumAccum<INT> @@old_transactions = 0;
                SumAccum<DOUBLE> @@recent_amount = 0;
                SumAccum<DOUBLE> @@old_amount = 0;
                MinAccum<DATETIME> @@first_transaction;
                MaxAccum<DATETIME> @@last_transaction;
                
                Start = {{Entity.*}};
                Start = SELECT s FROM Start:s WHERE s.id == entityId;
                
                Transactions = SELECT t FROM Start:s -(HAS_TRANSACTION:e)-> Transaction:t
                ACCUM @@first_transaction += t.transaction_date,
                      @@last_transaction += t.transaction_date;
                
                # Recent transactions (last 30 days)
                Recent = SELECT t FROM Start:s -(HAS_TRANSACTION:e)-> Transaction:t
                WHERE t.transaction_date > datetime_sub(now(), INTERVAL 30 DAY)
                ACCUM @@recent_transactions += 1,
                      @@recent_amount += t.amount;
                
                # Old transactions (older than 30 days)
                Old = SELECT t FROM Start:s -(HAS_TRANSACTION:e)-> Transaction:t
                WHERE t.transaction_date <= datetime_sub(now(), INTERVAL 30 DAY)
                ACCUM @@old_transactions += 1,
                      @@old_amount += t.amount;
                
                PRINT @@recent_transactions, @@old_transactions, @@recent_amount, @@old_amount, @@first_transaction, @@last_transaction;
            }}
            """
            
            result = self.tg_client._execute_gsql(query)
            
            if result and 'results' in result and result['results']:
                stats = result['results'][0]
                features.update({
                    'recent_transaction_count': stats[0],
                    'old_transaction_count': stats[1],
                    'recent_transaction_amount': stats[2],
                    'old_transaction_amount': stats[3],
                    'first_transaction_date': stats[4],
                    'last_transaction_date': stats[5],
                    'activity_recency': self._calculate_activity_recency(stats[5]),
                    'transaction_trend': self._calculate_transaction_trend(stats[0], stats[1])
                })
            else:
                features.update({
                    'recent_transaction_count': 0,
                    'old_transaction_count': 0,
                    'recent_transaction_amount': 0.0,
                    'old_transaction_amount': 0.0,
                    'first_transaction_date': None,
                    'last_transaction_date': None,
                    'activity_recency': 0.0,
                    'transaction_trend': 0.0
                })
                
        except Exception as e:
            logger.warning(f"Error generating temporal features for {entity_id}: {e}")
            features.update({
                'recent_transaction_count': 0,
                'old_transaction_count': 0,
                'recent_transaction_amount': 0.0,
                'old_transaction_amount': 0.0,
                'first_transaction_date': None,
                'last_transaction_date': None,
                'activity_recency': 0.0,
                'transaction_trend': 0.0
            })
        
        return features
    
    def _calculate_amount_variance(self, entity_id: str) -> float:
        """Calculate variance in transaction amounts"""
        try:
            query = f"""
            CREATE QUERY getAmountVariance(STRING entityId) FOR GRAPH {self.tg_client.config['tigergraph']['graph_name']} {{
                SumAccum<DOUBLE> @@sum = 0;
                SumAccum<DOUBLE> @@sum_sq = 0;
                SumAccum<INT> @@count = 0;
                
                Start = {{Entity.*}};
                Start = SELECT s FROM Start:s WHERE s.id == entityId;
                
                Transactions = SELECT t FROM Start:s -(HAS_TRANSACTION:e)-> Transaction:t
                ACCUM @@sum += t.amount,
                      @@sum_sq += t.amount * t.amount,
                      @@count += 1;
                
                DOUBLE variance = 0;
                IF @@count > 1 THEN
                    DOUBLE mean = @@sum / @@count;
                    variance = (@@sum_sq / @@count) - (mean * mean);
                END;
                
                PRINT variance;
            }}
            """
            
            result = self.tg_client._execute_gsql(query)
            
            if result and 'results' in result and result['results']:
                return result['results'][0][0]
            
            return 0.0
            
        except Exception as e:
            logger.warning(f"Error calculating amount variance for {entity_id}: {e}")
            return 0.0
    
    def _calculate_suspicious_pattern_score(self, entity_id: str) -> float:
        """Calculate suspicious pattern score"""
        try:
            # Check for structuring patterns (transactions just under reporting threshold)
            query = f"""
            CREATE QUERY getSuspiciousPatterns(STRING entityId) FOR GRAPH {self.tg_client.config['tigergraph']['graph_name']} {{
                SumAccum<INT> @@structuring_count = 0;
                SumAccum<INT> @@round_amount_count = 0;
                SumAccum<INT> @@high_frequency_count = 0;
                SumAccum<INT> @@total_transactions = 0;
                
                Start = {{Entity.*}};
                Start = SELECT s FROM Start:s WHERE s.id == entityId;
                
                Transactions = SELECT t FROM Start:s -(HAS_TRANSACTION:e)-> Transaction:t
                ACCUM @@total_transactions += 1,
                      IF t.amount >= 9000 AND t.amount < 10000 THEN
                          @@structuring_count += 1
                      END,
                      IF t.amount IN [50000, 100000, 250000, 500000, 1000000] THEN
                          @@round_amount_count += 1
                      END;
                
                # High frequency (multiple transactions in short time)
                # This would need more complex temporal analysis
                
                DOUBLE suspicious_score = 0;
                IF @@total_transactions > 0 THEN
                    suspicious_score = (@@structuring_count + @@round_amount_count) / @@total_transactions;
                END;
                
                PRINT suspicious_score;
            }}
            """
            
            result = self.tg_client._execute_gsql(query)
            
            if result and 'results' in result and result['results']:
                return result['results'][0][0]
            
            return 0.0
            
        except Exception as e:
            logger.warning(f"Error calculating suspicious pattern score for {entity_id}: {e}")
            return 0.0
    
    def _calculate_network_density(self, entity_id: str) -> float:
        """Calculate network density around the entity"""
        try:
            # Get direct neighbors and their connections
            query = f"""
            CREATE QUERY getNetworkDensity(STRING entityId) FOR GRAPH {self.tg_client.config['tigergraph']['graph_name']} {{
                SumAccum<INT> @@actual_edges = 0;
                SumAccum<INT> @@possible_edges = 0;
                SetAccum<VERTEX> @@neighbors;
                
                Start = {{Entity.*}};
                Start = SELECT s FROM Start:s WHERE s.id == entityId;
                
                # Get direct neighbors
                Neighbors = SELECT t FROM Start:s -(SIMILAR_TO:e)-> Entity:t
                ACCUM @@neighbors += t;
                
                # Count actual edges between neighbors
                NeighborEdges = SELECT t FROM Neighbors:s -(SIMILAR_TO:e)-> Entity:t
                WHERE t IN @@neighbors
                ACCUM @@actual_edges += 1;
                
                # Calculate possible edges
                INT neighbor_count = @@neighbors.size();
                IF neighbor_count > 1 THEN
                    @@possible_edges = neighbor_count * (neighbor_count - 1) / 2;
                END;
                
                DOUBLE density = 0;
                IF @@possible_edges > 0 THEN
                    density = @@actual_edges / @@possible_edges;
                END;
                
                PRINT density;
            }}
            """
            
            result = self.tg_client._execute_gsql(query)
            
            if result and 'results' in result and result['results']:
                return result['results'][0][0]
            
            return 0.0
            
        except Exception as e:
            logger.warning(f"Error calculating network density for {entity_id}: {e}")
            return 0.0
    
    def _calculate_activity_recency(self, last_transaction_date: str) -> float:
        """Calculate activity recency score"""
        if not last_transaction_date:
            return 0.0
        
        try:
            # Parse date and calculate days since last activity
            last_date = datetime.fromisoformat(last_transaction_date.replace('Z', '+00:00'))
            days_since = (datetime.now() - last_date).days
            
            # Convert to recency score (0-1, where 1 is very recent)
            if days_since <= 1:
                return 1.0
            elif days_since <= 7:
                return 0.8
            elif days_since <= 30:
                return 0.6
            elif days_since <= 90:
                return 0.4
            elif days_since <= 365:
                return 0.2
            else:
                return 0.0
                
        except Exception as e:
            logger.warning(f"Error calculating activity recency: {e}")
            return 0.0
    
    def _calculate_transaction_trend(self, recent_count: int, old_count: int) -> float:
        """Calculate transaction trend (increasing/decreasing activity)"""
        if old_count == 0:
            return 0.0 if recent_count == 0 else 1.0
        
        # Calculate trend as ratio of recent to old transactions
        trend = recent_count / old_count
        
        # Normalize to 0-1 range
        if trend >= 2.0:
            return 1.0  # Strongly increasing
        elif trend >= 1.5:
            return 0.8  # Increasing
        elif trend >= 0.8:
            return 0.5  # Stable
        elif trend >= 0.5:
            return 0.3  # Decreasing
        else:
            return 0.0  # Strongly decreasing
    
    def save_features_to_csv(self, features_df: pd.DataFrame, filename: str):
        """Save features to CSV file"""
        try:
            features_df.to_csv(filename, index=False)
            logger.info(f"Saved features to {filename}")
        except Exception as e:
            logger.error(f"Error saving features: {e}")
            raise
    
    def get_feature_summary(self, features_df: pd.DataFrame) -> Dict:
        """Get summary statistics of generated features"""
        summary = {
            'total_entities': len(features_df),
            'feature_columns': list(features_df.columns),
            'numeric_features': features_df.select_dtypes(include=[np.number]).columns.tolist(),
            'categorical_features': features_df.select_dtypes(include=['object']).columns.tolist()
        }
        
        # Add statistics for numeric features
        numeric_stats = {}
        for col in summary['numeric_features']:
            if col != 'entity_id':
                numeric_stats[col] = {
                    'mean': features_df[col].mean(),
                    'std': features_df[col].std(),
                    'min': features_df[col].min(),
                    'max': features_df[col].max(),
                    'median': features_df[col].median()
                }
        
        summary['numeric_statistics'] = numeric_stats
        
        return summary 