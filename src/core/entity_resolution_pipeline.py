#!/usr/bin/env python3
"""
Entity Resolution Pipeline
Main orchestrator for entity resolution, graph generation, and feature extraction
"""

import pandas as pd
import logging
from typing import Dict, List, Any, Optional
import json
from datetime import datetime
import os

from .entity_resolution import EntityResolver
from .tigergraph_client import TigerGraphClient
from .feature_generator import FeatureGenerator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EntityResolutionPipeline:
    """
    Main pipeline for entity resolution, graph generation, and feature extraction
    """
    
    def __init__(self, 
                 resolution_config: str = "config/resolution_config.yaml",
                 tigergraph_config: str = "config/tigergraph_config.yaml"):
        """Initialize the pipeline"""
        self.entity_resolver = EntityResolver(resolution_config)
        self.tg_client = TigerGraphClient(tigergraph_config)
        self.feature_generator = FeatureGenerator(self.tg_client)
        
        # Pipeline state
        self.entities = []
        self.transactions = []
        self.entity_mapping = {}
        self.features_df = None
        
    def run_full_pipeline(self, 
                         customer_data_path: str,
                         transaction_data_path: str,
                         output_dir: str = "output") -> Dict[str, Any]:
        """
        Run the complete entity resolution pipeline
        
        Args:
            customer_data_path: Path to customer data CSV
            transaction_data_path: Path to transaction data CSV
            output_dir: Directory to save outputs
            
        Returns:
            Dictionary with pipeline results and statistics
        """
        logger.info("Starting full entity resolution pipeline")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Step 1: Load and preprocess data
        logger.info("Step 1: Loading and preprocessing data")
        customers = self._load_customer_data(customer_data_path)
        transactions = self._load_transaction_data(transaction_data_path)
        
        # Step 2: Entity Resolution
        logger.info("Step 2: Running entity resolution")
        entities = self._run_entity_resolution(customers)
        
        # Step 3: Create entity mapping for transactions
        logger.info("Step 3: Creating entity mapping")
        entity_mapping = self._create_entity_mapping(entities)
        
        # Step 4: Graph Generation
        logger.info("Step 4: Generating graph in TigerGraph")
        self._generate_graph(entities, transactions, entity_mapping)
        
        # Step 5: Feature Generation
        logger.info("Step 5: Generating graph-based features")
        features_df = self._generate_features(entities)
        
        # Step 6: Save results
        logger.info("Step 6: Saving results")
        results = self._save_results(output_dir, entities, features_df)
        
        # Step 7: Generate summary
        logger.info("Step 7: Generating summary")
        summary = self._generate_summary(entities, features_df)
        
        logger.info("Pipeline completed successfully")
        return {
            'summary': summary,
            'results': results,
            'entities': entities,
            'features': features_df
        }
    
    def _load_customer_data(self, file_path: str) -> List[Dict]:
        """Load customer data from CSV"""
        try:
            df = pd.read_csv(file_path)
            customers = df.to_dict('records')
            logger.info(f"Loaded {len(customers)} customer records")
            return customers
        except Exception as e:
            logger.error(f"Error loading customer data: {e}")
            raise
    
    def _load_transaction_data(self, file_path: str) -> List[Dict]:
        """Load transaction data from CSV"""
        try:
            df = pd.read_csv(file_path)
            transactions = df.to_dict('records')
            logger.info(f"Loaded {len(transactions)} transaction records")
            return transactions
        except Exception as e:
            logger.error(f"Error loading transaction data: {e}")
            raise
    
    def _run_entity_resolution(self, customers: List[Dict]) -> List[Dict]:
        """Run entity resolution on customer data"""
        try:
            entities = self.entity_resolver.resolve_entities(customers)
            self.entities = entities
            
            # Save entities to CSV
            self.entity_resolver.save_entities_to_csv('output/resolved_entities.csv')
            
            # Get summary
            summary = self.entity_resolver.get_entity_summary()
            logger.info(f"Entity resolution summary: {summary}")
            
            return entities
            
        except Exception as e:
            logger.error(f"Error in entity resolution: {e}")
            raise
    
    def _create_entity_mapping(self, entities: List[Dict]) -> Dict[str, str]:
        """Create mapping from customer names to entity IDs"""
        entity_mapping = {}
        
        for entity in entities:
            for record in entity['records']:
                customer_name = record.get('name', '')
                if customer_name:
                    entity_mapping[customer_name] = entity['entity_id']
        
        self.entity_mapping = entity_mapping
        logger.info(f"Created entity mapping for {len(entity_mapping)} customer names")
        
        return entity_mapping
    
    def _generate_graph(self, entities: List[Dict], transactions: List[Dict], entity_mapping: Dict[str, str]):
        """Generate graph in TigerGraph"""
        try:
            # Create graph schema
            self.tg_client.create_graph_schema()
            
            # Upsert entities
            self.tg_client.upsert_entities(entities)
            
            # Upsert transactions
            self.tg_client.upsert_transactions(transactions)
            
            # Create transaction edges
            self.tg_client.create_transaction_edges(transactions, entity_mapping)
            
            # Create similarity edges
            self.tg_client.create_similarity_edges(entities, similarity_threshold=0.8)
            
            # Get graph statistics
            stats = self.tg_client.get_graph_statistics()
            logger.info(f"Graph statistics: {stats}")
            
        except Exception as e:
            logger.error(f"Error generating graph: {e}")
            raise
    
    def _generate_features(self, entities: List[Dict]) -> pd.DataFrame:
        """Generate graph-based features"""
        try:
            features_df = self.feature_generator.generate_all_features(entities)
            self.features_df = features_df
            
            # Save features
            self.feature_generator.save_features_to_csv(features_df, 'output/entity_features.csv')
            
            # Get feature summary
            feature_summary = self.feature_generator.get_feature_summary(features_df)
            logger.info(f"Feature generation summary: {feature_summary}")
            
            return features_df
            
        except Exception as e:
            logger.error(f"Error generating features: {e}")
            raise
    
    def _save_results(self, output_dir: str, entities: List[Dict], features_df: pd.DataFrame) -> Dict[str, str]:
        """Save all pipeline results"""
        results = {}
        
        try:
            # Save entities with features
            entities_with_features = []
            for entity in entities:
                entity_features = features_df[features_df['entity_id'] == entity['entity_id']]
                if not entity_features.empty:
                    entity_row = entity_features.iloc[0].to_dict()
                    entity_with_features = {**entity, **entity_row}
                    entities_with_features.append(entity_with_features)
            
            # Save to JSON
            entities_file = os.path.join(output_dir, 'entities_with_features.json')
            with open(entities_file, 'w') as f:
                json.dump(entities_with_features, f, indent=2, default=str)
            results['entities_json'] = entities_file
            
            # Save to CSV
            entities_csv = os.path.join(output_dir, 'entities_with_features.csv')
            entities_df = pd.DataFrame(entities_with_features)
            entities_df.to_csv(entities_csv, index=False)
            results['entities_csv'] = entities_csv
            
            # Save entity mapping
            mapping_file = os.path.join(output_dir, 'entity_mapping.json')
            with open(mapping_file, 'w') as f:
                json.dump(self.entity_mapping, f, indent=2)
            results['entity_mapping'] = mapping_file
            
            # Save features separately
            features_file = os.path.join(output_dir, 'features.csv')
            features_df.to_csv(features_file, index=False)
            results['features'] = features_file
            
            logger.info(f"Results saved to {output_dir}")
            
        except Exception as e:
            logger.error(f"Error saving results: {e}")
            raise
        
        return results
    
    def _generate_summary(self, entities: List[Dict], features_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive summary of the pipeline"""
        try:
            # Entity resolution summary
            entity_summary = self.entity_resolver.get_entity_summary()
            
            # Graph statistics
            graph_stats = self.tg_client.get_graph_statistics()
            
            # Feature summary
            feature_summary = self.feature_generator.get_feature_summary(features_df)
            
            # Risk analysis
            risk_analysis = self._analyze_risk_patterns(entities, features_df)
            
            # Performance metrics
            performance_metrics = {
                'total_processing_time': datetime.now().isoformat(),
                'entities_processed': len(entities),
                'features_generated': len(features_df.columns) if features_df is not None else 0
            }
            
            summary = {
                'entity_resolution': entity_summary,
                'graph_statistics': graph_stats,
                'feature_summary': feature_summary,
                'risk_analysis': risk_analysis,
                'performance': performance_metrics
            }
            
            # Save summary
            summary_file = 'output/pipeline_summary.json'
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            raise
    
    def _analyze_risk_patterns(self, entities: List[Dict], features_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze risk patterns in the data"""
        try:
            risk_analysis = {
                'high_risk_entities': 0,
                'pep_entities': 0,
                'suspicious_patterns': 0,
                'high_transaction_volume': 0,
                'risk_distribution': {}
            }
            
            if features_df is not None:
                # High risk entities (risk_score > 0.7)
                high_risk = features_df[features_df['risk_score'] > 0.7]
                risk_analysis['high_risk_entities'] = len(high_risk)
                
                # PEP entities
                pep_entities = features_df[features_df['pep_ind'] == True]
                risk_analysis['pep_entities'] = len(pep_entities)
                
                # Suspicious patterns
                suspicious = features_df[features_df['suspicious_pattern_score'] > 0.5]
                risk_analysis['suspicious_patterns'] = len(suspicious)
                
                # High transaction volume
                high_volume = features_df[features_df['total_transaction_amount'] > 1000000]
                risk_analysis['high_transaction_volume'] = len(high_volume)
                
                # Risk distribution
                risk_analysis['risk_distribution'] = {
                    'low_risk': len(features_df[features_df['risk_score'] <= 0.3]),
                    'medium_risk': len(features_df[(features_df['risk_score'] > 0.3) & (features_df['risk_score'] <= 0.7)]),
                    'high_risk': len(features_df[features_df['risk_score'] > 0.7])
                }
            
            return risk_analysis
            
        except Exception as e:
            logger.error(f"Error analyzing risk patterns: {e}")
            return {}
    
    def get_top_entities_by_feature(self, feature: str, top_n: int = 10) -> pd.DataFrame:
        """Get top entities by a specific feature"""
        if self.features_df is None:
            logger.warning("No features available. Run the pipeline first.")
            return pd.DataFrame()
        
        if feature not in self.features_df.columns:
            logger.warning(f"Feature '{feature}' not found in available features")
            return pd.DataFrame()
        
        top_entities = self.features_df.nlargest(top_n, feature)
        return top_entities[['entity_id', 'entity_type', feature]]
    
    def get_entity_details(self, entity_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific entity"""
        try:
            # Find entity
            entity = next((e for e in self.entities if e['entity_id'] == entity_id), None)
            if not entity:
                logger.warning(f"Entity {entity_id} not found")
                return {}
            
            # Get features
            features = {}
            if self.features_df is not None:
                entity_features = self.features_df[self.features_df['entity_id'] == entity_id]
                if not entity_features.empty:
                    features = entity_features.iloc[0].to_dict()
            
            # Get neighbors
            neighbors = self.tg_client.get_entity_neighbors(entity_id, max_depth=2)
            
            return {
                'entity': entity,
                'features': features,
                'neighbors': neighbors
            }
            
        except Exception as e:
            logger.error(f"Error getting entity details: {e}")
            return {}
    
    def run_page_rank_analysis(self) -> Dict[str, float]:
        """Run PageRank analysis and return top entities"""
        try:
            page_rank_scores = self.tg_client.run_page_rank()
            
            # Convert to DataFrame for easier analysis
            page_rank_df = pd.DataFrame([
                {'entity_id': entity_id, 'page_rank_score': score}
                for entity_id, score in page_rank_scores.items()
            ])
            
            # Get top entities
            top_entities = page_rank_df.nlargest(20, 'page_rank_score')
            
            return {
                'scores': page_rank_scores,
                'top_entities': top_entities.to_dict('records')
            }
            
        except Exception as e:
            logger.error(f"Error running PageRank analysis: {e}")
            return {} 