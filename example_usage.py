#!/usr/bin/env python3
"""
Example usage of the Entity Resolution Pipeline
Demonstrates how to use the complete pipeline with sample data
"""

import sys
import os
import logging
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from core.entity_resolution_pipeline import EntityResolutionPipeline

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main function to demonstrate the pipeline"""
    
    # Check if sample data exists
    customer_data_path = "data/sample_customer_large.csv"
    transaction_data_path = "data/sample_trnx_large.csv"
    
    if not os.path.exists(customer_data_path):
        logger.error(f"Customer data not found at {customer_data_path}")
        logger.info("Please run src/data_synthesizer/generate_sample_data.py first to create sample data")
        return
    
    if not os.path.exists(transaction_data_path):
        logger.error(f"Transaction data not found at {transaction_data_path}")
        logger.info("Please run src/data_synthesizer/generate_sample_data.py first to create sample data")
        return
    
    try:
        # Initialize the pipeline
        logger.info("Initializing Entity Resolution Pipeline...")
        pipeline = EntityResolutionPipeline()
        
        # Run the full pipeline
        logger.info("Running full pipeline...")
        results = pipeline.run_full_pipeline(
            customer_data_path=customer_data_path,
            transaction_data_path=transaction_data_path,
            output_dir="output"
        )
        
        # Print summary
        logger.info("Pipeline completed successfully!")
        print("\n" + "="*50)
        print("PIPELINE SUMMARY")
        print("="*50)
        
        summary = results['summary']
        
        # Entity Resolution Summary
        entity_summary = summary['entity_resolution']
        print(f"\nEntity Resolution:")
        print(f"  Total entities: {entity_summary.get('total_entities', 0)}")
        print(f"  Individual entities: {entity_summary.get('individual_entities', 0)}")
        print(f"  Business entities: {entity_summary.get('business_entities', 0)}")
        print(f"  PEP entities: {entity_summary.get('pep_entities', 0)}")
        print(f"  Average confidence: {entity_summary.get('avg_confidence', 0):.3f}")
        print(f"  Average risk score: {entity_summary.get('avg_risk_score', 0):.3f}")
        
        # Graph Statistics
        graph_stats = summary['graph_statistics']
        print(f"\nGraph Statistics:")
        print(f"  Vertices: {graph_stats.get('vertex_count', 0)}")
        print(f"  Edges: {graph_stats.get('edge_count', 0)}")
        
        # Risk Analysis
        risk_analysis = summary['risk_analysis']
        print(f"\nRisk Analysis:")
        print(f"  High risk entities: {risk_analysis.get('high_risk_entities', 0)}")
        print(f"  PEP entities: {risk_analysis.get('pep_entities', 0)}")
        print(f"  Suspicious patterns: {risk_analysis.get('suspicious_patterns', 0)}")
        print(f"  High transaction volume: {risk_analysis.get('high_transaction_volume', 0)}")
        
        risk_dist = risk_analysis.get('risk_distribution', {})
        print(f"  Risk distribution:")
        print(f"    Low risk: {risk_dist.get('low_risk', 0)}")
        print(f"    Medium risk: {risk_dist.get('medium_risk', 0)}")
        print(f"    High risk: {risk_dist.get('high_risk', 0)}")
        
        # Feature Summary
        feature_summary = summary['feature_summary']
        print(f"\nFeature Generation:")
        print(f"  Total entities with features: {feature_summary.get('total_entities', 0)}")
        print(f"  Number of features: {len(feature_summary.get('feature_columns', []))}")
        print(f"  Numeric features: {len(feature_summary.get('numeric_features', []))}")
        
        # Example: Get top entities by PageRank
        print(f"\n" + "="*50)
        print("TOP ENTITIES BY PAGERANK")
        print("="*50)
        
        page_rank_results = pipeline.run_page_rank_analysis()
        if page_rank_results and 'top_entities' in page_rank_results:
            top_entities = page_rank_results['top_entities'][:10]
            for i, entity in enumerate(top_entities, 1):
                print(f"{i:2d}. {entity['entity_id']}: {entity['page_rank_score']:.6f}")
        
        # Example: Get top entities by transaction amount
        print(f"\n" + "="*50)
        print("TOP ENTITIES BY TRANSACTION AMOUNT")
        print("="*50)
        
        top_by_amount = pipeline.get_top_entities_by_feature('total_transaction_amount', top_n=10)
        if not top_by_amount.empty:
            for i, row in top_by_amount.iterrows():
                entity_id = row['entity_id']
                amount = row['total_transaction_amount']
                entity_type = row['entity_type']
                print(f"{i+1:2d}. {entity_id} ({entity_type}): ${amount:,.2f}")
        
        # Example: Get details for a specific entity
        print(f"\n" + "="*50)
        print("ENTITY DETAILS EXAMPLE")
        print("="*50)
        
        if results['entities']:
            # Get details for the first entity
            first_entity_id = results['entities'][0]['entity_id']
            entity_details = pipeline.get_entity_details(first_entity_id)
            
            if entity_details:
                entity = entity_details['entity']
                features = entity_details['features']
                
                print(f"Entity ID: {entity['entity_id']}")
                print(f"Type: {entity['entity_type']}")
                print(f"Primary Name: {entity['primary_name']}")
                print(f"PEP Status: {entity['pep_ind']}")
                print(f"Risk Score: {entity['risk_score']:.3f}")
                print(f"Confidence: {entity['confidence']:.3f}")
                print(f"Record Count: {entity['record_count']}")
                
                if features:
                    print(f"\nKey Features:")
                    print(f"  PageRank Score: {features.get('page_rank_score', 0):.6f}")
                    print(f"  Transaction Count: {features.get('transaction_count', 0)}")
                    print(f"  Total Amount: ${features.get('total_transaction_amount', 0):,.2f}")
                    print(f"  Direct Connections: {features.get('direct_connections', 0)}")
                    print(f"  PEP Connections: {features.get('pep_connections', 0)}")
        
        print(f"\n" + "="*50)
        print("OUTPUT FILES")
        print("="*50)
        print("Results have been saved to the 'output' directory:")
        print("  - entities_with_features.csv: Complete entity data with features")
        print("  - features.csv: All generated features")
        print("  - entity_mapping.json: Mapping from customer names to entity IDs")
        print("  - pipeline_summary.json: Complete pipeline summary")
        
        print(f"\nPipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Error running pipeline: {e}")
        raise

def run_entity_resolution_only():
    """Run only entity resolution without TigerGraph (for testing)"""
    
    from core.entity_resolution import EntityResolver
    import pandas as pd
    
    customer_data_path = "data/sample_customer_large.csv"
    
    if not os.path.exists(customer_data_path):
        logger.error(f"Customer data not found at {customer_data_path}")
        return
    
    try:
        # Load customer data
        df = pd.read_csv(customer_data_path)
        customers = df.to_dict('records')
        
        # Run entity resolution
        resolver = EntityResolver()
        entities = resolver.resolve_entities(customers)
        
        # Save results
        resolver.save_entities_to_csv('output/entities_only.csv')
        
        # Print summary
        summary = resolver.get_entity_summary()
        print(f"\nEntity Resolution Results:")
        print(f"  Total entities: {summary['total_entities']}")
        print(f"  Individual entities: {summary['individual_entities']}")
        print(f"  Business entities: {summary['business_entities']}")
        print(f"  PEP entities: {summary['pep_entities']}")
        print(f"  Average confidence: {summary['avg_confidence']:.3f}")
        
    except Exception as e:
        logger.error(f"Error running entity resolution: {e}")
        raise

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Entity Resolution Pipeline Example')
    parser.add_argument('--entity-only', action='store_true', 
                       help='Run only entity resolution without TigerGraph')
    
    args = parser.parse_args()
    
    if args.entity_only:
        print("Running entity resolution only...")
        run_entity_resolution_only()
    else:
        print("Running full pipeline...")
        main() 