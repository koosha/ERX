#!/usr/bin/env python3
"""
TigerGraph Client Module
Handles graph operations and integration with TigerGraph database
"""

import requests
import json
import logging
import yaml
from typing import Dict, List, Any, Optional
import pandas as pd
from datetime import datetime
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TigerGraphClient:
    """
    TigerGraph client for graph operations
    """
    
    def __init__(self, config_path: str = "config/tigergraph_config.yaml"):
        """Initialize TigerGraph client"""
        self.config = self._load_config(config_path)
        self.base_url = self._get_base_url()
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        # Authentication
        self._authenticate()
        
    def _load_config(self, config_path: str) -> Dict:
        """Load TigerGraph configuration"""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            return config
        except Exception as e:
            logger.error(f"Error loading TigerGraph config: {e}")
            raise
    
    def _get_base_url(self) -> str:
        """Get base URL for TigerGraph REST API"""
        protocol = "https" if self.config['tigergraph']['https'] else "http"
        host = self.config['tigergraph']['host']
        port = self.config['tigergraph']['rest_port']
        return f"{protocol}://{host}:{port}"
    
    def _authenticate(self):
        """Authenticate with TigerGraph"""
        try:
            auth_url = f"{self.base_url}/requesttoken"
            auth_data = {
                "graph": self.config['tigergraph']['graph_name'],
                "username": self.config['tigergraph']['username'],
                "password": self.config['tigergraph']['password']
            }
            
            if self.config['tigergraph']['secret']:
                auth_data["secret"] = self.config['tigergraph']['secret']
            
            response = self.session.post(auth_url, json=auth_data)
            response.raise_for_status()
            
            token_data = response.json()
            if 'results' in token_data and 'token' in token_data['results']:
                token = token_data['results']['token']
                self.session.headers.update({'Authorization': f'Bearer {token}'})
                logger.info("Successfully authenticated with TigerGraph")
            else:
                logger.warning("No token received from authentication")
                
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            raise
    
    def create_graph_schema(self):
        """Create graph schema in TigerGraph"""
        try:
            # Define vertex types
            vertex_types = [
                {
                    "name": "Entity",
                    "attributes": [
                        {"name": "id", "type": "STRING", "is_primary": True},
                        {"name": "type", "type": "STRING"},
                        {"name": "confidence", "type": "DOUBLE"},
                        {"name": "pep_ind", "type": "BOOL"},
                        {"name": "risk_score", "type": "DOUBLE"},
                        {"name": "primary_name", "type": "STRING"},
                        {"name": "primary_email", "type": "STRING"},
                        {"name": "primary_phone", "type": "STRING"},
                        {"name": "primary_address", "type": "STRING"},
                        {"name": "record_count", "type": "INT"},
                        {"name": "sources", "type": "STRING"},
                        {"name": "created_at", "type": "DATETIME"},
                        {"name": "updated_at", "type": "DATETIME"}
                    ]
                },
                {
                    "name": "Transaction",
                    "attributes": [
                        {"name": "id", "type": "STRING", "is_primary": True},
                        {"name": "amount", "type": "DOUBLE"},
                        {"name": "currency", "type": "STRING"},
                        {"name": "transaction_type", "type": "STRING"},
                        {"name": "transaction_date", "type": "DATETIME"},
                        {"name": "status", "type": "STRING"},
                        {"name": "description", "type": "STRING"},
                        {"name": "originator_name", "type": "STRING"},
                        {"name": "beneficiary_name", "type": "STRING"},
                        {"name": "originator_country", "type": "STRING"},
                        {"name": "beneficiary_country", "type": "STRING"}
                    ]
                }
            ]
            
            # Define edge types
            edge_types = [
                {
                    "name": "HAS_TRANSACTION",
                    "from_vertex": "Entity",
                    "to_vertex": "Transaction",
                    "attributes": [
                        {"name": "role", "type": "STRING"},  # "originator" or "beneficiary"
                        {"name": "created_at", "type": "DATETIME"}
                    ]
                },
                {
                    "name": "SIMILAR_TO",
                    "from_vertex": "Entity",
                    "to_vertex": "Entity",
                    "attributes": [
                        {"name": "similarity_score", "type": "DOUBLE"},
                        {"name": "matching_fields", "type": "STRING"},
                        {"name": "created_at", "type": "DATETIME"}
                    ]
                }
            ]
            
            # Create schema using GSQL
            gsql_commands = []
            
            # Drop existing schema if exists
            gsql_commands.append(f"DROP GRAPH {self.config['tigergraph']['graph_name']}")
            
            # Create graph
            gsql_commands.append(f"CREATE GRAPH {self.config['tigergraph']['graph_name']}()")
            
            # Create vertex types
            for vertex in vertex_types:
                attrs = ", ".join([f"{attr['name']} {attr['type']}" for attr in vertex['attributes']])
                gsql_commands.append(f"CREATE VERTEX {vertex['name']} ({attrs})")
            
            # Create edge types
            for edge in edge_types:
                attrs = ", ".join([f"{attr['name']} {attr['type']}" for attr in edge['attributes']])
                gsql_commands.append(
                    f"CREATE UNDIRECTED EDGE {edge['name']} "
                    f"({attrs}) FROM {edge['from_vertex']} TO {edge['to_vertex']}"
                )
            
            # Execute GSQL commands
            for command in gsql_commands:
                self._execute_gsql(command)
            
            logger.info("Graph schema created successfully")
            
        except Exception as e:
            logger.error(f"Error creating graph schema: {e}")
            raise
    
    def _execute_gsql(self, command: str) -> Dict:
        """Execute GSQL command"""
        try:
            gsql_url = f"{self.base_url}/gsqlserver/gsql"
            data = {"command": command}
            
            response = self.session.post(gsql_url, json=data)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"GSQL execution failed: {e}")
            raise
    
    def upsert_entities(self, entities: List[Dict]):
        """Upsert entities into TigerGraph"""
        try:
            logger.info(f"Upserting {len(entities)} entities")
            
            # Prepare entity data
            entity_data = []
            for entity in entities:
                entity_vertex = {
                    "id": entity['entity_id'],
                    "type": entity['entity_type'],
                    "confidence": entity['confidence'],
                    "pep_ind": entity['pep_ind'],
                    "risk_score": entity['risk_score'],
                    "primary_name": entity['primary_name'],
                    "primary_email": entity['primary_email'],
                    "primary_phone": entity['primary_phone'],
                    "primary_address": entity['primary_address'],
                    "record_count": entity['record_count'],
                    "sources": ";".join(entity['sources']),
                    "created_at": entity['created_at'],
                    "updated_at": entity['updated_at']
                }
                entity_data.append(entity_vertex)
            
            # Upsert vertices
            self._upsert_vertices("Entity", entity_data)
            
            logger.info("Entities upserted successfully")
            
        except Exception as e:
            logger.error(f"Error upserting entities: {e}")
            raise
    
    def upsert_transactions(self, transactions: List[Dict]):
        """Upsert transactions into TigerGraph"""
        try:
            logger.info(f"Upserting {len(transactions)} transactions")
            
            # Prepare transaction data
            transaction_data = []
            for txn in transactions:
                transaction_vertex = {
                    "id": txn['transaction_id'],
                    "amount": float(txn['transaction_amount']),
                    "currency": txn['currency'],
                    "transaction_type": txn['transaction_type'],
                    "transaction_date": txn['transaction_date'],
                    "status": txn['transaction_status'],
                    "description": txn['transaction_description'],
                    "originator_name": txn['originator_name'],
                    "beneficiary_name": txn['beneficiary_name'],
                    "originator_country": txn['originator_country'],
                    "beneficiary_country": txn['beneficiary_country']
                }
                transaction_data.append(transaction_vertex)
            
            # Upsert vertices
            self._upsert_vertices("Transaction", transaction_data)
            
            logger.info("Transactions upserted successfully")
            
        except Exception as e:
            logger.error(f"Error upserting transactions: {e}")
            raise
    
    def create_transaction_edges(self, transactions: List[Dict], entity_mapping: Dict[str, str]):
        """Create edges between entities and transactions"""
        try:
            logger.info("Creating transaction edges")
            
            edges = []
            for txn in transactions:
                # Find originator entity
                originator_name = txn['originator_name']
                originator_entity = entity_mapping.get(originator_name)
                
                # Find beneficiary entity
                beneficiary_name = txn['beneficiary_name']
                beneficiary_entity = entity_mapping.get(beneficiary_name)
                
                if originator_entity:
                    edges.append({
                        "from": originator_entity,
                        "to": txn['transaction_id'],
                        "attributes": {
                            "role": "originator",
                            "created_at": txn['transaction_date']
                        }
                    })
                
                if beneficiary_entity:
                    edges.append({
                        "from": beneficiary_entity,
                        "to": txn['transaction_id'],
                        "attributes": {
                            "role": "beneficiary",
                            "created_at": txn['transaction_date']
                        }
                    })
            
            # Upsert edges
            self._upsert_edges("HAS_TRANSACTION", edges)
            
            logger.info(f"Created {len(edges)} transaction edges")
            
        except Exception as e:
            logger.error(f"Error creating transaction edges: {e}")
            raise
    
    def create_similarity_edges(self, entities: List[Dict], similarity_threshold: float = 0.8):
        """Create similarity edges between entities"""
        try:
            logger.info("Creating similarity edges")
            
            edges = []
            for i, entity1 in enumerate(entities):
                for j, entity2 in enumerate(entities[i+1:], i+1):
                    # Calculate similarity (simplified)
                    similarity = self._calculate_entity_similarity(entity1, entity2)
                    
                    if similarity >= similarity_threshold:
                        edges.append({
                            "from": entity1['entity_id'],
                            "to": entity2['entity_id'],
                            "attributes": {
                                "similarity_score": similarity,
                                "matching_fields": "name,email,phone",
                                "created_at": datetime.now().isoformat()
                            }
                        })
            
            # Upsert edges
            self._upsert_edges("SIMILAR_TO", edges)
            
            logger.info(f"Created {len(edges)} similarity edges")
            
        except Exception as e:
            logger.error(f"Error creating similarity edges: {e}")
            raise
    
    def _calculate_entity_similarity(self, entity1: Dict, entity2: Dict) -> float:
        """Calculate similarity between two entities"""
        # Simple similarity calculation based on primary fields
        similarities = []
        
        # Name similarity
        if entity1['primary_name'] and entity2['primary_name']:
            name_sim = self._string_similarity(entity1['primary_name'], entity2['primary_name'])
            similarities.append(name_sim * 0.4)
        
        # Email similarity
        if entity1['primary_email'] and entity2['primary_email']:
            email_sim = 1.0 if entity1['primary_email'] == entity2['primary_email'] else 0.0
            similarities.append(email_sim * 0.3)
        
        # Phone similarity
        if entity1['primary_phone'] and entity2['primary_phone']:
            phone_sim = 1.0 if entity1['primary_phone'] == entity2['primary_phone'] else 0.0
            similarities.append(phone_sim * 0.2)
        
        # Address similarity
        if entity1['primary_address'] and entity2['primary_address']:
            addr_sim = self._string_similarity(entity1['primary_address'], entity2['primary_address'])
            similarities.append(addr_sim * 0.1)
        
        return sum(similarities) if similarities else 0.0
    
    def _string_similarity(self, str1: str, str2: str) -> float:
        """Calculate string similarity using simple ratio"""
        if not str1 or not str2:
            return 0.0
        
        # Simple character-based similarity
        set1 = set(str1.lower())
        set2 = set(str2.lower())
        
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        return intersection / union if union > 0 else 0.0
    
    def _upsert_vertices(self, vertex_type: str, vertices: List[Dict]):
        """Upsert vertices to TigerGraph"""
        try:
            url = f"{self.base_url}/graph/{self.config['tigergraph']['graph_name']}/vertices/{vertex_type}"
            
            for vertex in vertices:
                response = self.session.post(url, json=vertex)
                response.raise_for_status()
                
        except Exception as e:
            logger.error(f"Error upserting vertices: {e}")
            raise
    
    def _upsert_edges(self, edge_type: str, edges: List[Dict]):
        """Upsert edges to TigerGraph"""
        try:
            url = f"{self.base_url}/graph/{self.config['tigergraph']['graph_name']}/edges/{edge_type}"
            
            for edge in edges:
                edge_data = {
                    "from": edge["from"],
                    "to": edge["to"],
                    "attributes": edge["attributes"]
                }
                
                response = self.session.post(url, json=edge_data)
                response.raise_for_status()
                
        except Exception as e:
            logger.error(f"Error upserting edges: {e}")
            raise
    
    def run_page_rank(self, max_iter: int = 20, damping_factor: float = 0.85) -> Dict[str, float]:
        """Run PageRank algorithm on the graph"""
        try:
            logger.info("Running PageRank algorithm")
            
            # GSQL query for PageRank
            query = f"""
            CREATE QUERY pageRank(INT maxIter = {max_iter}, FLOAT damping = {damping_factor}) FOR GRAPH {self.config['tigergraph']['graph_name']} {{
                MaxAccum<FLOAT> @@maxScore;
                SumAccum<FLOAT> @score = 1;
                SumAccum<FLOAT> @receivedScore = 0;
                SumAccum<INT> @outDegree = 0;
                SetAccum<EDGE> @@edgeSet;
                
                Start = {{Entity.*}};
                
                # Initialize
                Start = SELECT s FROM Start:s ACCUM s.@outDegree = s.outdegree("HAS_TRANSACTION");
                
                # PageRank iterations
                WHILE TRUE LIMIT maxIter DO
                    Start = SELECT s FROM Start:s
                    ACCUM s.@receivedScore = 0;
                    
                    Start = SELECT s FROM Start:s -(HAS_TRANSACTION:e)-> Transaction:t
                    ACCUM t.@receivedScore += s.@score / s.@outDegree;
                    
                    Start = SELECT s FROM Start:s
                    ACCUM s.@score = (1 - damping) + damping * s.@receivedScore,
                          @@maxScore += s.@score;
                END;
                
                # Normalize scores
                Start = SELECT s FROM Start:s
                ACCUM s.@score = s.@score / @@maxScore;
                
                PRINT Start[Start.id, Start.@score];
            }}
            """
            
            # Execute query
            result = self._execute_gsql(query)
            
            # Parse results
            page_rank_scores = {}
            if 'results' in result:
                for row in result['results']:
                    entity_id = row[0]
                    score = row[1]
                    page_rank_scores[entity_id] = score
            
            logger.info(f"PageRank completed. Calculated scores for {len(page_rank_scores)} entities")
            return page_rank_scores
            
        except Exception as e:
            logger.error(f"Error running PageRank: {e}")
            raise
    
    def run_connected_components(self) -> Dict[str, int]:
        """Run Connected Components algorithm"""
        try:
            logger.info("Running Connected Components algorithm")
            
            query = f"""
            CREATE QUERY connectedComponents() FOR GRAPH {self.config['tigergraph']['graph_name']} {{
                SumAccum<INT> @cc_id = 0;
                SumAccum<INT> @min_id = 0;
                SetAccum<VERTEX> @@cc_set;
                
                Start = {{Entity.*}};
                
                # Initialize
                Start = SELECT s FROM Start:s ACCUM s.@cc_id = getvid(s);
                
                # Connected components iterations
                WHILE TRUE DO
                    Start = SELECT s FROM Start:s -(SIMILAR_TO:e)-> Entity:t
                    ACCUM s.@min_id += t.@cc_id;
                    
                    Start = SELECT s FROM Start:s
                    ACCUM IF s.@min_id > 0 AND s.@min_id < s.@cc_id THEN
                              s.@cc_id = s.@min_id
                          END,
                          s.@min_id = 0;
                END;
                
                PRINT Start[Start.id, Start.@cc_id];
            }}
            """
            
            result = self._execute_gsql(query)
            
            # Parse results
            component_mapping = {}
            if 'results' in result:
                for row in result['results']:
                    entity_id = row[0]
                    component_id = row[1]
                    component_mapping[entity_id] = component_id
            
            logger.info(f"Connected Components completed. Found {len(set(component_mapping.values()))} components")
            return component_mapping
            
        except Exception as e:
            logger.error(f"Error running Connected Components: {e}")
            raise
    
    def get_entity_neighbors(self, entity_id: str, max_depth: int = 2) -> Dict:
        """Get entity neighbors up to specified depth"""
        try:
            query = f"""
            CREATE QUERY getNeighbors(STRING entityId, INT maxDepth = {max_depth}) FOR GRAPH {self.config['tigergraph']['graph_name']} {{
                SetAccum<VERTEX> @@visited;
                SetAccum<EDGE> @@edges;
                
                Start = {{Entity.*}};
                Start = SELECT s FROM Start:s WHERE s.id == entityId;
                
                WHILE Start.size() > 0 LIMIT maxDepth DO
                    Start = SELECT t FROM Start:s -(HAS_TRANSACTION:e)-> Transaction:t
                    ACCUM @@edges += e;
                    
                    Start = SELECT t FROM Start:s -(SIMILAR_TO:e)-> Entity:t
                    WHERE t NOT IN @@visited
                    ACCUM @@visited += t,
                          @@edges += e;
                END;
                
                PRINT @@visited, @@edges;
            }}
            """
            
            result = self._execute_gsql(query)
            return result
            
        except Exception as e:
            logger.error(f"Error getting neighbors: {e}")
            raise
    
    def get_graph_statistics(self) -> Dict:
        """Get graph statistics"""
        try:
            query = f"""
            CREATE QUERY getGraphStats() FOR GRAPH {self.config['tigergraph']['graph_name']} {{
                SumAccum<INT> @@vertexCount;
                SumAccum<INT> @@edgeCount;
                
                Start = {{Entity.*}};
                Start = SELECT s FROM Start:s ACCUM @@vertexCount += 1;
                
                Edges = {{Transaction.*}};
                Edges = SELECT t FROM Edges:t ACCUM @@edgeCount += 1;
                
                PRINT @@vertexCount, @@edgeCount;
            }}
            """
            
            result = self._execute_gsql(query)
            
            if 'results' in result and result['results']:
                stats = result['results'][0]
                return {
                    'vertex_count': stats[0],
                    'edge_count': stats[1]
                }
            
            return {'vertex_count': 0, 'edge_count': 0}
            
        except Exception as e:
            logger.error(f"Error getting graph statistics: {e}")
            raise 