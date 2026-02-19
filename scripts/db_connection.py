# scripts/db_connection.py
"""
Docstring for scripts.db_connection

Module de connexion à la base de données de staging.

Best practice:
- Connection pooling pour performances
- Gestion des erreurs
- Logging des requêtes
- Fermeture automatique des connexions
"""

import sqlalchemy as sa
from sqlalchemy import create_engine, text
from contextlib import contextmanager
import pandas as pd
import logging
from typing import Optional, Dict, Any
import sys
from pathlib import Path

# Ajouter le dossier parent au path pour importer confid
sys.path.append(str(Path(__file__).parent.parent))
from config import DATABASE_URI, LOG_LEVEL, LOG_FORMAT

# Configuration du logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class StagingDatabase:
    """
    Gestionnaire de connexion à la base de données de staging.
    
    Simule l'environnement de production avec :
    - Connection pooling
    - Gestion des transactions
    - Logging des requêtes
    - Métriques de performance
    """
    
    def __init__(self, database_uri: str = DATABASE_URI):
        """
        Initialise la connexion à la base.
        
        Args:
            database_uri: URI de connexion (SQLite ou PostgreSQL)
        """
        self.database_uri = database_uri
        self.engine = None
        self._connect()
    
    def _connect(self):
        """Établit la connexion avec pool de connexions."""
        try:
            # En production, on ajouterait :
            # pool_size=10, max_overflow=20, pool_timeout=30
            self.engine = create_engine(
                self.database_uri,
                echo=False,  # Mettre True pour debug SQL
                future=True
            )
            logger.info(f"✅ Connexion établie à {self.database_uri}")
        except Exception as e:
            logger.error(f"❌ Erreur de connexion : {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """
        Context manager pour connexion sécurisée.
        
        Usage:
            with db.get_connection() as conn:
                result = conn.execute(query)
        """
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Exécute une requête SQL et retourne un DataFrame.
        
        Args:
            query: Requête SQL (peut contenir des :param)
            params: Dictionnaire de paramètres (SQL injection safe)
        
        Returns:
            DataFrame avec les résultats
        """
        try:
            logger.info(f"🔍 Exécution requête : {query[:100]}...")
            
            with self.get_connection() as conn:
                if params:
                    result = pd.read_sql(text(query), conn, params=params)
                else:
                    result = pd.read_sql(query, conn)
            
            logger.info(f"✅ {len(result)} lignes récupérées")
            return result
            
        except Exception as e:
            logger.error(f"❌ Erreur SQL : {e}")
            logger.error(f"   Requête : {query}")
            raise