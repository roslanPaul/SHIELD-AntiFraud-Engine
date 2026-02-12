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