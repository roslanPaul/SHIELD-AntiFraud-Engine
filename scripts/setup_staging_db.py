# scripts/setup_staging_db.py
"""
Script de création de l'environnement de staging SQL.

Transforme les fichiers CSV bruts en base de données relationnelle
avec index, contraintes et métadonnées.

Usage:
    python scripts/setup_staging_db.py
"""

import pandas as pd
import sqlite3
import logging
from pathlib import Path
import sys
from datetime import datetime

# Ajouter le dossier parent au path
sys.path.append(str(Path(__file__).parent.parent))
from config import DATA_DIR, DATABASE_PATH, TABLES, LOG_LEVEL, LOG_FORMAT

# Configuration logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


class StagingDatabaseSetup:
    """
    Gestionnaire de création de la base de données de staging.
    
    Fonctionnalités :
    - Import des CSV
    - Création des index
    - Validation des données
    - Génération de statistiques
    """
    
    def __init__(self, data_dir: Path = DATA_DIR, db_path: Path = DATABASE_PATH):
        self.data_dir = data_dir
        self.db_path = db_path
        self.conn = None
        self.stats = {}
    
    def connect(self):
        """Établit la connexion à la base."""
        logger.info(f"📂 Connexion à {self.db_path}")
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")  # Activer les contraintes FK
    
    def import_table(self, table_name: str, csv_filename: str) -> bool:
        """
        Import une table depuis un CSV.
        
        Args:
            table_name: Nom de la table SQL
            csv_filename: Nom du fichier CSV
        
        Returns:
            True si succès
        """
        csv_path = self.data_dir / csv_filename
        
        if not csv_path.exists():
            logger.error(f"❌ Fichier introuvable : {csv_path}")
            return False
        
        try:
            logger.info(f"📥 Import de {table_name}...")
            
            # Lecture CSV
            df = pd.read_csv(csv_path)
            
            # Import dans SQLite
            df.to_sql(table_name, self.conn, if_exists='replace', index=False)
            
            # Statistiques
            self.stats[table_name] = {
                'rows': len(df),
                'columns': len(df.columns),
                'size_mb': csv_path.stat().st_size / (1024 * 1024)
            }
            
            logger.info(f"   ✅ {len(df):,} lignes importées ({len(df.columns)} colonnes)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur import {table_name} : {e}")
            return False
    
    def create_indexes(self):
        """
        Crée les index pour optimiser les performances des requêtes.
        
        Best Practice : Index sur les clés étrangères et colonnes fréquemment filtrées
        """
        logger.info("🔧 Création des index...")
        
        indexes = [
            # Clés primaires
            "CREATE INDEX IF NOT EXISTS idx_customer_id ON customer_profile(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_merchant_id ON merchant_registry(merchant_id)",
            "CREATE INDEX IF NOT EXISTS idx_transaction_id ON transactions(transaction_id)",
            "CREATE INDEX IF NOT EXISTS idx_device_txn ON device_fingerprinting(transaction_id)",
            "CREATE INDEX IF NOT EXISTS idx_alert_txn ON fraud_alerts_history(transaction_id)",
            
            # Clés étrangères (pour JOINs)
            "CREATE INDEX IF NOT EXISTS idx_txn_customer ON transactions(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_txn_merchant ON transactions(merchant_id)",
            "CREATE INDEX IF NOT EXISTS idx_alert_customer ON fraud_alerts_history(customer_id)",
            
            # Colonnes de filtrage fréquent
            "CREATE INDEX IF NOT EXISTS idx_txn_timestamp ON transactions(transaction_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_txn_fraud ON transactions(is_fraud)",
            "CREATE INDEX IF NOT EXISTS idx_merchant_country ON merchant_registry(merchant_country)",
            "CREATE INDEX IF NOT EXISTS idx_alert_confirmed ON fraud_alerts_history(is_confirmed_fraud)",
            "CREATE INDEX IF NOT EXISTS idx_device_vpn ON device_fingerprinting(is_vpn)",
        ]
        
        for idx_query in indexes:
            try:
                self.conn.execute(idx_query)
                logger.info(f"   ✅ {idx_query.split('idx_')[1].split(' ')[0]}")
            except Exception as e:
                logger.warning(f"   ⚠️  Index déjà existant ou erreur : {e}")
        
        self.conn.commit()
        logger.info("✅ Index créés")
    
    def validate_data_quality(self):
        """
        Valide la qualité des données importées.
        
        Checks :
        - Pas de NULL dans les clés primaires
        - Cohérence des foreign keys
        - Distribution des labels
        """
        logger.info("🔍 Validation de la qualité des données...")
        
        # Check 1 : Clés primaires uniques
        checks = [
            ("Unicité customer_id", 
             "SELECT COUNT(*) - COUNT(DISTINCT customer_id) as dups FROM customer_profile"),
            
            ("Unicité merchant_id", 
             "SELECT COUNT(*) - COUNT(DISTINCT merchant_id) as dups FROM merchant_registry"),
            
            ("Unicité transaction_id", 
             "SELECT COUNT(*) - COUNT(DISTINCT transaction_id) as dups FROM transactions"),
            
            # Check 2 : Foreign keys
            ("Cohérence FK customer", 
             """SELECT COUNT(*) as orphans 
                FROM transactions t 
                LEFT JOIN customer_profile c ON t.customer_id = c.customer_id 
                WHERE c.customer_id IS NULL"""),
            
            ("Cohérence FK merchant", 
             """SELECT COUNT(*) as orphans 
                FROM transactions t 
                LEFT JOIN merchant_registry m ON t.merchant_id = m.merchant_id 
                WHERE m.merchant_id IS NULL"""),
            
            # Check 3 : Distribution labels
            ("Taux de fraude", 
             "SELECT AVG(is_fraud) * 100 as fraud_pct FROM transactions"),
        ]
        
        all_valid = True
        for check_name, query in checks:
            try:
                cursor = self.conn.execute(query)
                result = cursor.fetchone()[0]
                
                if 'dups' in query or 'orphans' in query:
                    if result == 0:
                        logger.info(f"   ✅ {check_name} : OK")
                    else:
                        logger.error(f"   ❌ {check_name} : {result} problèmes")
                        all_valid = False
                else:
                    logger.info(f"   ℹ️  {check_name} : {result:.3f}%")
            except Exception as e:
                logger.error(f"   ❌ Erreur check {check_name} : {e}")
                all_valid = False
        
        return all_valid
    
    def generate_metadata(self):
        """
        Génère une table de métadonnées pour documentation.
        
        Best Practice : Toujours documenter la provenance des données
        """
        logger.info("📝 Génération des métadonnées...")
        
        metadata = {
            'table_name': [],
            'row_count': [],
            'column_count': [],
            'size_mb': [],
            'created_at': []
        }
        
        for table_name, stats in self.stats.items():
            metadata['table_name'].append(table_name)
            metadata['row_count'].append(stats['rows'])
            metadata['column_count'].append(stats['columns'])
            metadata['size_mb'].append(round(stats['size_mb'], 2))
            metadata['created_at'].append(datetime.now().isoformat())
        
        df_metadata = pd.DataFrame(metadata)
        df_metadata.to_sql('_metadata', self.conn, if_exists='replace', index=False)
        
        logger.info("✅ Métadonnées sauvegardées dans table '_metadata'")
    
    def print_summary(self):
        """Affiche un résumé de l'environnement créé."""
        print("\n" + "="*70)
        print("📊 RÉSUMÉ DE L'ENVIRONNEMENT DE STAGING")
        print("="*70)
        print(f"\n📂 Base de données : {self.db_path}")
        print(f"📅 Date de création : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"\n📋 Tables importées :")
        
        total_rows = 0
        total_size = 0
        
        for table_name, stats in self.stats.items():
            print(f"   • {table_name:30s} : {stats['rows']:>10,} lignes  |  {stats['columns']:>2} colonnes  |  {stats['size_mb']:>6.2f} MB")
            total_rows += stats['rows']
            total_size += stats['size_mb']
        
        print(f"\n💾 Total : {total_rows:,} lignes | {total_size:.2f} MB")
        print("="*70 + "\n")
    
    def setup(self):
        """Pipeline complet de setup."""
        logger.info("🚀 Démarrage du setup de l'environnement de staging...")
        
        # 1. Connexion
        self.connect()
        
        # 2. Import des tables
        all_imported = True
        for table_name, csv_filename in TABLES.items():
            if not self.import_table(table_name, csv_filename):
                all_imported = False
        
        if not all_imported:
            logger.error("❌ Certaines tables n'ont pas pu être importées")
            return False
        
        # 3. Création des index
        self.create_indexes()
        
        # 4. Validation
        if not self.validate_data_quality():
            logger.warning("⚠️  Certains checks de qualité ont échoué")
        
        # 5. Métadonnées
        self.generate_metadata()
        
        # 6. Résumé
        self.print_summary()
        
        # 7. Fermeture
        self.conn.close()
        
        logger.info("✅ Setup terminé avec succès")
        return True


def main():
    """Point d'entrée principal."""
    print("\n" + "="*70)
    print("🏗️  SETUP ENVIRONNEMENT DE STAGING - PROJET SHIELD")
    print("="*70 + "\n")
    
    # Vérification que les CSV existent
    if not DATA_DIR.exists():
        logger.error(f"❌ Dossier {DATA_DIR} introuvable")
        logger.info("💡 Conseil : Exécuter d'abord le simulateur de données")
        return
    
    # Setup
    setup = StagingDatabaseSetup()
    success = setup.setup()
    
    if success:
        print("\n✅ Environnement prêt ! Tu peux maintenant :")
        print("   1. Lancer le notebook d'analyse : notebooks/01_EDA_Risk_Analysis.ipynb")
        print("   2. Tester la connexion : python scripts/db_connection.py")
        print("   3. Exécuter des requêtes SQL depuis : sql/")
    else:
        print("\n❌ Le setup a échoué - Vérifier les logs ci-dessus")


if __name__ == "__main__":
    main()
