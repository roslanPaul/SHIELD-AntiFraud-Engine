# config.py
# Configuration centralisée du projet SHIELD
# Best Practice : Un seul fichier de config pour toute l'équipe

from pathlib import Path

# Chemins du projet
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
DATABASE_DIR = PROJECT_ROOT / "database"
SQL_DIR = PROJECT_ROOT / "sql"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
FIGURES_DIR = OUTPUTS_DIR / "figures"
REPORTS_DIR = OUTPUTS_DIR / "reports"

# Créer les dossiers s'ils n'existent pas
for directory in [DATA_DIR, DATABASE_DIR, SQL_DIR, NOTEBOOKS_DIR, OUTPUTS_DIR, FIGURES_DIR, REPORTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Configuration Base de Données
DATABASE_PATH = DATABASE_DIR / "shield_staging.db"
DATABASE_URI = f"sqlite:///{DATABASE_PATH}"

# En production, ce serait :
# DATABASE_URI = "postgresql://user:password@staging-server.bank.internal:5432/shield_db"

# Tables de la base
TABLES = {
    'customer_profile': 'customer_profile.csv',
    'merchant_registry': 'merchant_registry.csv',
    'transactions': 'transactions.csv',
    'device_fingerprinting': 'device_fingerprinting.csv',
    'fraud_alerts_history': 'fraud_alerts_history.csv'
}

# Paramètres Métier (définis avec le Risk Manager)
BUSINESS_PARAMS = {
    'fraud_target_reduction': 0.20,  # Objectif : -20% de pertes
    'acceptable_false_positive_rate': 0.02,  # Max 2% de faux positifs
    'label_lag_threshold_days': 10,  # Seuil de confiance des labels
    'training_window_days': 90,  # Fenêtre d'entraînement
    'min_transactions_for_analysis': 100  # Seuil statistique
}

# Paramètres Graphiques (pour cohérence visuelle)
PLOT_STYLE = {
    'style': 'seaborn-v0_8-darkgrid',
    'palette': 'husl',
    'figure_size': (12, 6),
    'dpi': 100
}

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

print(f"✅ Configuration chargée - Projet SHIELD")
print(f"   📁 Racine : {PROJECT_ROOT}")
print(f"   🗄️  Base de données : {DATABASE_PATH}")
