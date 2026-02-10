# test_simulator.py
# Script de test rapide du simulateur

import pandas as pd
from bank_data_simulator_advanced import AdvancedBankDataSimulator

print("🧪 TEST RAPIDE DU SIMULATEUR\n")

# Configuration pour test rapide (dataset réduit)
simulator = AdvancedBankDataSimulator(
    n_customers=5000,      # 5k clients au lieu de 50k
    n_merchants=500,       # 500 commerçants
    n_transactions=25000,  # 25k transactions
    fraud_rate=0.003,      # 0.3% (un peu plus pour voir les patterns)
    simulation_days=90     # 3 mois
)

# Génération
data = simulator.generate_all_tables(
    save_to_csv=True,
    output_dir='data_test'
)

# Validation rapide
print("\n🔍 VALIDATION DES DONNÉES :\n")

# 1. Vérifier les liens entre tables
print("1. Intégrité référentielle :")
txn = data['transactions']
print(f"   ✓ Tous les customer_id existent : {txn['customer_id'].isin(data['customers']['customer_id']).all()}")
print(f"   ✓ Tous les merchant_id existent : {txn['merchant_id'].isin(data['merchants']['merchant_id']).all()}")

# 2. Vérifier la distribution des fraudes
print(f"\n2. Distribution des fraudes :")
fraud_by_type = txn[txn['is_fraud'] == 1]['fraud_type'].value_counts()
for fraud_type, count in fraud_by_type.items():
    print(f"   • {fraud_type}: {count} ({count/len(txn)*100:.3f}%)")

# 3. Patterns de montants
print(f"\n3. Analyse des montants :")
print(f"   • Médiane transaction normale : {txn[txn['is_fraud']==0]['amount'].median():.2f}€")
print(f"   • Médiane transaction fraude : {txn[txn['is_fraud']==1]['amount'].median():.2f}€")
print(f"   • Moyenne transaction normale : {txn[txn['is_fraud']==0]['amount'].mean():.2f}€")
print(f"   • Moyenne transaction fraude : {txn[txn['is_fraud']==1]['amount'].mean():.2f}€")

# 4. Distribution temporelle
print(f"\n4. Distribution temporelle :")
txn['hour'] = pd.to_datetime(txn['transaction_timestamp']).dt.hour
print(f"   • Heure avec le plus de transactions : {txn['hour'].mode()[0]}h")
fraud_hour = txn[txn['is_fraud']==1]['hour'].mode()
if len(fraud_hour) > 0:
    print(f"   • Heure avec le plus de fraudes : {fraud_hour[0]}h")

# 5. Devices suspects
print(f"\n5. Devices suspects :")
devices = data['devices']
suspicious_devices = devices[devices['device_user_count'] > 3]
print(f"   • Devices partagés (>3 users) : {len(suspicious_devices['device_id'].unique())}")
print(f"   • Taux VPN (fraudes) : {devices[txn['is_fraud']==1]['is_vpn'].mean()*100:.1f}%")
print(f"   • Taux VPN (légitimes) : {devices[txn['is_fraud']==0]['is_vpn'].mean()*100:.1f}%")

# 6. Commerçants compromis
print(f"\n6. Commerçants compromis :")
merchants = data['merchants']
compromised = merchants[merchants['is_compromised'] == 1]
print(f"   • Nombre de terminaux compromis : {len(compromised)}")
txn_compromised = txn[txn['merchant_id'].isin(compromised['merchant_id'])]
print(f"   • Transactions sur terminaux compromis : {len(txn_compromised)}")
print(f"   • Taux de fraude sur ces terminaux : {txn_compromised['is_fraud'].mean()*100:.1f}%")

# 7. Performance du système d'alertes
print(f"\n7. Performance système d'alertes :")
alerts = data['alerts']
if len(alerts) > 0:
    # Taux de détection
    total_frauds = txn['is_fraud'].sum()
    detected_frauds = alerts['is_confirmed_fraud'].sum()
    detection_rate = (detected_frauds / total_frauds * 100) if total_frauds > 0 else 0
    
    print(f"   • Fraudes totales : {total_frauds}")
    print(f"   • Fraudes détectées : {detected_frauds}")
    print(f"   • Taux de détection : {detection_rate:.1f}%")
    print(f"   • Faux positifs : {(~alerts['is_confirmed_fraud']).sum()}")
    
    # Précision
    precision = alerts['is_confirmed_fraud'].mean() * 100
    print(f"   • Précision des alertes : {precision:.1f}%")

print("\n✅ TEST TERMINÉ - Fichiers dans /data_test/\n")
