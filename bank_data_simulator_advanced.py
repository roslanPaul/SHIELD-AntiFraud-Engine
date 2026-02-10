# bank_data_simulator_advanced.py
# Simulateur de données bancaires ultra-réaliste avec patterns comportementaux

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

# Configuration
fake = Faker('fr_FR')
np.random.seed(42)
random.seed(42)

class AdvancedBankDataSimulator:
    """
    Simule l'écosystème de données d'une néo-banque avec patterns comportementaux avancés.
    
    Améliorations vs version basique :
    1. Saisonnalité et cycles de vie (horaires, jours, périodes)
    2. Behavioral profiling (cohérence client-commerçant)
    3. Topologie réseau (clusters de fraude organisée)
    4. Latence de détection (feedback loop réaliste)
    5. Types de fraude spécifiques (card testing, account takeover, etc.)
    """
    
    def __init__(self, 
                 n_customers=50000,
                 n_merchants=5000,
                 n_transactions=500000,
                 fraud_rate=0.0018,
                 simulation_days=180):
        
        self.n_customers = n_customers
        self.n_merchants = n_merchants
        self.n_transactions = n_transactions
        self.fraud_rate = fraud_rate
        self.simulation_days = simulation_days
        
        # Dates de simulation
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=simulation_days)
        
        # Structures pour patterns avancés
        self.customer_profiles = {}  # Historique comportemental par client
        self.merchant_clusters = {}  # Clusters de fraude organisée
        self.compromised_devices = set()  # Devices compromis
        
        print("🏦 Initialisation du simulateur AVANCÉ NeoBank France...")
        print(f"   📊 {n_customers:,} clients | {n_merchants:,} commerçants | {n_transactions:,} transactions")
        print(f"   📅 Période : {simulation_days} jours ({self.start_date.date()} → {self.end_date.date()})")
    
    
    # ========================================
    # HELPER : Facteurs de saisonnalité
    # ========================================
    
    def _get_seasonal_factor(self, dt: datetime) -> float:
        """
        Calcule un coefficient de probabilité de transaction basé sur :
        - Jour de la semaine (plus d'achats le samedi)
        - Heure (pic midi et soir, creux la nuit)
        - Périodes spéciales (Noël, soldes)
        
        Returns: float entre 0.1 et 2.0
        """
        # 1. Facteur jour de semaine (lundi=0, dimanche=6)
        day_weights = {
            0: 0.9,   # Lundi
            1: 0.95,  # Mardi
            2: 1.0,   # Mercredi
            3: 1.05,  # Jeudi
            4: 1.3,   # Vendredi (sorties)
            5: 1.6,   # Samedi (shopping)
            6: 0.7    # Dimanche (commerces fermés)
        }
        day_factor = day_weights[dt.weekday()]
        
        # 2. Facteur horaire
        hour = dt.hour
        if 0 <= hour <= 5:
            hour_factor = 0.15  # Très peu de transactions légitimes la nuit
        elif 6 <= hour <= 8:
            hour_factor = 0.6   # Petit-déjeuner
        elif 9 <= hour <= 11:
            hour_factor = 1.0   # Matin
        elif 12 <= hour <= 14:
            hour_factor = 1.4   # Déjeuner (pic)
        elif 15 <= hour <= 17:
            hour_factor = 1.1   # Après-midi
        elif 18 <= hour <= 21:
            hour_factor = 1.5   # Dîner/soirée (pic)
        else:
            hour_factor = 0.8   # Fin de soirée
        
        # 3. Facteur saisonnier (mois)
        month = dt.month
        if month == 12:
            month_factor = 1.8  # Noël
        elif month in [1, 7]:
            month_factor = 1.5  # Soldes
        elif month in [6, 7, 8]:
            month_factor = 1.3  # Vacances d'été
        else:
            month_factor = 1.0
        
        return day_factor * hour_factor * month_factor
    
    
    def _is_customer_merchant_compatible(self, customer: pd.Series, merchant: pd.Series) -> float:
        """
        Calcule la probabilité qu'un client utilise ce commerçant (cohérence comportementale).
        
        Règles métier :
        - Client Premium → commerçants low/medium risk (80%)
        - Client Basic → évite commerçants Premium (électronique, voyages)
        - PEP → évite casinos, crypto
        
        Returns: probabilité entre 0.0 et 1.0
        """
        segment = customer['customer_segment']
        risk = merchant['merchant_risk_category']
        mcc = merchant['mcc_code']
        
        # Matrice de compatibilité segment-risque
        compatibility_matrix = {
            ('Basic', 'low'): 0.9,
            ('Basic', 'medium'): 0.6,
            ('Basic', 'high'): 0.2,
            ('Standard', 'low'): 0.8,
            ('Standard', 'medium'): 0.9,
            ('Standard', 'high'): 0.5,
            ('Premium', 'low'): 0.7,
            ('Premium', 'medium'): 0.9,
            ('Premium', 'high'): 0.8,
            ('Private', 'low'): 0.6,
            ('Private', 'medium'): 0.8,
            ('Private', 'high'): 0.9,
        }
        
        base_prob = compatibility_matrix.get((segment, risk), 0.5)
        
        # Ajustements spécifiques
        if customer['is_pep'] == 1 and mcc in ['7995', '5999']:  # Casino, crypto
            base_prob *= 0.1  # PEP évitent les secteurs sensibles
        
        if segment == 'Basic' and mcc in ['5735', '4121']:  # Électronique, taxis
            base_prob *= 0.3  # Pas les moyens
        
        return base_prob
    
    
    # ========================================
    # TABLE 1 : CUSTOMER_PROFILE (avec behavioral traits)
    # ========================================
    
    def generate_customer_profile(self) -> pd.DataFrame:
        """
        Génère profils clients avec traits comportementaux.
        
        Nouveautés :
        - spending_velocity : tendance à faire des achats rapprochés
        - risk_tolerance : acceptation des commerçants à risque
        - preferred_hours : plages horaires favorites
        """
        print("\n📋 Génération CUSTOMER_PROFILE (avec behavioral traits)...")
        
        segments = np.random.choice(
            ['Basic', 'Standard', 'Premium', 'Private'],
            size=self.n_customers,
            p=[0.45, 0.40, 0.12, 0.03]
        )
        
        account_ages = np.random.gamma(shape=2, scale=180, size=self.n_customers)
        account_ages = np.clip(account_ages, 30, 1825).astype(int)
        
        credit_scores = np.random.normal(loc=680, scale=80, size=self.n_customers)
        credit_scores = np.clip(credit_scores, 300, 850).astype(int)
        
        # Montant moyen selon segment
        segment_avg_amounts = {
            'Basic': (15, 25),
            'Standard': (35, 20),
            'Premium': (120, 60),
            'Private': (450, 200)
        }
        
        avg_amounts = [
            max(5, np.random.normal(segment_avg_amounts[seg][0], 
                                   segment_avg_amounts[seg][1]))
            for seg in segments
        ]
        
        # NOUVEAUTÉ : Traits comportementaux
        spending_velocities = np.random.choice(
            ['low', 'medium', 'high'],
            size=self.n_customers,
            p=[0.6, 0.3, 0.1]  # La plupart sont "low velocity"
        )
        
        risk_tolerances = [
            0.8 if seg in ['Premium', 'Private'] else 
            0.5 if seg == 'Standard' else 0.2
            for seg in segments
        ]
        
        # Plages horaires favorites (8-10, 12-14, 18-21)
        preferred_hours = [
            np.random.choice(['morning', 'lunch', 'evening', 'night'], 
                           p=[0.3, 0.4, 0.25, 0.05])
            for _ in range(self.n_customers)
        ]
        
        customers = pd.DataFrame({
            'customer_id': [f'CUST_{i:08d}' for i in range(1, self.n_customers + 1)],
            'customer_name': [fake.name() for _ in range(self.n_customers)],
            'email': [fake.email() for _ in range(self.n_customers)],
            'customer_segment': segments,
            'account_age_days': account_ages,
            'credit_score': credit_scores,
            'avg_transaction_amount': np.round(avg_amounts, 2),
            'is_pep': [
                1 if seg in ['Premium', 'Private'] and random.random() < 0.02 else 0
                for seg in segments
            ],
            'active_cards': np.random.choice([1, 2, 3], size=self.n_customers, p=[0.7, 0.25, 0.05]),
            'annual_income': [
                int(np.random.normal(
                    {'Basic': 25000, 'Standard': 45000, 'Premium': 85000, 'Private': 250000}[seg],
                    {'Basic': 8000, 'Standard': 15000, 'Premium': 30000, 'Private': 100000}[seg]
                ))
                for seg in segments
            ],
            'account_opening_date': [
                self.end_date - timedelta(days=int(age))
                for age in account_ages
            ],
            
            # Nouveaux champs comportementaux
            'spending_velocity': spending_velocities,
            'risk_tolerance': risk_tolerances,
            'preferred_hours': preferred_hours,
            'avg_transactions_per_week': np.random.poisson(lam=5, size=self.n_customers)
        })
        
        # Sauvegarder les profils pour référence future
        self.customer_profiles = customers.set_index('customer_id').to_dict('index')
        
        print(f"   ✅ {len(customers):,} profils clients avec behavioral traits")
        return customers
    
    
    # ========================================
    # TABLE 2 : MERCHANT_REGISTRY (avec clusters)
    # ========================================
    
    def generate_merchant_registry(self) -> pd.DataFrame:
        """
        Génère commerçants avec identification de clusters à risque.
        """
        print("\n🏪 Génération MERCHANT_REGISTRY (avec clusters de fraude)...")
        
        mcc_categories = {
            '5411': ('Supermarché', 'low'),
            '5812': ('Restaurant', 'medium'),
            '5999': ('E-commerce Divers', 'high'),
            '4121': ('Transport Taxi', 'medium'),
            '7995': ('Casino/Jeux', 'high'),
            '5735': ('Électronique', 'high'),
            '5912': ('Pharmacie', 'low'),
            '5941': ('Articles Sport', 'medium'),
            '5661': ('Chaussures', 'low'),
            '5542': ('Station Service', 'low'),
            '4814': ('Télécom', 'medium'),
            '7399': ('Services Business', 'medium'),
            '5311': ('Grand Magasin', 'low'),
            '5722': ('Électroménager', 'medium'),
            '5815': ('Streaming/Digital', 'high')
        }
        
        mcc_distribution = list(mcc_categories.keys())
        mcc_weights = [0.15, 0.12, 0.18, 0.05, 0.02, 0.08, 0.06, 0.04, 0.03, 0.10, 0.05, 0.04, 0.03, 0.03, 0.02]
        
        merchant_mccs = np.random.choice(mcc_distribution, size=self.n_merchants, p=mcc_weights)
        
        def get_chargeback_rate(risk_level):
            if risk_level == 'low':
                return max(0, np.random.normal(0.3, 0.15))
            elif risk_level == 'medium':
                return max(0, np.random.normal(0.8, 0.3))
            else:
                return max(0, np.random.normal(2.1, 0.8))
        
        merchants = pd.DataFrame({
            'merchant_id': [f'MERCH_{i:07d}' for i in range(1, self.n_merchants + 1)],
            'merchant_name': [fake.company() for _ in range(self.n_merchants)],
            'mcc_code': merchant_mccs,
            'merchant_category': [mcc_categories[mcc][0] for mcc in merchant_mccs],
            'merchant_risk_category': [mcc_categories[mcc][1] for mcc in merchant_mccs],
            'chargeback_rate_30d': [
                round(get_chargeback_rate(mcc_categories[mcc][1]), 2)
                for mcc in merchant_mccs
            ],
            'merchant_city': [fake.city() for _ in range(self.n_merchants)],
            'merchant_country': np.random.choice(
                ['FR', 'BE', 'ES', 'IT', 'GB', 'US', 'CN'],
                size=self.n_merchants,
                p=[0.85, 0.05, 0.03, 0.02, 0.02, 0.02, 0.01]
            ),
            'avg_monthly_volume': np.random.lognormal(mean=9, sigma=1.5, size=self.n_merchants).astype(int),
            'registration_date': [
                fake.date_between(start_date='-5y', end_date='today')
                for _ in range(self.n_merchants)
            ]
        })
        
        # NOUVEAUTÉ : Créer des clusters de fraude (terminaux compromis)
        # 0.5% des commerçants sont compromis
        n_compromised = int(self.n_merchants * 0.005)
        compromised_idx = np.random.choice(merchants.index, size=n_compromised, replace=False)
        merchants['is_compromised'] = 0
        merchants.loc[compromised_idx, 'is_compromised'] = 1
        
        # Sauvegarder les clusters
        self.merchant_clusters['compromised'] = merchants[merchants['is_compromised'] == 1]['merchant_id'].tolist()
        
        print(f"   ✅ {len(merchants):,} commerçants générés")
        print(f"   🔴 {n_compromised} terminaux compromis identifiés (fraude organisée)")
        
        return merchants
    
    
    # ========================================
    # TABLE 3 : TRANSACTIONS (AVANCÉE - avec patterns)
    # ========================================
    
    def generate_transactions_advanced(self, customers: pd.DataFrame, merchants: pd.DataFrame) -> pd.DataFrame:
        """
        Génère transactions avec patterns comportementaux avancés.
        
        Patterns de fraude simulés :
        1. Card Testing : petits montants (<5€) sur marchands étrangers
        2. Account Takeover : gros montants sur commerçants high-risk
        3. Compromised Terminal : fraude via terminaux compromis
        4. Velocity Fraud : achats multiples rapides
        5. Geographic Anomaly : changement de pays soudain
        """
        print("\n💳 Génération TRANSACTIONS AVANCÉE (patterns comportementaux)...")
        
        transactions = []
        customer_last_txn = {}  # Tracking de la dernière transaction par client
        customer_countries = {}  # Pays habituels par client
        
        # Identification des fraudeurs (1% comptes compromis)
        n_fraudsters = int(self.n_customers * 0.01)
        fraudster_ids = np.random.choice(customers['customer_id'].values, size=n_fraudsters, replace=False)
        fraudster_set = set(fraudster_ids)
        
        for i in range(self.n_transactions):
            # 1. Génération timestamp avec saisonnalité
            days_ago = np.random.exponential(scale=30)
            days_ago = min(days_ago, self.simulation_days)
            
            # Heure pondérée par saisonnalité
            hour_probs = [0.01, 0.01, 0.01, 0.01, 0.01, 0.02,
                         0.03, 0.05, 0.07, 0.08, 0.09, 0.10,
                         0.09, 0.08, 0.07, 0.06, 0.07, 0.08,
                         0.06, 0.04, 0.03, 0.02, 0.01, 0.01]
            
            # 2. Normalise pour forcer la somme à 1.0
            hour_probs = np.array(hour_probs)
            hour_probs /= hour_probs.sum()
            
            # Force la conversion en int() pour éviter le conflit avec timedelta
            hour = int(np.random.choice(range(24), p=hour_probs))
            
            txn_timestamp = self.end_date - timedelta(
                days=float(days_ago), # Par sécurité, timedelta gère bien les floats pour days
                hours=hour,
                minutes=int(np.random.randint(0, 60)),
                seconds=int(np.random.randint(0, 60))
            )
            
            # Appliquer facteur saisonnier (skip transaction si hors période)
            seasonal_factor = self._get_seasonal_factor(txn_timestamp)
            if random.random() > seasonal_factor / 2.0:  # Normalisation
                continue
            
            # 2. Sélection client
            customer = customers.sample(1, weights=customers['avg_transaction_amount']).iloc[0]
            customer_id = customer['customer_id']
            
            # 3. Sélection commerçant avec compatibilité
            attempts = 0
            while attempts < 5:
                merchant = merchants.sample(1).iloc[0]
                compatibility = self._is_customer_merchant_compatible(customer, merchant)
                if random.random() < compatibility:
                    break
                attempts += 1
            
            # 4. Détection des patterns de fraude
            is_fraud = 0
            fraud_type = 'legit'
            detection_delay = None
            
            # PATTERN A : Card Testing (0.05% des transactions)
            if random.random() < 0.0005:
                is_fraud = 1
                fraud_type = 'card_testing'
                amount = round(random.uniform(0.5, 4.99), 2)
                # Forcer commerçant étranger
                foreign_merchants = merchants[merchants['merchant_country'] != 'FR']
                if len(foreign_merchants) > 0:
                    merchant = foreign_merchants.sample(1).iloc[0]
                detection_delay = random.randint(1, 3)  # Détecté rapidement
            
            # PATTERN B : Account Takeover (0.12% - clients Premium/Private)
            elif (customer_id in fraudster_set and 
                  customer['customer_segment'] in ['Premium', 'Private'] and 
                  random.random() < 0.15):
                is_fraud = 1
                fraud_type = 'account_takeover'
                amount = round(random.uniform(1000, 5000), 2)
                # Montants souvent ronds
                if random.random() < 0.4:
                    amount = round(amount / 100) * 100
                # Forcer commerçant high-risk
                high_risk = merchants[merchants['merchant_risk_category'] == 'high']
                if len(high_risk) > 0:
                    merchant = high_risk.sample(1).iloc[0]
                detection_delay = random.randint(7, 45)  # Plus long à détecter
            
            # PATTERN C : Compromised Terminal (70% de fraude sur ces terminaux)
            elif merchant['merchant_id'] in self.merchant_clusters['compromised']:
                if random.random() < 0.7:
                    is_fraud = 1
                    fraud_type = 'compromised_terminal'
                    amount = round(max(10, np.random.normal(customer['avg_transaction_amount'], 30)), 2)
                    detection_delay = random.randint(14, 60)
                else:
                    amount = round(max(1, np.random.normal(customer['avg_transaction_amount'], 15)), 2)
            
            # PATTERN D : Velocity Fraud (achats très rapprochés)
            elif customer_id in customer_last_txn:
                last_txn_time = customer_last_txn[customer_id]
                time_diff = (txn_timestamp - last_txn_time).total_seconds() / 60  # minutes
                
                if time_diff < 5 and random.random() < 0.3:  # 2 achats en 5min = suspect
                    is_fraud = 1
                    fraud_type = 'velocity_fraud'
                    amount = round(random.uniform(50, 300), 2)
                    detection_delay = random.randint(1, 7)
                else:
                    amount = round(max(1, np.random.normal(customer['avg_transaction_amount'], 15)), 2)
            
            # PATTERN E : Geographic Anomaly
            elif customer_id in customer_countries:
                usual_country = customer_countries[customer_id]
                if merchant['merchant_country'] != usual_country and merchant['merchant_country'] != 'FR':
                    if random.random() < 0.15:  # 15% de ces changements sont frauduleux
                        is_fraud = 1
                        fraud_type = 'geographic_anomaly'
                        amount = round(random.uniform(100, 800), 2)
                        detection_delay = random.randint(3, 21)
                    else:
                        amount = round(max(1, np.random.normal(customer['avg_transaction_amount'], 15)), 2)
                else:
                    amount = round(max(1, np.random.normal(customer['avg_transaction_amount'], 15)), 2)
            
            # Transaction normale
            else:
                amount = round(max(1, np.random.normal(customer['avg_transaction_amount'], 15)), 2)
            
            # Mise à jour tracking client
            customer_last_txn[customer_id] = txn_timestamp
            if customer_id not in customer_countries:
                customer_countries[customer_id] = merchant['merchant_country']
            
            # Statut transaction
            if is_fraud and random.random() < 0.15:  # 15% des fraudes sont bloquées immédiatement
                status = 'declined'
            elif not is_fraud and random.random() < 0.02:  # 2% faux positifs
                status = 'declined'
            else:
                status = 'approved'
            
            transactions.append({
                'transaction_id': f'TXN_{i+1:010d}',
                'customer_id': customer_id,
                'merchant_id': merchant['merchant_id'],
                'transaction_timestamp': txn_timestamp,
                'amount': amount,
                'currency': 'EUR',
                'mcc_code': merchant['mcc_code'],
                'merchant_country': merchant['merchant_country'],
                'merchant_city': merchant['merchant_city'],
                'transaction_type': np.random.choice(
                    ['card_present', 'card_not_present', 'contactless', 'online'],
                    p=[0.35, 0.25, 0.30, 0.10]
                ),
                'is_international': 1 if merchant['merchant_country'] != 'FR' else 0,
                'is_fraud': is_fraud,
                'fraud_type': fraud_type,
                'detection_delay_days': detection_delay,
                'transaction_status': status,
                'merchant_risk_category': merchant['merchant_risk_category']
            })
            
            # Progression
            if (len(transactions) % 50000 == 0 and len(transactions) > 0):
                fraud_count = sum(t['is_fraud'] for t in transactions)
                print(f"   ⏳ {len(transactions):,} transactions | {fraud_count} fraudes")
        
        df_transactions = pd.DataFrame(transactions)
        
        # Statistiques finales
        print(f"\n   ✅ {len(df_transactions):,} transactions générées")
        print(f"   🚨 Fraudes par type :")
        for fraud_type in df_transactions[df_transactions['is_fraud'] == 1]['fraud_type'].value_counts().items():
            print(f"      • {fraud_type[0]}: {fraud_type[1]}")
        print(f"   💰 Montant total fraudé : {df_transactions[df_transactions['is_fraud']==1]['amount'].sum():,.2f}€")
        print(f"   ⏱️  Délai moyen de détection : {df_transactions[df_transactions['is_fraud']==1]['detection_delay_days'].mean():.1f} jours")
        
        return df_transactions
    
    
    # ========================================
    # TABLE 4 : DEVICE_FINGERPRINTING (avec réseau)
    # ========================================
    
    def generate_device_fingerprinting(self, transactions: pd.DataFrame) -> pd.DataFrame:
        """
        Génère empreintes devices avec détection de réseaux frauduleux.
        """
        print("\n📱 Génération DEVICE_FINGERPRINTING (réseau de fraude)...")
        
        devices = []
        customer_devices = {}
        device_usage_count = {}  # Tracking du nombre d'utilisateurs par device
        
        for _, txn in transactions.iterrows():
            customer_id = txn['customer_id']
            
            # Initialisation device pour nouveau client
            if customer_id not in customer_devices:
                customer_devices[customer_id] = {
                    'device_id': f'DEV_{len(customer_devices):08d}',
                    'os': np.random.choice(['iOS', 'Android', 'Windows', 'MacOS'], p=[0.35, 0.40, 0.15, 0.10]),
                    'browser': np.random.choice(['Safari', 'Chrome', 'Firefox', 'Edge'], p=[0.30, 0.50, 0.10, 0.10])
                }
            
            # Pattern : Fraude organisée = même device pour plusieurs clients
            if txn['fraud_type'] == 'account_takeover' and random.random() < 0.3:
                # Réutiliser un device compromis existant
                if len(self.compromised_devices) > 0 and random.random() < 0.6:
                    device_id = random.choice(list(self.compromised_devices))
                else:
                    device_id = f'DEV_FRAUD_{len(self.compromised_devices):05d}'
                    self.compromised_devices.add(device_id)
                
                customer_devices[customer_id]['device_id'] = device_id
            
            current_device = customer_devices[customer_id]
            device_id = current_device['device_id']
            
            # Tracking usage
            if device_id not in device_usage_count:
                device_usage_count[device_id] = set()
            device_usage_count[device_id].add(customer_id)
            
            # Changement de device (suspect si fréquent)
            device_changed = 0
            if random.random() < 0.05:
                device_changed = 1
                customer_devices[customer_id] = {
                    'device_id': f'DEV_{random.randint(100000, 999999):08d}',
                    'os': np.random.choice(['iOS', 'Android', 'Windows', 'MacOS']),
                    'browser': np.random.choice(['Safari', 'Chrome', 'Firefox', 'Edge'])
                }
            
            # Fraude = plus de VPN, émulateurs
            if txn['is_fraud']:
                is_vpn = 1 if random.random() < 0.65 else 0
                is_emulator = 1 if random.random() < 0.35 else 0
            else:
                is_vpn = 1 if random.random() < 0.08 else 0
                is_emulator = 0
            
            devices.append({
                'transaction_id': txn['transaction_id'],
                'device_id': device_id,
                'device_type': np.random.choice(['mobile', 'tablet', 'desktop'], p=[0.65, 0.10, 0.25]),
                'os': current_device['os'],
                'browser': current_device['browser'],
                'ip_address': fake.ipv4(),
                'is_vpn': is_vpn,
                'is_emulator': is_emulator,
                'device_change_24h': device_changed,
                'screen_resolution': np.random.choice(['1920x1080', '1366x768', '375x667', '414x896']),
                'language': 'fr-FR',
                'timezone': 'Europe/Paris',
                'user_agent': f"Mozilla/5.0 ({current_device['os']}) {current_device['browser']}"
            })
        
        df_devices = pd.DataFrame(devices)
        
        # Ajouter métrique de "device sharing" (réseau de fraude)
        df_devices['device_user_count'] = df_devices['device_id'].map(
            lambda x: len(device_usage_count.get(x, set()))
        )
        
        print(f"   ✅ {len(df_devices):,} empreintes générées")
        print(f"   🔒 VPN : {df_devices['is_vpn'].sum():,} ({df_devices['is_vpn'].mean()*100:.1f}%)")
        print(f"   🤖 Émulateurs : {df_devices['is_emulator'].sum():,}")
        print(f"   🌐 Devices partagés (>5 users) : {(df_devices['device_user_count'] > 5).sum()}")
        
        return df_devices
    
    
    # ========================================
    # TABLE 5 : FRAUD_ALERTS_HISTORY
    # ========================================
    
    def generate_fraud_alerts_history(self, transactions: pd.DataFrame) -> pd.DataFrame:
        """
        Génère historique alertes avec latence réaliste.
        """
        print("\n🚨 Génération FRAUD_ALERTS_HISTORY...")
        
        # Ancien système détecte 65% des vraies fraudes
        fraud_txns = transactions[transactions['is_fraud'] == 1].sample(frac=0.65)
        
        # Faux positifs : 2.5% des transactions légitimes
        legit_txns = transactions[transactions['is_fraud'] == 0].sample(frac=0.025)
        
        alerted_txns = pd.concat([fraud_txns, legit_txns])
        
        alerts = []
        
        for _, txn in alerted_txns.iterrows():
            # Temps de traitement
            if txn['is_fraud']:
                response_time = int(np.random.exponential(scale=45))
            else:
                response_time = int(np.random.exponential(scale=12))
            
            # Score alerte (plus élevé si vraie fraude)
            if txn['is_fraud']:
                alert_score = round(random.uniform(70, 98), 1)
            else:
                alert_score = round(random.uniform(35, 75), 1)
            
            alerts.append({
                'alert_id': f'ALERT_{len(alerts)+1:08d}',
                'transaction_id': txn['transaction_id'],
                'customer_id': txn['customer_id'],
                'alert_date': txn['transaction_timestamp'],
                'alert_type': np.random.choice([
                    'velocity', 'amount_threshold', 'geo_mismatch', 
                    'new_merchant', 'time_anomaly', 'device_fingerprint'
                ]),
                'alert_score': alert_score,
                'is_confirmed_fraud': txn['is_fraud'],
                'fraud_type': txn['fraud_type'] if txn['is_fraud'] else None,
                'response_time_minutes': response_time,
                'reviewed_by': f'ANALYST_{random.randint(1, 25):02d}',
                'resolution': 'fraud_confirmed' if txn['is_fraud'] else 'false_positive',
                'confirmation_date': txn['transaction_timestamp'] + timedelta(
                    days=txn['detection_delay_days'] if txn['is_fraud'] else 0
                )
            })
        
        df_alerts = pd.DataFrame(alerts)
        
        print(f"   ✅ {len(df_alerts):,} alertes générées")
        print(f"   ✔️  Vrais positifs : {df_alerts['is_confirmed_fraud'].sum():,}")
        print(f"   ✖️  Faux positifs : {(~df_alerts['is_confirmed_fraud']).sum():,}")
        print(f"   ⏱️  Temps moyen : {df_alerts['response_time_minutes'].mean():.1f} min")
        
        return df_alerts
    
    
    # ========================================
    # ORCHESTRATION COMPLÈTE
    # ========================================
    
    def generate_all_tables(self, save_to_csv=True, output_dir='data'):
        """
        Génère l'écosystème complet de données.
        """
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        print("\n" + "="*70)
        print("🚀 GÉNÉRATION ÉCOSYSTÈME BANCAIRE AVANCÉ")
        print("="*70)
        
        # Créer dossier output
        os.makedirs(output_dir, exist_ok=True)
        
        # 1. Profils clients
        customers = self.generate_customer_profile()
        
        # 2. Commerçants
        merchants = self.generate_merchant_registry()
        
        # 3. Transactions (cœur du système)
        transactions = self.generate_transactions_advanced(customers, merchants)
        
        # 4. Device fingerprinting
        devices = self.generate_device_fingerprinting(transactions)
        
        # 5. Historique alertes
        alerts = self.generate_fraud_alerts_history(transactions)
        
        # Sauvegarde
        if save_to_csv:
            print(f"\n💾 Sauvegarde en CSV dans /{output_dir}/...")
            customers.to_csv(f'{output_dir}/customer_profile.csv', index=False)
            merchants.to_csv(f'{output_dir}/merchant_registry.csv', index=False)
            transactions.to_csv(f'{output_dir}/transactions.csv', index=False)
            devices.to_csv(f'{output_dir}/device_fingerprinting.csv', index=False)
            alerts.to_csv(f'{output_dir}/fraud_alerts_history.csv', index=False)
            print("   ✅ Tous les fichiers sauvegardés")
        
        # Statistiques finales
        self._print_final_stats(customers, merchants, transactions, devices, alerts)
        
        return {
            'customers': customers,
            'merchants': merchants,
            'transactions': transactions,
            'devices': devices,
            'alerts': alerts
        }
    
    
    def _print_final_stats(self, customers, merchants, transactions, devices, alerts):
        """Affiche statistiques complètes."""
        print("\n" + "="*70)
        print("📊 STATISTIQUES FINALES")
        print("="*70)
        
        print(f"\n📋 CLIENTS :")
        print(f"   Total : {len(customers):,}")
        print(f"   Par segment : {customers['customer_segment'].value_counts().to_dict()}")
        
        print(f"\n🏪 COMMERÇANTS :")
        print(f"   Total : {len(merchants):,}")
        print(f"   Compromis : {merchants['is_compromised'].sum()}")
        
        print(f"\n💳 TRANSACTIONS :")
        print(f"   Total : {len(transactions):,}")
        print(f"   Fraudes : {transactions['is_fraud'].sum():,} ({transactions['is_fraud'].mean()*100:.3f}%)")
        print(f"   Montant total : {transactions['amount'].sum():,.2f}€")
        print(f"   Montant fraudé : {transactions[transactions['is_fraud']==1]['amount'].sum():,.2f}€")
        
        print(f"\n📱 DEVICES :")
        print(f"   Empreintes uniques : {devices['device_id'].nunique():,}")
        print(f"   Devices partagés (>3 users) : {(devices['device_user_count'] > 3).sum():,}")
        
        print(f"\n🚨 ALERTES :")
        print(f"   Total : {len(alerts):,}")
        print(f"   Précision : {alerts['is_confirmed_fraud'].mean()*100:.1f}%")
        
        print("\n" + "="*70)
        print("✅ GÉNÉRATION TERMINÉE")
        print("="*70 + "\n")


# ========================================
# SCRIPT D'EXÉCUTION
# ========================================

if __name__ == "__main__":
    
    print("""
╔══════════════════════════════════════════════════════════════════╗
║  🏦 SIMULATEUR DE DONNÉES BANCAIRES AVANCÉ                       ║
║  Version 2.0 - Production Ready                                  ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    # Configuration
    simulator = AdvancedBankDataSimulator(
        n_customers=50000,
        n_merchants=5000,
        n_transactions=500000,
        fraud_rate=0.0018,
        simulation_days=180
    )
    
    # Génération complète
    data = simulator.generate_all_tables(
        save_to_csv=True,
        output_dir='data'
    )
    
    print("\n💡 Prochaines étapes suggérées :")
    print("   1. Vérifier les fichiers dans /data/")
    print("   2. Explorer avec pandas : df = pd.read_csv('data/transactions.csv')")
    print("   3. Créer des features avancées (pipeline feature engineering)")
    print("   4. Entraîner un modèle de ML (XGBoost, Random Forest)")
