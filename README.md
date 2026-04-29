# 🚀 TP3 : Optimisation d'un Pipeline de Données Distribué

Ce projet documente l'évolution technique et l'optimisation d'un service d'enrichissement de données haute performance. L'objectif était de transformer un traitement séquentiel lent en un système capable de gérer des flux massifs de données avec une latence minimale.

## 🏗️ Architecture du Système
Le flux de données suit le schéma suivant :
**Producteur Utilisateur** $\rightarrow$ **Kafka (Topic: users)** $\rightarrow$ **Service d'Enrichissement (Go)** $\rightarrow$ **Kafka (Topic: notification)** $\rightarrow$ **Consommateur**.

Le cœur du projet réside dans l'optimisation du service Go, qui doit enrichir chaque utilisateur avec son email depuis une base de données MySQL avant de renvoyer le résultat.

---

## 📈 Évolution des Optimisations (Le Chemin vers H5)

Le projet a été développé suivant une approche empirique : **Hypothèse $\rightarrow$ Implémentation $\rightarrow$ Mesure**.

### 🔴 Baseline : Traitement Séquentiel
*   **Approche :** Lecture d'un message $\rightarrow$ Requête SQL $\rightarrow$ Envoi Kafka $\rightarrow$ Message suivant.
*   **Goulot d'étranglement :** Latence réseau et temps de réponse MySQL bloquant.

### 🟡 H1 : Parallélisation (Concurrent DB Queries)
*   **Approche :** Implémentation de *Worker Pools* pour traiter plusieurs requêtes MySQL simultanément.
*   **Gain :** Utilisation optimisée du CPU et réduction drastique du temps d'attente global.

### 🟡 H2 : Batching & Tuning Kafka
*   **Approche :** Regroupement des messages en lots (*batches*) pour réduire le nombre d'allers-retours réseau et optimisation des paramètres Kafka (`batch.size`, `linger.ms`).
*   **Gain :** Augmentation massive du débit (*throughput*).

### 🟡 H3 : Connection Pooling
*   **Approche :** Mise en place d'un pool de connexions MySQL pour éliminer le coût de création/destruction des sessions TCP.
*   **Gain :** Stabilisation de la latence sous haute charge.

### 🟢 H4 : Mise en Cache Locale (In-Memory Caching)
*   **Approche :** Utilisation d'une cache en mémoire pour stocker les résultats des requêtes fréquentes.
*   **Gain :** Élimination quasi-totale des appels DB pour les utilisateurs récurrents.

### 🌟 H5 : Cache avec Stratégie de Warm-up (Final)
*   **Approche :** Ajout d'une phase de pré-chargement (*Warm-up*) au démarrage du service pour remplir la cache avec les données les plus sollicitées.
*   **Gain :** Suppression du pic de latence initial (*Cold Start*), assurant des performances maximales dès la première seconde.

---

## 🛠️ Stack Technique
*   **Langage :** [Go (Golang)](https://go.dev/)
*   **Messagerie :** [Apache Kafka](https://kafka.apache.org/)
*   **Base de données :** [MySQL](https://www.mysql.com/)
*   **Architecture :** Event-Driven / Distributed Pipeline

---

## 🧪 Guide de Test et Installation

### Pré-requis
- Go 1.21+
- Un cluster Kubernetes opérationnel
- Kafka et MySQL déployés sur le cluster

### Installation & Déploiement
1. **Cloner le dépôt :**
   ```bash
   git clone https://github.com/Es3n13/TP3_System.git
   cd TP3_System
   ```

2. **Déployer les services :**
   Utilisez vos fichiers YAML pour déployer MySQL, Kafka et le service de traitement.

### Procédure de Test (Kubernetes)

Pour tester le pipeline en temps réel et observer les optimisations, suivez ces étapes :

**1. Monitoring des logs (Ouvrez 3 terminaux séparés) :**

*   **Terminal 1 : Suivre le Producteur**
    ```bash
    kubectl logs -f user-producer-job-<POD_ID>
    ```
*   **Terminal 2 : Suivre le Consommateur (Notifications)**
    ```bash
    kubectl logs -f notification-consumer-deployment-<POD_ID> --tail=50
    ```
*   **Terminal 3 : Suivre le Processeur (H5)**
    ```bash
    kubectl logs -f user-processor-h5-<POD_ID>
    ```

**2. Lancer un batch de données :**
Une fois que vos terminaux de logs sont prêts, déclenchez l'envoi des messages avec la commande suivante :
```bash
kubectl replace --force -f user-producer.yaml
```

### Validation des performances
Observez le **Terminal 3 (Processeur)** : vous verrez les statistiques de Cache Warm up et du temps de traitement.
