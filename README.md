# Optimisation d'un Pipeline Distribué : TP3 Système

Ce projet explore l'optimisation progressive d'un pipeline de traitement de données distribué. L'objectif principal est d'identifier et de lever les goulots d'étranglement d'une architecture basée sur des files d'attente pour maximiser le débit et minimiser la latence.

## 🎯 Objectif du Projet

Le système implémente un flux de données suivant ce schéma :
**Producteur Utilisateur** $\rightarrow$ **Kafka** $\rightarrow$ **Service d'Application** $\rightarrow$ **Kafka** $\rightarrow$ **Consommateur**.

L'enjeu réside dans l'optimisation du *Service d'Application*, qui doit traiter les messages entrants, interagir avec une base de données MySQL, et transmettre les résultats vers un second topic Kafka.

## 🏗️ Architecture & Évolution Technique

Le projet est structuré autour d'une série d'hypothèses (H) visant à améliorer les performances de manière itérative.

### 0. Baseline : Traitement Synchrone
Le point de départ consiste en un traitement séquentiel : chaque message est lu, traité via une requête SQL unique, puis envoyé à Kafka avant de passer au message suivant. Cette approche est limitée par la latence réseau et le temps de réponse de la base de données.

### 1. H1 : Requêtes Concurrentes via Worker Pools
Pour pallier la nature bloquante des appels DB, nous avons implémenté des **pools de travailleurs (worker pools)**. 
- **Gain :** Le système peut désormais traiter plusieurs requêtes MySQL en parallèle, exploitant mieux les ressources CPU et réduisant le temps d'attente global.

### 2. H2 : Processus de Batching
L'optimisation s'est ensuite portée sur la réduction du nombre d'allers-retours avec l'infrastructure.
- **Batching SQL :** Regroupement de plusieurs requêtes en une seule instruction pour réduire l'overhead du protocole MySQL.
- **Tuning Kafka :** Optimisation des paramètres du producteur Kafka (ex: `linger.ms`, `batch.size`) pour envoyer des lots de messages plutôt que des messages individuels.
- **Gain :** Augmentation significative du débit (*throughput*) grâce à la réduction des interruptions système et des appels réseau.

### 3. H3 : Connection Pooling & Pipeline à Deux Étapes
Afin de stabiliser les performances sous haute charge, l'architecture a évolué vers un modèle plus sophistiqué :
- **Connection Pooling :** Gestion optimisée des connexions MySQL pour éviter le coût de création/destruction fréquent des sessions.
- **Pipeline à deux étapes :** Introduction d'une couche d'**Agrégateurs** qui regroupent les données avant de les distribuer aux **DB Workers**.
- **Gain :** Meilleure gestion de la pression sur la base de données et fluidification du flux de données interne.

## 🚀 Roadmap : Optimisations Futures

Le projet prévoit l'implémentation des étapes suivantes pour atteindre des performances optimales :

- **H4 : Mise en cache locale (In-Memory Caching)** : Implémentation d'une cache en mémoire pour stocker les résultats des requêtes fréquentes et éviter des appels redondants à MySQL.
- **H5 : Warm-up de la Cache** : Stratégie de pré-chargement de la cache avec les données les plus sollicitées au démarrage du service pour éliminer le pic de latence initial (*cold start*).

## 🛠️ Stack Technique

- **Langage :** [Go (Golang)](https://go.dev/)
- **Messagerie :** [Apache Kafka](https://kafka.apache.org/) (via la bibliothèque `confluent-kafka-go`)
- **Base de données :** [MySQL](https://www.mysql.com/)

## 📦 Installation et Lancement

### Prérequis
- Go 1.21+
- Un cluster Kafka opérationnel
- Une instance MySQL configurée

### Installation
```bash
# Cloner le dépôt
git clone https://github.com/Es3n13/TP3_System.git
cd TP3_System

# Installer les dépendances
go mod download
```

### Configuration
Modifiez les variables d'environnement ou le fichier de configuration pour pointer vers vos instances Kafka et MySQL.

### Exécution
```bash
# Lancer le service d'application
go run main.go
```
