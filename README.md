# azure-data-engineering-pipeline
#  Projet d'Ingénierie des Données sur Azure

##  Description
Ce projet met en place une architecture moderne d’ingénierie des données sur **Microsoft Azure**, depuis l’ingestion jusqu’à l’exposition des données.  
L’objectif est de créer un pipeline complet permettant de :
- Ingest les données brutes depuis **SQL Server** avec **Azure Data Factory**,
- Stocker en **Data Lake Storage Gen2** (zones Bronze, Silver, Gold),
- Transformer les données avec **Azure Databricks**,
- Exposer les données via **Azure Synapse Analytics** (vues et procédures stockées),
- Sécuriser les secrets (connexions, credentials) avec **Azure Key Vault**.

---

##  Architecture
Le pipeline suit une approche **Medallion (Bronze, Silver, Gold)** :
Sources → Azure Data Factory → Data Lake (Bronze)
→ Azure Databricks → Data Lake (Silver / Gold)
→ Azure Synapse Analytics (vues + procédures)

 **Azure Key Vault** est utilisé tout au long du pipeline pour sécuriser les clés et secrets.

<img width="1536" height="1024" alt="Architecture Azure projet" src="https://github.com/user-attachments/assets/f4a7e8c0-fc5e-4e6a-94bd-eebe2102de8c" />

---

## ⚙️ Étapes principales

### 1 Source : **SQL Server**
- Données transactionnelles et opérationnelles.
- Connexion sécurisée via **Azure Key Vault**.

### 2 Ingestion avec **Azure Data Factory**
- Pipelines configurés pour extraire les données depuis SQL Server.
- Stockage initial dans la zone **Bronze** du Data Lake.

### 3 Stockage dans **Azure Data Lake Gen2**
- Organisation en zones :
  - **Bronze** : données brutes,
  - **Silver** : données nettoyées et conformes,
  - **Gold** : données prêtes pour l’analyse.

### 4 Transformation avec **Azure Databricks**
- Notebooks Spark (PySpark) pour :
  - Nettoyage,
  - Normalisation,
  - Agrégation des données.

### 5 Exposition avec **Azure Synapse**
- Création de **vues** et **procédures stockées**,
- Automatisation via pipeline Synapse,
- Données prêtes pour la BI ou le reporting.

---

##  Contenu du Repository
- `adf/` → JSON des pipelines Azure Data Factory,
- `databricks/` → Notebooks de transformation (PySpark),
- `synapse/` → Scripts SQL pour vues et procédures stockées,
- `docs/` → Documentation et notes supplémentaires,
- `architecture.png` → Schéma de l’architecture.

---

##  Technologies utilisées
- **SQL Server** (source de données)
- **Azure Data Factory** (ETL/ELT)
- **Azure Data Lake Storage Gen2**
- **Azure Databricks (Spark, PySpark)**
- **Azure Synapse Analytics**
- **Azure Key Vault** (gestion sécurisée des secrets)
- **GitHub** (versionning & documentation)

---

##  Auteurs
**Mohamed Lamine Sanoh**


