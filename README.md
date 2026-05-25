# InduTech — Pipeline tickets support (Redpanda/Kafka + PySpark)

## Présentation

InduTech met en place un pipeline de données de tickets support industriel. Des tickets sont produits vers un topic Kafka (Redpanda), consommés en batch avec PySpark, enrichis avec une logique de routage métier, puis exportés en fichiers (Parquet/JSON/CSV) pour analyses. La version principale est entièrement conteneurisée sans dépendance MySQL.

## Architecture

Schéma global : `data/architecture_redpanda.png`

![Architecture](data/architecture_redpanda.png)

Schéma du pipeline : `data/mermaid-ai-diagram-2026-03-20-084424.png`

![Pipeline](data/mermaid-ai-diagram-2026-03-20-084424.png)

**Vidéo explicative du pipeline** : [Regarder la démonstration](https://www.loom.com/share/96ff3f34e3ef46ba83a6c74909f0c6f3)

### Flux de données (batch JSON)

```
[Producer Kafka] -> topic `client_tickets`
        |
        v
[PySpark batch read (Kafka)]
        |
        v
[Parse JSON + enrichissement]
        |
        +--> Fichiers de sortie :
              - output/tickets_enriched_parquet/
              - output/tickets_by_type_json/
              - output/analyses/*
```

---

## Cloner le projet

```bash
git clone https://github.com/PascalDuval/InduTechData.git
cd InduTechData
```

### Prérequis

- **Docker Desktop** ≥ 4.x (démarré avant toute utilisation)
- Python 3.10+ (pour la version locale uniquement)
- Java 11+ et Apache Spark 3.5.x (version locale uniquement)

### Installation Python (version locale)

```bash
python -m venv venv

# Windows
.\venv\Scripts\Activate.ps1

# Linux/Mac
source venv/bin/activate

pip install -r requirements.txt
```

---

## Contenu du dépôt

```
InduTechData/
├── data/                          # Diagrammes et documentation
├── scripts/                       # Scripts Spark complémentaires
├── tests/                         # Tests Kafka producer/consumer
├── producer_tickets.py            # Producteur Kafka de tickets
├── poc_tickets_batch_json.py      # Batch PySpark → exports fichiers (version principale)
├── poc_tickets_batch.py           # Batch PySpark → MySQL (alternatif)
├── consumer_mysql.py              # Consommateur Kafka → MySQL
├── stream_tickets_pyspark.py      # Streaming PySpark → console
├── analyze_outputs.py             # Analyses post-pipeline (pandas sur Parquet)
├── test_conn.py                   # Test de connexion Kafka/Redpanda
├── test_pyspark.py                # Test rapide PySpark
├── start_spark.ps1                # Bootstrap Spark/Java/Hadoop (Windows)
├── docker-compose.yml             # Orchestration Docker
├── Dockerfile.producer            # Image Docker producteur
├── Dockerfile.pyspark             # Image Docker PySpark batch
└── requirements.txt               # Dépendances Python
```

---

## Démarrage avec Docker (version recommandée)

### Séquence complète

```powershell
# 1. Nettoyer les sorties précédentes
Remove-Item -Recurse -Force .\output\*

# 2. Arrêter les conteneurs existants
docker compose down

# 3. Construire et démarrer
docker compose up --build -d

# 4. Attendre ~30 secondes puis vérifier
docker compose ps
```

Attendu : 3 conteneurs UP (`redpanda`, `producer_tickets`, `pyspark_batch`).

```powershell
# 5. Vérifier le topic Kafka
docker compose exec -T redpanda rpk topic list
# Attendu : client_tickets

# 6. Vérifier les logs du producteur
docker compose logs producer

# 7. Vérifier les logs PySpark
docker compose logs pyspark
# Attendu : traitement terminé sans erreur (exit 0)

# 8. Vérifier les fichiers de sortie
docker compose exec pyspark ls -la /app/output/

# 9. Arrêter et nettoyer
docker compose down -v
```

### Conteneurs

| Conteneur | Rôle |
|-----------|------|
| `redpanda` | Broker Kafka/Redpanda |
| `producer_tickets` | Génère et publie des tickets toutes les secondes |
| `pyspark_batch` | Lit le topic, traite et exporte en fichiers |

### Ports exposés

| Service | Port |
|---------|------|
| Kafka/Redpanda | 19093 |
| Schema Registry | 18085 |
| PandaProxy | 18086 |
| Admin API | 9648 |

---

## Démarrage local (sans Docker)

```powershell
# 1. Activer le venv
.\venv\Scripts\Activate.ps1

# 2. Démarrer Redpanda (dans son propre dossier)
cd docker-redpanda
docker compose up -d
cd ..

# 3. Vérifier la connexion Kafka
python test_conn.py

# 4. Lancer le producteur de tickets
python producer_tickets.py

# 5. Lancer le batch (dans un autre terminal)
python poc_tickets_batch_json.py

# 6. Vérifier les sorties
Get-ChildItem .\output\tickets_enriched_parquet -Force
Get-ChildItem .\output\tickets_by_type_json -Force
Get-ChildItem .\output\analyses -Force
```

---

## Script clé : `poc_tickets_batch_json.py`

### Ce que fait le script

- Lit le topic Kafka `client_tickets` en mode batch (snapshot `earliest → latest`)
- Parse les messages JSON selon un schéma explicite
- Convertit `created_at` en timestamp réel
- Enrichit chaque ticket avec la colonne `equipe_support` (routage métier)
- Calcule plusieurs agrégations analytiques
- Exporte les résultats en Parquet, JSON et CSV
- Fallback pandas automatique si PySpark échoue à écrire (Windows)

### Analyses générées dans `output/analyses/`

| Dossier | Contenu |
|---------|---------|
| `tickets_by_priority_json/` | Tickets par priorité |
| `tickets_by_team_json/` | Tickets par équipe support |
| `tickets_by_day_csv/` | Tickets par jour |
| `top_clients_csv/` | Top 10 clients |

---

## Post-analyse avec `analyze_outputs.py`

Relit le Parquet généré et produit des fichiers dans `output/analyses/post_python/` :

```powershell
.\venv\Scripts\Activate.ps1
python analyze_outputs.py
```

Résultats : `tickets_by_type.json`, `tickets_by_priority.json`, `tickets_by_team.json`, `tickets_by_day.csv`, `top_clients.csv`.

---

## Modèle de données — ticket

```json
{
  "ticket_id": 42,
  "client_id": 7,
  "created_at": "2026-03-20T10:15:30",
  "demande": "Problème de connexion",
  "type_demande": "technique",
  "priorite": "haute"
}
```

Champ enrichi : `equipe_support`
- `technique` → `Equipe Technique`
- `facturation` → `Equipe Facturation`
- `compte` → `Equipe Compte`
- autres → `Equipe Generale`

---

## Format des fichiers exportés

Les sorties Spark sont des dossiers contenant :
- **JSON** : `part-*.json` (JSON Lines)
- **Parquet** : `part-*.parquet`
- **CSV** : `part-*.csv`

Lire un fichier Parquet :

```python
import pandas as pd
df = pd.read_parquet("output/tickets_enriched_parquet/part-00000.parquet")
print(df.head())
```

---

## Dépannage

| Problème | Solution |
|----------|----------|
| Docker Desktop non démarré | Ouvrir Docker Desktop, attendre l'icône verte |
| Erreur `pipe/dockerDesktopLinuxEngine` | Redémarrer Docker Desktop en administrateur |
| PySpark ne trouve pas le topic | Attendre 30s que Redpanda soit prêt |
| Exports vides sur Windows | Normal si fallback pandas — vérifier `part-00000.*` |
| Port déjà utilisé | Vérifier avec `netstat -an | findstr 19093` |

---

## Stack technique

| Composant | Version |
|-----------|---------|
| PySpark | 3.5.1 |
| kafka-python | latest |
| pandas | latest |
| pyarrow | latest |
| Redpanda | dev-container |

## Licence

Projet pédagogique — OpenClassRooms Data Engineer.
