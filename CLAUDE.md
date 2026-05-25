# InduTech — CLAUDE.md

## Présentation du projet

InduTech est un pipeline de données de tickets support industriel. Il ingère des tickets clients via Redpanda (compatible Kafka), les traite en batch avec PySpark, les enrichit avec une logique de routage métier, et exporte les résultats en fichiers (Parquet, JSON, CSV).

## Architecture

```
[producer_tickets.py]
        |
        v  topic: client_tickets
[Redpanda / Kafka]
        |
        v
[poc_tickets_batch_json.py (PySpark batch)]
        |
        +---> output/tickets_enriched_parquet/
        +---> output/tickets_by_type_json/
        +---> output/analyses/
        |       +---> tickets_by_priority_json/
        |       +---> tickets_by_team_json/
        |       +---> tickets_by_day_csv/
        |       +---> top_clients_csv/
        |
        +---> [analyze_outputs.py (post-traitement pandas)]
                    output/analyses/post_python/
```

Schémas visuels dans `data/` : `architecture_redpanda.png`, `mermaid-diagram-complet.png`.

## Stack technique

| Composant | Technologie |
|-----------|------------|
| Message broker | Redpanda (compatible Kafka) |
| Traitement batch | PySpark 3.5.1 |
| Conteneurisation | Docker / Docker Compose |
| Post-analyse | pandas + pyarrow |
| Stockage sortie | Parquet, JSON, CSV |

## Scripts principaux

| Fichier | Rôle |
|---------|------|
| `producer_tickets.py` | Génère et publie des tickets JSON dans Redpanda |
| `poc_tickets_batch_json.py` | Job PySpark batch : lecture Kafka → enrichissement → exports |
| `analyze_outputs.py` | Post-analyse pandas sur le Parquet généré |
| `consumer_mysql.py` | Consommateur alternatif vers MySQL (désactivé par défaut) |
| `stream_tickets_pyspark.py` | Streaming PySpark vers console (mode dev) |
| `start_spark.ps1` | Bootstrap Spark/Java/Hadoop sous Windows |

## Lancer le projet (Docker — version recommandée)

```powershell
# 1. Nettoyer les sorties précédentes
Remove-Item -Recurse -Force .\output\*

# 2. (Re)construire et démarrer
docker compose up --build -d

# 3. Vérifier les conteneurs (attendre ~30s)
docker compose ps

# 4. Contrôler les logs
docker compose logs producer
docker compose logs pyspark

# 5. Arrêter
docker compose down -v
```

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

Champ enrichi généré : `equipe_support` (technique → Equipe Technique, facturation → Equipe Facturation, compte → Equipe Compte, autres → Equipe Generale).

## Ports Docker

| Service | Port hôte |
|---------|-----------|
| Kafka/Redpanda | 19093 |
| Schema Registry | 18085 |
| PandaProxy | 18086 |
| Admin API | 9648 |

## Règles importantes

- `venv/` et `output/` sont exclus du dépôt (voir `.gitignore`).
- MySQL est désactivé dans la version courante : toutes les sorties passent par les fichiers.
- Sur Windows, si PySpark échoue à écrire, un fallback pandas prend le relais.
- Le job PySpark s'arrête normalement après le batch (exit 0) — comportement attendu.
