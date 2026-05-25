import os
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"

PARQUET_PATH = OUTPUT_DIR / "tickets_enriched_parquet" / "part-00000.parquet"

ANALYSIS_DIR = OUTPUT_DIR / "analyses" / "post_python"
ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

BY_TYPE_JSON = ANALYSIS_DIR / "tickets_by_type_from_parquet.json"
BY_PRIORITY_JSON = ANALYSIS_DIR / "tickets_by_priority_from_parquet.json"
BY_TEAM_JSON = ANALYSIS_DIR / "tickets_by_team_from_parquet.json"
BY_DAY_CSV = ANALYSIS_DIR / "tickets_by_day_from_parquet.csv"
TOP_CLIENTS_CSV = ANALYSIS_DIR / "top_clients_from_parquet.csv"


def main() -> None:
    if not PARQUET_PATH.exists():
        raise SystemExit(f"Parquet not found: {PARQUET_PATH}")

    df = pd.read_parquet(PARQUET_PATH)

    # Analyses (similaires aux sorties JSON du pipeline)
    by_type = df.groupby("type_demande", dropna=False).size().reset_index(name="nb_tickets")
    by_priority = df.groupby("priorite", dropna=False).size().reset_index(name="nb_tickets")
    by_team = df.groupby("equipe_support", dropna=False).size().reset_index(name="nb_tickets")

    if "created_at_ts" in df.columns:
        df["jour"] = pd.to_datetime(df["created_at_ts"]).dt.date
    else:
        df["jour"] = pd.to_datetime(df["created_at"]).dt.date
    by_day = df.groupby("jour", dropna=False).size().reset_index(name="nb_tickets")

    top_clients = (
        df.groupby("client_id", dropna=False)
        .size()
        .reset_index(name="nb_tickets")
        .sort_values("nb_tickets", ascending=False)
        .head(10)
    )

    # Exports
    by_type.to_json(BY_TYPE_JSON, orient="records", lines=True, force_ascii=False)
    by_priority.to_json(BY_PRIORITY_JSON, orient="records", lines=True, force_ascii=False)
    by_team.to_json(BY_TEAM_JSON, orient="records", lines=True, force_ascii=False)
    by_day.to_csv(BY_DAY_CSV, index=False)
    top_clients.to_csv(TOP_CLIENTS_CSV, index=False)

    print("Analyses écrites dans:", ANALYSIS_DIR)
    print("-", BY_TYPE_JSON.name)
    print("-", BY_PRIORITY_JSON.name)
    print("-", BY_TEAM_JSON.name)
    print("-", BY_DAY_CSV.name)
    print("-", TOP_CLIENTS_CSV.name)


if __name__ == "__main__":
    main()
