# Visualizing ATP tennis data

## [🎾Final dashboard in Google Datastudio](https://datastudio.google.com/reporting/6cdac7b4-ec21-4ce4-83a9-86713c1b3e70)
![](/project/gifs/demo.gif)
----

## Project steps:

### 1. Provision resources with Terraform - [scripts](/project/terraform/)
- Google storage bucket
- BigQuery datasets (external, internal, production)

### 2. Data Ingestion with Airflow - [scripts](/project/airflow/)
From [source](https://github.com/JeffSackmann/tennis_atp) to Google Storage

- ATP_matches (will ignore all not main events for now)
- ATP_rankings
- ATP_players

### 3. Data Transformation with DBT - [scripts](/project/dbt/)
- rankings
    - filter only top 100 players per every ranking week
- matches
    - Aggregate top players' stats per every ranking week
        - career match stats
        - last 52 weeks match stats
        - last 52 weeks serve stats
        - last 52 weeks breakpoints stats
    - Current winnersof main tournament per every ranking week
        - Grand slam
        - Master 1000

### 4. [Dashboard](https://datastudio.google.com/reporting/6cdac7b4-ec21-4ce4-83a9-86713c1b3e70)
- current week filter
- top 100 players list with match stats
- current GS & Masters winners
- plots for:
    - serve stats: aces - df scatter
    - own breakpoints graph
    - faced breakpoints graph