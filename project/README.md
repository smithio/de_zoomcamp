# Visualizing ATP tennis data

## Steps:

### 1. Provision resources with Terraform
- Google storage bucket
- BigQuery datasets (external, internal, production)

### 2. Ingest data from source to Google Storage
Source - https://github.com/JeffSackmann/tennis_atp
- ATP_matches (will ignore all not main events for now)
- ATP_rankings
- ATP_players

### 3. Data Transformation
- matches
    - Aggregate players' stats (aces, wins, win ratio)
    - main tournament winners
- rankings
    - filter only top 100 per every week

### 4. Dashboard
- current week filter
- top 10 players
- last GS & Masters winners
- some lists
    - most aces
    - most wins
    - most win ratio
