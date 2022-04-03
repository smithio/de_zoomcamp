from google.cloud import storage

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

csv_schemas = {
    'atp_matches': [
        {
            "name": "tourney_id",
            "type": "STRING"
        },
        {
            "name": "tourney_name",
            "type": "STRING"
        },
        {
            "name": "surface",
            "type": "STRING"
        },
        {
            "name": "draw_size",
            "type": "INT64"
        },
        {
            "name": "tourney_level",
            "type": "STRING"
        },
        {
            "name": "tourney_date",
            "type": "INT64"
        },
        {
            "name": "match_num",
            "type": "INT64"
        },
        {
            "name": "winner_id",
            "type": "INT64"
        },
        {
            "name": "winner_seed",
            "type": "INT64"
        },
        {
            "name": "winner_entry",
            "type": "STRING"
        },
        {
            "name": "winner_name",
            "type": "STRING"
        },
        {
            "name": "winner_hand",
            "type": "STRING"
        },
        {
            "name": "winner_ht",
            "type": "INT64"
        },
        {
            "name": "winner_ioc",
            "type": "STRING"
        },
        {
            "name": "winner_age",
            "type": "FLOAT64"
        },
        {
            "name": "loser_id",
            "type": "INT64"
        },
        {
            "name": "loser_seed",
            "type": "INT64"
        },
        {
            "name": "loser_entry",
            "type": "STRING"
        },
        {
            "name": "loser_name",
            "type": "STRING"
        },
        {
            "name": "loser_hand",
            "type": "STRING"
        },
        {
            "name": "loser_ht",
            "type": "INT64"
        },
        {
            "name": "loser_ioc",
            "type": "STRING"
        },
        {
            "name": "loser_age",
            "type": "FLOAT64"
        },
        {
            "name": "score",
            "type": "STRING"
        },
        {
            "name": "best_of",
            "type": "INT64"
        },
        {
            "name": "round",
            "type": "STRING"
        },
        {
            "name": "minutes",
            "type": "INT64"
        },
        {
            "name": "w_ace",
            "type": "INT64"
        },
        {
            "name": "w_df",
            "type": "INT64"
        },
        {
            "name": "w_svpt",
            "type": "INT64"
        },
        {
            "name": "w_1stIn",
            "type": "INT64"
        },
        {
            "name": "w_1stWon",
            "type": "INT64"
        },
        {
            "name": "w_2ndWon",
            "type": "INT64"
        },
        {
            "name": "w_SvGms",
            "type": "INT64"
        },
        {
            "name": "w_bpSaved",
            "type": "INT64"
        },
        {
            "name": "w_bpFaced",
            "type": "INT64"
        },
        {
            "name": "l_ace",
            "type": "INT64"
        },
        {
            "name": "l_df",
            "type": "INT64"
        },
        {
            "name": "l_svpt",
            "type": "INT64"
        },
        {
            "name": "l_1stIn",
            "type": "INT64"
        },
        {
            "name": "l_1stWon",
            "type": "INT64"
        },
        {
            "name": "l_2ndWon",
            "type": "INT64"
        },
        {
            "name": "l_SvGms",
            "type": "INT64"
        },
        {
            "name": "l_bpSaved",
            "type": "INT64"
        },
        {
            "name": "l_bpFaced",
            "type": "INT64"
        },
        {
            "name": "winner_rank",
            "type": "INT64"
        },
        {
            "name": "winner_rank_points",
            "type": "INT64"
        },
        {
            "name": "loser_rank",
            "type": "INT64"
        },
        {
            "name": "loser_rank_points",
            "type": "INT64"
        }
    ],
    'atp_rankings': [
        {
            "name": "ranking_date",
            "type": "INT64"
        },
        {
            "name": "rank",
            "type": "INT64"
        },
        {
            "name": "player",
            "type": "INT64"
        },
        {
            "name": "points",
            "type": "INT64"
        }
    ],
    'atp_players': [
        {
            "name": "player_id",
            "type": "INT64"
        },
        {
            "name": "name_first",
            "type": "STRING"
        },
        {
            "name": "name_last",
            "type": "STRING"
        },
        {
            "name": "hand",
            "type": "STRING"
        },
        {
            "name": "dob",
            "type": "INT64"
        },
        {
            "name": "ioc",
            "type": "STRING"
        },
        {
            "name": "height",
            "type": "INT64"
        },
        {
            "name": "wikidata_id",
            "type": "STRING"
        }
    ]
}