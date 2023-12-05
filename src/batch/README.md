## How to run data ingestion using Google Batch

Specify which data source to ingest:
```bash
export MODULE=ukbb_pqtl
```

Run:
```bash
# Upload code.
gsutil -m cp -r . gs://genetics_etl_python_playground/batch/code
# Submit batch job.
gcloud batch jobs submit \
  "${MODULE//_/-}-$(date --utc +"%Y%m%d-%H%M%S")" \
  --config <(python3 ${MODULE}.py) \
  --location europe-west1
```

## Note on Synapse authentication
To access data from Synapse controlled access datasets:
1. Go to https://www.synapse.org/#!PersonalAccessTokens.
2. Create a new token with “View” and “Download” permissions.
3. Store that token in [Google Cloud Secret Manager](https://console.cloud.google.com/security/secret-manager) as `synapse_token`.

Note that an unused token expires after 180 days, and will need to be generared again and updated in Google Cloud.
