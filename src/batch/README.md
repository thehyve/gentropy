## How to run a batch job

Set up:
```bash
export MODULE=eqtl_catalogue
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
