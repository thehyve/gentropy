## How to run a batch job
```bash
# Prepare and upload config.
python3 eqtl_catalogue.py > config.json
# Upload the script.
gsutil cp eqtl_catalogue.py gs://genetics_etl_python_playground/batch/eqtl_catalogue/eqtl_catalogue.py
gsutil cp eqtl_catalogue.sh gs://genetics_etl_python_playground/batch/eqtl_catalogue/eqtl_catalogue.sh
# Submit the batch job.
gcloud batch jobs submit \
  eqtl-catalogue24 \
  --config config.json \
  --location europe-west1
```
