To stage a workflow template:
```bash
python -m eqtl_catalogue \
  --runner DataflowRunner \
  --project open-targets-genetics-dev \
  --staging_location gs://genetics-portal-dev-staging/beam \
  --template_location gs://genetics_etl_python_playground/beam/eqtl_catalogue \
  --region europe-west1 \
  --requirements_file requirements.txt
```

To run a pipeline:
```bash
gcloud dataflow jobs run eqtl-test \
  --gcs-location gs://genetics_etl_python_playground/beam/eqtl_catalogue \
  --region europe-west1 \
  --worker-machine-type n2-highmem-4 \
  --additional-experiments no_use_multiple_sdk_containers
```

## How to run a batch job
```bash
wget -q -O eqtl_input.tsv https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/master/tabix/tabix_ftp_paths_imported.tsv
export NUMBER_OF_JOBS=$(tail -n+2 eqtl_input.tsv | wc -l)
gsutil cp eqtl_input.tsv eqtl_catalogue.py gs://gs://genetics_etl_python_playground/batch/eqtl_catalogue
gcloud batch jobs submit \
  eqtl_catalogue \

```
