To stage a workflow template:
```bash
python -m eqtl_catalogue \
  --runner DataflowRunner \
  --project open-targets-genetics-dev \
  --staging_location gs://genetics-portal-dev-staging/beam \
  --template_location gs://genetics_etl_python_playground/beam/eqtl_catalogue \
  --region europe-west1
```

To run a pipeline:
```bash
gcloud dataflow jobs run eqtl-test \
  --gcs-location gs://genetics_etl_python_playground/beam/eqtl_catalogue \
  --region europe-west1
```