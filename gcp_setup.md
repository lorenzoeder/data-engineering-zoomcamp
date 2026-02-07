Install gcloud following Google online tutorial

gcloud init

Make bucket:
gsutil mb -l europe-west2 gs://taxi_ml_model_a2e7442e-cebd-47b3-bd4

Extract model to GCS bucket:
bq --project_id=project-a2e7442e-cebd-47b3-bd4 --location=europe-west2 extract -m ny_taxi.tip_model gs://taxi_ml_model_a2e7442e-cebd-47b3-bd4/tip_model