#!/bin/bash
set -e
PROJECT=branch-delivery-tracker
REGION=asia-southeast1
SERVICE=inventory-tracker
BUCKET=${PROJECT}-data

gcloud config set project $PROJECT
gcloud services enable run.googleapis.com cloudbuild.googleapis.com storage.googleapis.com --quiet
gsutil mb -p $PROJECT -l $REGION gs://$BUCKET 2>/dev/null || echo "Bucket already exists"

gcloud run deploy $SERVICE \
  --source . \
  --region $REGION \
  --platform managed \
  --allow-unauthenticated \
  --set-env-vars DATA_DIR=/data \
  --add-volume=name=data-vol,type=cloud-storage,bucket=$BUCKET \
  --add-volume-mount=volume=data-vol,mount-path=/data \
  --memory 1Gi \
  --timeout 300 \
  --quiet

gcloud run services describe $SERVICE --region $REGION --format='value(status.url)'
