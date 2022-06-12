GOOGLE_PROJECT_ID=mlops-3

gcloud builds submit \
--gcs-source-staging-dir gs://polygonio-news-sentiment-test/source \
--gcs-log-dir gs://polygonio-news-sentiment-test/logs \
--tag gcr.io/$GOOGLE_PROJECT_ID/polygonio \
--project $GOOGLE_PROJECT_ID \
--impersonate-service-account 411650743689@cloudbuild.gserviceaccount.com
