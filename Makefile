export IMAGE_NAME=polygonio-news-sentiment-data

local-build:
	docker build --no-cache -t polygonio-news-sentiment-repo/$(IMAGE_NAME) .

local-run:
	docker run --rm polygonio-news-sentiment-repo/$(IMAGE_NAME)

local-run-up:
	docker run -e POLYGON_API_KEY=$(POLYGON_API_KEY) --entrypoint bash -itd polygonio-news-sentiment-repo/$(IMAGE_NAME)

gcp-auth:
#	gcloud components update
#	gcloud auth login
	gcloud config set project mlops-3
	gcloud auth configure-docker us-central1-docker.pkg.dev

gcp-build:
	docker build -t "us-central1-docker.pkg.dev/mlops-3/polygonio-news-sentiment-repo/$(IMAGE_NAME)" .

gcp-push:
	docker push us-central1-docker.pkg.dev/mlops-3/polygonio-news-sentiment-repo/$(IMAGE_NAME)

gcp-run:
	gcloud run deploy polygonio-news-sentiment-data-v2 \
  	--image us-central1-docker.pkg.dev/mlops-3/polygonio-news-sentiment-repo/$(IMAGE_NAME) \
  	--platform managed \
  	--region us-central1 \
  	--project mlops-3
