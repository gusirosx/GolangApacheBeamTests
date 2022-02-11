export REGION='us-central1'

GOLANG_PROTOBUF_REGISTRATION_CONFLICT=warn go run main.go \
  --runner direct \
  --project=$GOOGLE_CLOUD_PROJECT \
  --region=$REGION \
  --temp_location gs://beanteste/tmp/ \
  --staging_location gs://beanteste/tmp/  