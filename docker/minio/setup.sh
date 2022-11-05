#!/bin/sh

# We set this so that we see all the executed lines in the console
set -x

echo "Creating s3 creds bucket on minio ${S3_ENDPOINT}"
mc alias set s3 "${S3_ENDPOINT}" "${AWS_ACCESS_KEY_ID}" "${AWS_SECRET_KEY}" --api S3v4

echo "Creating s3 ${S3_BUCKET} bucket on minio"
mc mb s3/"${S3_BUCKET}" --ignore-existing

set +x
