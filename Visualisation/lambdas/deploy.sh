#!/bin/sh
set -e

if ! [ "$(sam --version)" ]; then
  echo "Missing aws-sam-cli.. Please install prior to running the deploy script."
  exit 1
fi

if [ $# -eq 0 ]; then
    echo "Missing params. Usage: ./deploy.sh <ENVIRONMENT_PREFIX>"
    exit 1
fi

ENVIRONMENT_PREFIX=$1
echo "Env prefix = ${ENVIRONMENT_PREFIX}"

echo "Packaging the elasticsearch functions..."
sam package --template-file ./lambdaDeploy.yaml --output-template-file lambdaDeployCFN.yaml --s3-bucket ${ENVIRONMENT_PREFIX}visualisationcodepackages

echo "Deploying the elasticsearch functions..."
sam deploy --template-file lambdaDeployCFN.yaml --stack-name ${ENVIRONMENT_PREFIX}datalake-elasticsearch-lambdas --capabilities CAPABILITY_IAM --parameter-overrides EnvironmentPrefix=${ENVIRONMENT_PREFIX}

