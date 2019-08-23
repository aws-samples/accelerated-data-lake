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

echo "Packaging the stagine-engine functions..."
sam package --template-file ./stagingEngine.yaml --output-template-file stagingEngineDeploy.yaml --s3-bucket ${ENVIRONMENT_PREFIX}stagingenginecodepackages

echo "Deploying the stagine-engine functions..."
sam deploy --template-file stagingEngineDeploy.yaml --stack-name ${ENVIRONMENT_PREFIX}datalake-staging-engine --capabilities CAPABILITY_IAM --parameter-overrides EnvironmentPrefix=${ENVIRONMENT_PREFIX}

