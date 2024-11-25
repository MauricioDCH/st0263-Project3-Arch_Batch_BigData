#!/bin/bash

# Assign parameters to variables
ACCESS_KEY=$1
SECRET_KEY=$2
SESSION_TOKEN=$3

# Configure AWS CLI
aws configure set aws_access_key_id "$ACCESS_KEY"
aws configure set aws_secret_access_key "$SECRET_KEY"
aws configure set aws_session_token "$SESSION_TOKEN"
aws configure set region "us-east-1"

echo "AWS credentials and region configured successfully."
