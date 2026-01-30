#!/bin/bash
# Setup script for AWS integration
# This script configures AWS credentials from environment variables

set -e

echo "Setting up AWS credentials..."

# Check if credentials are set
if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo "ERROR: AWS_ACCESS_KEY_ID environment variable is not set"
    echo "Please add AWS_ACCESS_KEY_ID to your GitHub Codespaces secrets"
    exit 1
fi

if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "ERROR: AWS_SECRET_ACCESS_KEY environment variable is not set"
    echo "Please add AWS_SECRET_ACCESS_KEY to your GitHub Codespaces secrets"
    exit 1
fi

# Configure AWS CLI
aws configure set aws_access_key_id "$AWS_ACCESS_KEY_ID"
aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY"
aws configure set region us-east-2
aws configure set output json

echo "AWS CLI configured successfully!"
echo "Testing S3 bucket access..."

# Try to access the stockscompute bucket
if aws s3 ls s3://stockscompute/ &>/dev/null; then
    echo "✓ Successfully connected to S3 bucket: stockscompute"
else
    echo "⚠ Could not list S3 bucket (permission issue - this may be expected)"
    echo "  The credentials are configured and will be used for uploads"
fi

echo "AWS setup complete!"
