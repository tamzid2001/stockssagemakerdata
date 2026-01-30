#!/usr/bin/env python3
"""Upload a predictions CSV file to AWS S3.

Simple utility to upload a locally created predictions CSV file to the
stockscompute bucket in the predictions folder.

CSV format expected:
- Any format is supported (user creates the CSV locally)

Example CSV columns:
- Ticker, Date, Prediction, Confidence
"""
from __future__ import annotations
import argparse
import os
from typing import Optional
from datetime import datetime

import boto3
from botocore.exceptions import ClientError


def upload_predictions_to_s3(
    csv_file_path: str,
    bucket_name: str,
    s3_prefix: str = "predictions/",
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    region_name: str = "us-east-2",
) -> str:
    """Upload a predictions CSV file to S3.
    
    Args:
        csv_file_path: Path to local CSV file
        bucket_name: S3 bucket name
        s3_prefix: S3 key prefix (default: predictions/)
        aws_access_key_id: AWS access key (uses AWS_ACCESS_KEY_ID env var if None)
        aws_secret_access_key: AWS secret key (uses AWS_SECRET_ACCESS_KEY env var if None)
        region_name: AWS region
        
    Returns:
        S3 object key where file was uploaded
    """
    # Validate local file exists
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"CSV file not found: {csv_file_path}")
    
    # Use environment variables for AWS credentials if not provided
    if aws_access_key_id is None:
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    if aws_secret_access_key is None:
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    if not aws_access_key_id or not aws_secret_access_key:
        raise RuntimeError(
            "AWS credentials not provided. Set AWS_ACCESS_KEY_ID and "
            "AWS_SECRET_ACCESS_KEY environment variables or pass them as arguments."
        )
    
    # Initialize S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )
    
    # Read file
    print(f"Reading CSV file: {csv_file_path}")
    with open(csv_file_path, "r") as f:
        csv_content = f.read()
    
    # Generate S3 key with timestamp
    file_name = os.path.basename(csv_file_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Insert timestamp before file extension
    name, ext = os.path.splitext(file_name)
    s3_file_name = f"{name}_{timestamp}{ext}"
    s3_key = os.path.join(s3_prefix, s3_file_name).replace("\\", "/")
    
    # Upload to S3
    try:
        print(f"Uploading to S3...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_content.encode("utf-8"),
            ContentType="text/csv",
        )
        print(f"✓ Successfully uploaded s3://{bucket_name}/{s3_key}")
        return s3_key
    except ClientError as e:
        raise RuntimeError(f"Failed to upload to S3: {e}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Upload predictions CSV file to S3"
    )
    p.add_argument(
        "--file", "-f", required=True, help="Path to local CSV file (e.g., predictions.csv)"
    )
    p.add_argument(
        "--bucket", "-b", required=True, help="S3 bucket name"
    )
    p.add_argument(
        "--prefix", "-p", default="predictions/", help="S3 key prefix (default: predictions/)"
    )
    p.add_argument(
        "--region", "-r", default="us-east-2", help="AWS region (default: us-east-2)"
    )
    p.add_argument(
        "--access-key", default=None, help="AWS access key ID (uses AWS_ACCESS_KEY_ID env var if not provided)"
    )
    p.add_argument(
        "--secret-key", default=None, help="AWS secret access key (uses AWS_SECRET_ACCESS_KEY env var if not provided)"
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    
    try:
        s3_key = upload_predictions_to_s3(
            csv_file_path=args.file,
            bucket_name=args.bucket,
            s3_prefix=args.prefix,
            aws_access_key_id=args.access_key,
            aws_secret_access_key=args.secret_key,
            region_name=args.region,
        )
        print(f"✓ Upload complete!")
    except Exception as e:
        print(f"✗ Upload failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()
