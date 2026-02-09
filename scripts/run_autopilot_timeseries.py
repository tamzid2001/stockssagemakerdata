#!/usr/bin/env python3
"""Run SageMaker Autopilot time series forecasting using fetched stock data."""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
from typing import Optional

import boto3
import pandas as pd
import pytz
from pandas.tseries.holiday import USFederalHolidayCalendar

from fetch_data import colab_download_price_csv, ensure_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Autopilot time series forecast using stock history.")
    parser.add_argument("--tickers", required=True, help="Comma-separated tickers, e.g. AAPL,MSFT")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD (defaults to today)")
    parser.add_argument("--interval", default="1d", choices=["1d", "1h"], help="Data interval")
    parser.add_argument("--s3-bucket", required=True, help="S3 bucket for Autopilot input/output")
    parser.add_argument("--s3-prefix", default="autopilot-stock-forecast", help="S3 prefix for data")
    parser.add_argument("--role-arn", required=True, help="IAM role ARN for SageMaker Autopilot job")
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"), help="AWS region")
    parser.add_argument("--forecast-horizon", type=int, default=7, help="Forecast horizon in days")
    parser.add_argument("--frequency", default="1D", help="Forecast frequency (e.g. 1D, 1H)")
    parser.add_argument("--max-runtime-seconds", type=int, default=40 * 60, help="Max AutoML runtime")
    parser.add_argument("--job-name-prefix", default="automl-ts", help="AutoML job name prefix")
    parser.add_argument(
        "--skip-if-monday-holiday",
        action="store_true",
        help="Skip the run if the previous Monday is a US federal holiday.",
    )
    parser.add_argument("--workdir", default="data/autopilot", help="Local working directory")
    return parser.parse_args()


def was_monday_holiday(reference_time: datetime) -> bool:
    monday = (reference_time - timedelta(days=reference_time.weekday())).date()
    holidays = USFederalHolidayCalendar().holidays(start=monday, end=monday)
    return not holidays.empty


def build_training_dataframe(csv_paths: list[str]) -> pd.DataFrame:
    frames = []
    for path in csv_paths:
        frame = pd.read_csv(path)
        frame = frame.rename(columns={"Item_Id": "item_id", "Date": "ts", "Price": "target"})
        frames.append(frame)
    combined = pd.concat(frames, ignore_index=True)
    combined["ts"] = pd.to_datetime(combined["ts"])
    combined = combined.sort_values(["item_id", "ts"], ascending=True)
    return combined


def upload_to_s3(local_path: str, bucket: str, key: str, region: str) -> str:
    s3 = boto3.client("s3", region_name=region)
    s3.upload_file(local_path, bucket, key)
    return f"s3://{bucket}/{key}"


def main() -> None:
    args = parse_args()
    timezone = pytz.timezone("America/New_York")
    now_ny = datetime.now(timezone)

    if args.skip_if_monday_holiday and was_monday_holiday(now_ny):
        monday = (now_ny - timedelta(days=now_ny.weekday())).date()
        print(f"Skipping run because Monday {monday} is a US federal holiday.")
        return

    ensure_dir(args.workdir)
    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    if not tickers:
        raise SystemExit("No tickers provided.")

    end_date = args.end or now_ny.date().strftime("%Y-%m-%d")

    csv_paths: list[str] = []
    for ticker in tickers:
        out_path = os.path.join(args.workdir, f"{ticker.upper()}_{args.start}_{end_date}.csv")
        csv_paths.append(
            colab_download_price_csv(
                ticker,
                start=args.start,
                end=end_date,
                interval=args.interval,
                out_path=out_path,
            )
        )

    training_df = build_training_dataframe(csv_paths)
    training_path = os.path.join(args.workdir, "autopilot_ts_train.csv")
    training_df.to_csv(training_path, index=False)

    s3_train_key = f"{args.s3-prefix}/data/train/autopilot_ts_train.csv"
    s3_output_prefix = f"s3://{args.s3_bucket}/{args.s3_prefix}/output"
    upload_to_s3(training_path, args.s3_bucket, s3_train_key, args.region)

    job_timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    automl_job_name = f"{args.job_name_prefix}-{job_timestamp}"

    automl_job_input_data_config = [
        {
            "ChannelType": "training",
            "ContentType": "text/csv;header=present",
            "CompressionType": "None",
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": f"s3://{args.s3_bucket}/{args.s3_prefix}/data/train/",
                }
            },
        }
    ]

    forecast_horizon = args.forecast_horizon
    if args.frequency.upper().endswith("H"):
        forecast_horizon *= 24

    automl_problem_type_config = {
        "TimeSeriesForecastingJobConfig": {
            "CompletionCriteria": {"MaxAutoMLJobRuntimeInSeconds": args.max_runtime_seconds},
            "ForecastFrequency": args.frequency.upper(),
            "ForecastHorizon": forecast_horizon,
            "ForecastQuantiles": ["p10", "p50", "p90"],
            "TimeSeriesConfig": {
                "TargetAttributeName": "target",
                "TimestampAttributeName": "ts",
                "ItemIdentifierAttributeName": "item_id",
            },
        }
    }

    sm = boto3.client("sagemaker", region_name=args.region)
    sm.create_auto_ml_job_v2(
        AutoMLJobName=automl_job_name,
        AutoMLJobInputDataConfig=automl_job_input_data_config,
        OutputDataConfig={"S3OutputPath": s3_output_prefix},
        AutoMLProblemTypeConfig=automl_problem_type_config,
        AutoMLJobObjective={"MetricName": "AverageWeightedQuantileLoss"},
        RoleArn=args.role_arn,
    )

    print(f"Started Autopilot job: {automl_job_name}")


if __name__ == "__main__":
    main()
