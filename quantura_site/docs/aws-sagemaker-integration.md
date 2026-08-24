# AWS / SageMaker integration

Quantura uses Vercel OIDC and AWS STS `AssumeRoleWithWebIdentity`. Users do not
enter permanent AWS access keys. The saved per-user configuration contains only
an AWS account ID, region, assumable role ARN, SageMaker execution-role ARN,
and S3 bucket name. Firebase ID-token verification scopes every server request
to the caller's own `users/{uid}/integrations/aws` document. Firestore rules
deny all direct client reads and writes to that collection.

## Role trust policy

Create the Vercel OIDC provider described in Vercel's AWS OIDC documentation,
then attach a trust policy to the role Quantura will assume. Replace the owner
and project values if the production Vercel project changes.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::YOUR_ACCOUNT_ID:oidc-provider/oidc.vercel.com/YOUR_VERCEL_TEAM_SLUG"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "oidc.vercel.com/YOUR_VERCEL_TEAM_SLUG:aud": "https://sts.amazonaws.com"
        },
        "StringLike": {
          "oidc.vercel.com/YOUR_VERCEL_TEAM_SLUG:sub": "owner:YOUR_VERCEL_TEAM_SLUG:project:quantura-api:environment:production"
        }
      }
    }
  ]
}
```

## Minimum assumed-role permissions

Scope the bucket and execution-role resources to the exact values configured
for the user. Additional `Describe*` permissions may be needed when Quantura
adds another SageMaker job type; do not grant `AdministratorAccess`.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "QuanturaDatasetBucket",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::YOUR_BUCKET"
    },
    {
      "Sid": "QuanturaDatasetObjects",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListMultipartUploadParts", "s3:AbortMultipartUpload"],
      "Resource": "arn:aws:s3:::YOUR_BUCKET/quantura/*"
    },
    {
      "Sid": "QuanturaSageMakerJobs",
      "Effect": "Allow",
      "Action": [
        "sagemaker:CreateAutoMLJobV2",
        "sagemaker:DescribeAutoMLJobV2",
        "sagemaker:ListTrainingJobs",
        "sagemaker:CreateModel",
        "sagemaker:CreateTransformJob",
        "sagemaker:DescribeTransformJob",
        "sagemaker:AddTags"
      ],
      "Resource": "*"
    },
    {
      "Sid": "PassOnlySageMakerExecutionRole",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_SAGEMAKER_EXECUTION_ROLE",
      "Condition": {
        "StringEquals": {
          "iam:PassedToService": "sagemaker.amazonaws.com"
        }
      }
    }
  ]
}
```

The SageMaker execution role itself needs access to the same bucket prefix and
the service permissions required by the selected SageMaker algorithm. AWS bills
the connected account because all SageMaker/S3 API calls use that account's
assumed identity; saving a configuration alone creates no resources and incurs
no SageMaker charge.
