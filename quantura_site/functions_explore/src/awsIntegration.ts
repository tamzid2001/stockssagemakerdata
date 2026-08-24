import { Router, Request } from "express";
import admin from "firebase-admin";
import { awsCredentialsProvider } from "@vercel/oidc-aws-credentials-provider";
import { GetCallerIdentityCommand, STSClient } from "@aws-sdk/client-sts";
import { ListTrainingJobsCommand, SageMakerClient } from "@aws-sdk/client-sagemaker";
import type { AutopilotAwsConfig } from "./autopilot";

export type UserAwsIntegration = {
  accountId: string;
  region: string;
  roleArn: string;
  executionRoleArn: string;
  s3Bucket: string;
  status: "connected" | "not_tested" | "connection_failed";
  lastTestedAt: string;
  lastTestMessage: string;
};

const REGIONS = new Set([
  "us-east-1", "us-east-2", "us-west-1", "us-west-2",
  "ca-central-1", "ca-west-1", "eu-central-1", "eu-central-2", "eu-west-1", "eu-west-2", "eu-west-3",
  "eu-north-1", "eu-south-1", "eu-south-2", "ap-east-1", "ap-south-1", "ap-south-2", "ap-northeast-1",
  "ap-northeast-2", "ap-northeast-3", "ap-southeast-1", "ap-southeast-2", "ap-southeast-3", "ap-southeast-4",
  "me-central-1", "me-south-1", "sa-east-1", "af-south-1", "il-central-1",
]);

function clean(value: unknown, max = 300): string {
  return String(value || "").trim().slice(0, max);
}

function roleAccount(roleArn: string): string {
  const match = roleArn.match(/^arn:aws:iam::(\d{12}):role\/[A-Za-z0-9+=,.@_\/-]{1,512}$/);
  return match ? match[1] : "";
}

export function validateAwsIntegration(value: Record<string, unknown>): UserAwsIntegration {
  const accountId = clean(value.accountId, 12);
  const region = clean(value.region, 40);
  const roleArn = clean(value.roleArn, 620);
  const executionRoleArn = clean(value.executionRoleArn, 620);
  const s3Bucket = clean(value.s3Bucket, 63).toLowerCase();
  const assumedAccount = roleAccount(roleArn);
  const executionAccount = roleAccount(executionRoleArn);
  if (!/^\d{12}$/.test(accountId)) throw new Error("Enter a 12-digit AWS account ID.");
  if (!REGIONS.has(region)) throw new Error("Select a supported AWS region.");
  if (!assumedAccount) throw new Error("Enter a valid IAM role ARN for Vercel to assume.");
  if (!executionAccount) throw new Error("Enter a valid SageMaker execution-role ARN.");
  if (assumedAccount !== accountId || executionAccount !== accountId) throw new Error("Both role ARNs must belong to the selected AWS account.");
  if (!/^(?!xn--)(?!.*\.\.)[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(s3Bucket)) throw new Error("Enter a valid S3 bucket name.");
  return {
    accountId,
    region,
    roleArn,
    executionRoleArn,
    s3Bucket,
    status: "not_tested",
    lastTestedAt: "",
    lastTestMessage: "Connection has not been tested.",
  };
}

function publicConfig(data: Record<string, unknown> | undefined): UserAwsIntegration | null {
  if (!data) return null;
  const roleArn = clean(data.roleArn, 620);
  if (!roleArn) return null;
  return {
    accountId: clean(data.accountId, 12),
    region: clean(data.region, 40),
    roleArn,
    executionRoleArn: clean(data.executionRoleArn, 620),
    s3Bucket: clean(data.s3Bucket, 63),
    status: ["connected", "connection_failed"].includes(clean(data.status)) ? clean(data.status) as UserAwsIntegration["status"] : "not_tested",
    lastTestedAt: clean(data.lastTestedAt, 60),
    lastTestMessage: clean(data.lastTestMessage, 240),
  };
}

export async function userFromRequest(req: Request, auth: admin.auth.Auth): Promise<admin.auth.DecodedIdToken> {
  const match = clean(req.headers.authorization, 10000).match(/^Bearer\s+(.+)$/i);
  if (!match) throw new Error("unauthenticated");
  try {
    const user = await auth.verifyIdToken(match[1]);
    if (user.firebase?.sign_in_provider === "anonymous") throw new Error("unauthenticated");
    return user;
  } catch (_error) {
    throw new Error("unauthenticated");
  }
}

function configRef(db: admin.firestore.Firestore, uid: string): admin.firestore.DocumentReference {
  return db.collection("users").doc(uid).collection("integrations").doc("aws");
}

export function credentialsForUser(config: UserAwsIntegration, uid: string) {
  return awsCredentialsProvider({
    audience: "https://sts.amazonaws.com",
    roleArn: config.roleArn,
    roleSessionName: `quantura-${uid.replace(/[^A-Za-z0-9+=,.@_-]/g, "-").slice(0, 48)}`,
    durationSeconds: 3600,
    clientConfig: { region: config.region },
  });
}

export async function resolveUserAutopilotAwsConfig(
  db: admin.firestore.Firestore,
  uid: string
): Promise<AutopilotAwsConfig> {
  const snapshot = await configRef(db, uid).get();
  const config = publicConfig(snapshot.exists ? snapshot.data() as Record<string, unknown> : undefined);
  if (!config) throw new Error("aws_integration_required");
  if (config.status !== "connected") throw new Error("aws_integration_not_verified");
  return {
    region: config.region,
    roleArn: config.executionRoleArn,
    bucket: config.s3Bucket,
    accessKeyId: "",
    secretAccessKey: "",
    sessionToken: "",
    transformInstanceType: "ml.m5.2xlarge",
    credentials: credentialsForUser(config, uid),
  };
}

async function testAwsConnection(config: UserAwsIntegration, uid: string): Promise<{ callerArn: string; accountId: string }> {
  const credentials = credentialsForUser(config, uid);
  const sts = new STSClient({ region: config.region, credentials });
  const identity = await sts.send(new GetCallerIdentityCommand({}));
  if (String(identity.Account || "") !== config.accountId) throw new Error("The assumed role returned a different AWS account.");
  const sagemaker = new SageMakerClient({ region: config.region, credentials });
  await sagemaker.send(new ListTrainingJobsCommand({ MaxResults: 1 }));
  return { callerArn: clean(identity.Arn, 620), accountId: String(identity.Account || "") };
}

export function registerAwsIntegrationRoutes(
  router: Router,
  dependencies: { db: admin.firestore.Firestore; auth: admin.auth.Auth }
): void {
  const { db, auth } = dependencies;
  router.get("/me/aws-integration", async (req, res) => {
    try {
      const user = await userFromRequest(req, auth);
      const snapshot = await configRef(db, user.uid).get();
      res.status(200).json({ ok: true, connected: snapshot.exists, config: publicConfig(snapshot.data() as Record<string, unknown> | undefined) });
    } catch (error) {
      const unauthenticated = String((error as Error).message) === "unauthenticated";
      res.status(unauthenticated ? 401 : 500).json({
        ok: false,
        error: unauthenticated ? "unauthenticated" : "aws_config_read_failed",
        message: unauthenticated ? "Sign in to manage AWS integration." : "The AWS configuration could not be loaded. Try again.",
      });
    }
  });

  router.put("/me/aws-integration", async (req, res) => {
    let user: admin.auth.DecodedIdToken;
    try {
      user = await userFromRequest(req, auth);
    } catch (_error) {
      res.status(401).json({ ok: false, error: "unauthenticated", message: "Sign in to manage AWS integration." });
      return;
    }
    let config: UserAwsIntegration;
    try {
      config = validateAwsIntegration((req.body || {}) as Record<string, unknown>);
    } catch (error) {
      res.status(400).json({ ok: false, error: "invalid_aws_configuration", message: clean((error as Error).message, 240) });
      return;
    }
    try {
      await configRef(db, user.uid).set({
        ...config,
        userId: user.uid,
        authMode: "vercel_oidc",
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: false });
      res.status(200).json({ ok: true, connected: true, config });
    } catch (_error) {
      res.status(500).json({ ok: false, error: "aws_config_write_failed", message: "The AWS configuration could not be saved. Try again." });
    }
  });

  router.post("/me/aws-integration/test", async (req, res) => {
    try {
      const user = await userFromRequest(req, auth);
      const ref = configRef(db, user.uid);
      const snapshot = await ref.get();
      const config = publicConfig(snapshot.data() as Record<string, unknown> | undefined);
      if (!config) {
        res.status(404).json({ ok: false, error: "aws_integration_required", message: "Save an AWS role configuration before testing it." });
        return;
      }
      try {
        const result = await testAwsConnection(config, user.uid);
        const lastTestedAt = new Date().toISOString();
        await ref.set({ status: "connected", lastTestedAt, lastTestMessage: "OIDC role and SageMaker access verified.", updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
        res.status(200).json({ ok: true, status: "connected", accountId: result.accountId, callerArn: result.callerArn, lastTestedAt });
      } catch (testError) {
        const message = /AccessDenied|not authorized/i.test(clean((testError as Error).message, 500))
          ? "The role was assumed, but it lacks SageMaker list permissions. Review the minimum IAM policy."
          : "AWS could not assume or verify this role. Review its Vercel OIDC trust policy and region.";
        await ref.set({ status: "connection_failed", lastTestedAt: new Date().toISOString(), lastTestMessage: message, updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
        res.status(400).json({ ok: false, error: "aws_connection_failed", message });
      }
    } catch (error) {
      const unauthenticated = clean((error as Error).message, 40) === "unauthenticated";
      res.status(unauthenticated ? 401 : 500).json({
        ok: false,
        error: unauthenticated ? "unauthenticated" : "aws_config_read_failed",
        message: unauthenticated ? "Sign in to test AWS integration." : "The saved AWS configuration could not be loaded. Try again.",
      });
    }
  });

  router.delete("/me/aws-integration", async (req, res) => {
    try {
      const user = await userFromRequest(req, auth);
      await configRef(db, user.uid).delete();
      res.status(200).json({ ok: true, connected: false });
    } catch (error) {
      const unauthenticated = clean((error as Error).message, 40) === "unauthenticated";
      res.status(unauthenticated ? 401 : 500).json({
        ok: false,
        error: unauthenticated ? "unauthenticated" : "aws_config_delete_failed",
        message: unauthenticated ? "Sign in to disconnect AWS integration." : "The AWS integration could not be disconnected. Try again.",
      });
    }
  });
}
