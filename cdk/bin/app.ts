#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { DeploymentPipelineStack } from '../lib/deployment-pipeline-stack';

const app = new cdk.App();

// Deploy across all three environments
for (const env of ['dev', 'staging', 'prod'] as const) {
  new DeploymentPipelineStack(app, `DeploymentPipeline-${env}`, {
    environment: env,
    env: {
      account: process.env.CDK_DEFAULT_ACCOUNT,
      region: process.env.CDK_DEFAULT_REGION ?? 'us-east-1',
    },
    tags: {
      Environment: env,
      Project: 'workflow-orchestration-patterns',
    },
  });
}
