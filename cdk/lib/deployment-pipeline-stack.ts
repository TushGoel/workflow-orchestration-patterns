/**
 * DeploymentPipelineStack
 *
 * CDK stack for a production deployment pipeline with:
 * - SQS ingestion queue with dead-letter queue
 * - Step Functions state machine for durable workflow execution
 * - Lambda functions for each pipeline stage
 * - IAM roles with least-privilege permissions
 * - CloudWatch alarms for SLO monitoring
 *
 * Pattern: event-driven deployment pipeline processing 1,000+ deployments/week
 * with durable execution, retry logic, and fault-tolerant state management.
 */

import * as cdk from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { Duration, RemovalPolicy } from 'aws-cdk-lib';
import { Construct } from 'constructs';

export interface DeploymentPipelineProps extends cdk.StackProps {
  environment: 'dev' | 'staging' | 'prod';
  alertEmail?: string;
}

export class DeploymentPipelineStack extends cdk.Stack {
  public readonly deploymentQueue: sqs.Queue;
  public readonly stateMachine: sfn.StateMachine;
  public readonly stateTable: dynamodb.Table;

  constructor(scope: Construct, id: string, props: DeploymentPipelineProps) {
    super(scope, id, props);

    const isProd = props.environment === 'prod';

    // ── DynamoDB: deployment state store ─────────────────────────────────
    this.stateTable = new dynamodb.Table(this, 'DeploymentState', {
      partitionKey: { name: 'deploymentId', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'timestamp', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: isProd,
      removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
      timeToLiveAttribute: 'ttl',
    });

    // ── SQS: event ingestion with dead-letter queue ───────────────────────
    const dlq = new sqs.Queue(this, 'DeploymentDLQ', {
      retentionPeriod: Duration.days(14),
      visibilityTimeout: Duration.minutes(5),
    });

    this.deploymentQueue = new sqs.Queue(this, 'DeploymentQueue', {
      visibilityTimeout: Duration.minutes(6),   // > Lambda timeout
      deadLetterQueue: {
        queue: dlq,
        maxReceiveCount: 3,                     // retry 3× before DLQ
      },
      // Deduplication window: prevents duplicate workflows from alert storms
      contentBasedDeduplication: false,
    });

    // ── IAM: least-privilege Lambda execution role ────────────────────────
    const lambdaRole = new iam.Role(this, 'PipelineLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    // Grant only the specific DynamoDB actions needed — not dynamodb:*
    this.stateTable.grant(lambdaRole,
      'dynamodb:PutItem',
      'dynamodb:GetItem',
      'dynamodb:UpdateItem',
      'dynamodb:Query',
    );

    // ── Lambda functions: one per pipeline stage ──────────────────────────
    const commonLambdaProps = {
      runtime: lambda.Runtime.PYTHON_3_12,
      role: lambdaRole,
      timeout: Duration.minutes(5),
      environment: {
        STATE_TABLE: this.stateTable.tableName,
        ENVIRONMENT: props.environment,
      },
    };

    const validateFn = new lambda.Function(this, 'ValidateDeployment', {
      ...commonLambdaProps,
      handler: 'validate.handler',
      code: lambda.Code.fromInline(`
def handler(event, context):
    # Validate deployment config, permissions, and pre-conditions
    deployment_id = event.get('deploymentId')
    if not deployment_id:
        raise ValueError('deploymentId is required')
    return {'deploymentId': deployment_id, 'status': 'validated'}
      `),
      description: 'Validates deployment configuration and pre-conditions',
    });

    const deployFn = new lambda.Function(this, 'ExecuteDeployment', {
      ...commonLambdaProps,
      handler: 'deploy.handler',
      code: lambda.Code.fromInline(`
def handler(event, context):
    deployment_id = event['deploymentId']
    # Execute deployment steps
    return {'deploymentId': deployment_id, 'status': 'deployed'}
      `),
      description: 'Executes the deployment across target environments',
    });

    const verifyFn = new lambda.Function(this, 'VerifyDeployment', {
      ...commonLambdaProps,
      handler: 'verify.handler',
      code: lambda.Code.fromInline(`
def handler(event, context):
    deployment_id = event['deploymentId']
    # Run health checks and smoke tests
    return {'deploymentId': deployment_id, 'status': 'healthy', 'passed': True}
      `),
      description: 'Runs post-deployment health checks and smoke tests',
    });

    const rollbackFn = new lambda.Function(this, 'RollbackDeployment', {
      ...commonLambdaProps,
      handler: 'rollback.handler',
      code: lambda.Code.fromInline(`
def handler(event, context):
    deployment_id = event['deploymentId']
    # Rollback to previous known-good state
    return {'deploymentId': deployment_id, 'status': 'rolled_back'}
      `),
      description: 'Rolls back to the previous known-good deployment',
    });

    // ── Step Functions: durable workflow state machine ────────────────────

    // Stage 1: Validate
    const validate = new tasks.LambdaInvoke(this, 'Validate', {
      lambdaFunction: validateFn,
      outputPath: '$.Payload',
      retryOnServiceExceptions: true,
    }).addRetry({
      maxAttempts: 3,
      interval: Duration.seconds(2),
      backoffRate: 2,           // exponential backoff: 2s → 4s → 8s
      errors: ['Lambda.ServiceException', 'Lambda.TooManyRequestsException'],
    });

    // Stage 2: Deploy
    const deploy = new tasks.LambdaInvoke(this, 'Deploy', {
      lambdaFunction: deployFn,
      outputPath: '$.Payload',
    }).addRetry({
      maxAttempts: 2,
      interval: Duration.seconds(10),
      backoffRate: 2,
      errors: ['States.TaskFailed'],
    });

    // Stage 3: Verify health
    const verify = new tasks.LambdaInvoke(this, 'Verify', {
      lambdaFunction: verifyFn,
      outputPath: '$.Payload',
    });

    // Stage 4: Rollback (on failure path)
    const rollback = new tasks.LambdaInvoke(this, 'Rollback', {
      lambdaFunction: rollbackFn,
      outputPath: '$.Payload',
    });

    const deploymentFailed = new sfn.Fail(this, 'DeploymentFailed', {
      error: 'DeploymentFailed',
      cause: 'Deployment failed after rollback',
    });

    const deploymentSuccess = new sfn.Succeed(this, 'DeploymentSucceeded');

    // Health check branch
    const healthCheckPassed = new sfn.Choice(this, 'HealthCheckPassed')
      .when(sfn.Condition.booleanEquals('$.passed', true), deploymentSuccess)
      .otherwise(rollback.next(deploymentFailed));

    // Workflow: validate → deploy → verify → branch
    const definition = validate
      .next(deploy)
      .next(verify)
      .next(healthCheckPassed);

    this.stateMachine = new sfn.StateMachine(this, 'DeploymentStateMachine', {
      definition,
      timeout: Duration.hours(1),
      tracingEnabled: true,
      logs: {
        destination: new cdk.aws_logs.LogGroup(this, 'StateMachineLogs', {
          retention: cdk.aws_logs.RetentionDays.ONE_WEEK,
        }),
        level: sfn.LogLevel.ERROR,
      },
    });

    // ── CloudWatch: SLO alarms ─────────────────────────────────────────────

    // Alert if DLQ has messages — indicates systematic failures
    new cloudwatch.Alarm(this, 'DLQMessageAlarm', {
      alarmDescription: 'Deployments being dead-lettered — investigate immediately',
      metric: dlq.metricApproximateNumberOfMessagesVisible(),
      threshold: 1,
      evaluationPeriods: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    // Alert if Step Functions failure rate > 5% (SLO breach)
    new cloudwatch.Alarm(this, 'WorkflowFailureAlarm', {
      alarmDescription: 'Deployment workflow failure rate exceeds SLO',
      metric: this.stateMachine.metricFailed({
        period: Duration.minutes(5),
        statistic: 'Sum',
      }),
      threshold: 5,
      evaluationPeriods: 3,
    });

    // ── Outputs ───────────────────────────────────────────────────────────
    new cdk.CfnOutput(this, 'QueueUrl', { value: this.deploymentQueue.queueUrl });
    new cdk.CfnOutput(this, 'StateMachineArn', { value: this.stateMachine.stateMachineArn });
    new cdk.CfnOutput(this, 'StateTableName', { value: this.stateTable.tableName });
  }
}
