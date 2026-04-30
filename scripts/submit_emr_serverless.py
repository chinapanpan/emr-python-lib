"""
Submit PySpark job to EMR Serverless with unified dependency package.

The unified package (pyspark_deps_all.tar.gz) contains:
- Custom shared libraries (shared_libs/)
- Third-party Python packages (requests, etc.)

When extracted via --archives, all packages are importable through PYTHONPATH.
"""

import boto3
import time
import json
import os
from datetime import datetime

REGION = os.environ.get("AWS_REGION", "ap-southeast-1")
S3_BUCKET = os.environ.get("S3_BUCKET", "zpfsingapore")
S3_PREFIX = os.environ.get("S3_PREFIX", "emr/poc")
S3_LOGS = f"s3://{S3_BUCKET}/{S3_PREFIX}/logs/serverless"
RELEASE_LABEL = os.environ.get("EMR_RELEASE", "emr-7.12.0")

S3_MAIN_JOB = f"s3://{S3_BUCKET}/{S3_PREFIX}/jobs/main_job.py"
S3_DEPS_ARCHIVE = f"s3://{S3_BUCKET}/{S3_PREFIX}/libs/pyspark_deps_all.tar.gz"


def get_or_create_application(client):
    """Get existing application or create a new one."""
    app_name = f"emr-poc-serverless-{RELEASE_LABEL.replace('emr-', '')}"

    response = client.list_applications()
    for app in response["applications"]:
        if app["name"] == app_name and app["state"] not in ["TERMINATED"]:
            print(f"[OK] Found existing app: {app['id']} (state: {app['state']})")
            if app["state"] in ["STOPPED", "CREATED"]:
                client.start_application(applicationId=app["id"])
                _wait_for_app(client, app["id"], "STARTED")
            return app["id"]

    print(f"[..] Creating EMR Serverless app: {app_name} ({RELEASE_LABEL})")
    response = client.create_application(
        name=app_name,
        releaseLabel=RELEASE_LABEL,
        type="Spark",
        architecture="X86_64",
        autoStartConfiguration={"enabled": True},
        autoStopConfiguration={"enabled": True, "idleTimeoutMinutes": 5},
    )
    app_id = response["applicationId"]
    print(f"[OK] Created: {app_id}")
    client.start_application(applicationId=app_id)
    _wait_for_app(client, app_id, "STARTED")
    return app_id


def _wait_for_app(client, app_id, target, timeout=300):
    start = time.time()
    while time.time() - start < timeout:
        resp = client.get_application(applicationId=app_id)
        state = resp["application"]["state"]
        if state == target:
            return
        time.sleep(10)
    raise TimeoutError(f"App {app_id} did not reach {target}")


def get_execution_role():
    """Get or create the EMR Serverless execution role."""
    iam = boto3.client("iam", region_name=REGION)
    role_name = "EMRServerlessS3RuntimeRole"

    try:
        resp = iam.get_role(RoleName=role_name)
        return resp["Role"]["Arn"]
    except iam.exceptions.NoSuchEntityException:
        pass

    trust = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "emr-serverless.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    })
    print(f"[..] Creating role: {role_name}")
    resp = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust)
    iam.attach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess")
    iam.attach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/CloudWatchLogsFullAccess")
    time.sleep(15)
    return resp["Role"]["Arn"]


def submit_job(client, app_id, role_arn):
    """Submit PySpark job with unified dependency archive."""
    job_name = f"poc-unified-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    spark_params = " ".join([
        f"--archives {S3_DEPS_ARCHIVE}#deps",
        "--conf spark.emr-serverless.driverEnv.PYTHONPATH=./deps",
        "--conf spark.executorEnv.PYTHONPATH=./deps",
    ])

    print(f"[..] Submitting: {job_name}")
    print(f"     Entry: {S3_MAIN_JOB}")
    print(f"     Deps:  {S3_DEPS_ARCHIVE}")
    print(f"     Params: {spark_params}")

    response = client.start_job_run(
        applicationId=app_id,
        executionRoleArn=role_arn,
        name=job_name,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": S3_MAIN_JOB,
                "sparkSubmitParameters": spark_params,
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": S3_LOGS}
            }
        },
    )
    job_run_id = response["jobRunId"]
    print(f"[OK] Submitted: {job_run_id}")
    return job_run_id


def wait_for_job(client, app_id, job_run_id, timeout=900):
    """Wait for job completion."""
    print(f"[..] Waiting for job (timeout: {timeout}s)...")
    start = time.time()
    while time.time() - start < timeout:
        resp = client.get_job_run(applicationId=app_id, jobRunId=job_run_id)
        state = resp["jobRun"]["state"]
        elapsed = int(time.time() - start)
        print(f"     [{elapsed}s] {state}")
        if state == "SUCCESS":
            return resp["jobRun"]
        elif state in ["FAILED", "CANCELLED"]:
            print(f"     Details: {resp['jobRun'].get('stateDetails', '')}")
            return resp["jobRun"]
        time.sleep(15)
    raise TimeoutError(f"Job did not complete within {timeout}s")


def main():
    print("=" * 60)
    print("EMR Serverless - Unified Dependency Package Submission")
    print(f"  Release: {RELEASE_LABEL}")
    print(f"  Region:  {REGION}")
    print("=" * 60)

    client = boto3.client("emr-serverless", region_name=REGION)
    app_id = get_or_create_application(client)
    role_arn = get_execution_role()
    job_run_id = submit_job(client, app_id, role_arn)
    result = wait_for_job(client, app_id, job_run_id)

    print("\n" + "=" * 60)
    print(f"  Result: {result['state']}")
    print(f"  Logs:   {S3_LOGS}/applications/{app_id}/jobs/{job_run_id}/")
    print("=" * 60)
    return result["state"] == "SUCCESS"


if __name__ == "__main__":
    exit(0 if main() else 1)
