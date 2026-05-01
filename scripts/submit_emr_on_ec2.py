"""
通过 spark-submit step 提交 PySpark 任务到 EMR on EC2。

使用与 EMR Serverless 相同的统一依赖归档。
唯一区别是 PYTHONPATH 配置项：
- EMR Serverless: spark.emr-serverless.driverEnv.PYTHONPATH
- EMR on EC2:     spark.yarn.appMasterEnv.PYTHONPATH
"""

import boto3
import time
import json
import os
from datetime import datetime

REGION = os.environ.get("EMR_REGION", "ap-southeast-1")
S3_BUCKET = os.environ.get("S3_BUCKET", "zpfsingapore")
S3_PREFIX = os.environ.get("S3_PREFIX", "emr/poc")
S3_LOGS = f"s3://{S3_BUCKET}/{S3_PREFIX}/logs/ec2"
RELEASE_LABEL = os.environ.get("EMR_RELEASE", "emr-7.12.0")

S3_MAIN_JOB = f"s3://{S3_BUCKET}/{S3_PREFIX}/jobs/main_job.py"
S3_DEPS_ARCHIVE = f"s3://{S3_BUCKET}/{S3_PREFIX}/libs/pyspark_deps_all.tar.gz"

CLUSTER_NAME = "emr-poc-ec2"
INSTANCE_TYPE = os.environ.get("EMR_INSTANCE_TYPE", "m5.xlarge")
INSTANCE_COUNT = int(os.environ.get("EMR_INSTANCE_COUNT", "2"))


def get_or_create_cluster(client):
    """获取已有运行中的集群或创建新集群。"""
    resp = client.list_clusters(ClusterStates=["WAITING", "RUNNING"])
    for c in resp.get("Clusters", []):
        if c["Name"] == CLUSTER_NAME:
            print(f"[OK] Found cluster: {c['Id']} ({c['Status']['State']})")
            return c["Id"]

    resp = client.list_clusters(ClusterStates=["STARTING", "BOOTSTRAPPING"])
    for c in resp.get("Clusters", []):
        if c["Name"] == CLUSTER_NAME:
            print(f"[OK] Cluster starting: {c['Id']}")
            _wait_for_cluster(client, c["Id"])
            return c["Id"]

    ec2 = boto3.client("ec2", region_name=REGION)
    subnets = ec2.describe_subnets(Filters=[{"Name": "default-for-az", "Values": ["true"]}])
    subnet_ids = [s["SubnetId"] for s in subnets["Subnets"]]
    subnet_id = os.environ.get("EMR_SUBNET_ID", subnet_ids[0])
    print(f"[..] Using subnet: {subnet_id}")

    print(f"[..] Creating cluster: {CLUSTER_NAME} ({RELEASE_LABEL})")
    resp = client.run_job_flow(
        Name=CLUSTER_NAME,
        ReleaseLabel=RELEASE_LABEL,
        Applications=[{"Name": "Spark"}, {"Name": "Hadoop"}],
        Instances={
            "MasterInstanceType": INSTANCE_TYPE,
            "SlaveInstanceType": INSTANCE_TYPE,
            "InstanceCount": INSTANCE_COUNT,
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
            "Ec2SubnetId": subnet_id,
        },
        ServiceRole="EMR_DefaultRole",
        JobFlowRole="EMR_EC2_DefaultRole",
        LogUri=S3_LOGS,
        VisibleToAllUsers=True,
    )
    cluster_id = resp["JobFlowId"]
    print(f"[OK] Created: {cluster_id}")
    _wait_for_cluster(client, cluster_id)
    return cluster_id


def _wait_for_cluster(client, cluster_id, timeout=900):
    print(f"[..] Waiting for cluster {cluster_id}...")
    start = time.time()
    while time.time() - start < timeout:
        resp = client.describe_cluster(ClusterId=cluster_id)
        state = resp["Cluster"]["Status"]["State"]
        print(f"     [{int(time.time()-start)}s] {state}")
        if state == "WAITING":
            return
        elif state in ["TERMINATED", "TERMINATED_WITH_ERRORS"]:
            reason = resp["Cluster"]["Status"].get("StateChangeReason", {})
            raise RuntimeError(f"Cluster failed: {reason}")
        time.sleep(30)
    raise TimeoutError(f"Cluster not ready within {timeout}s")


def submit_step(client, cluster_id):
    """使用统一依赖归档提交 spark-submit step。"""
    step_name = f"poc-unified-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    # spark-submit 参数 - 相同归档，不同环境变量配置
    args = [
        "spark-submit",
        "--deploy-mode", "cluster",
        "--archives", f"{S3_DEPS_ARCHIVE}#deps",
        "--conf", "spark.yarn.appMasterEnv.PYTHONPATH=./deps",
        "--conf", "spark.executorEnv.PYTHONPATH=./deps",
        S3_MAIN_JOB,
    ]

    print(f"[..] Submitting step: {step_name}")
    print(f"     Command: {' '.join(args)}")

    resp = client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[{
            "Name": step_name,
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": args,
            },
        }],
    )
    step_id = resp["StepIds"][0]
    print(f"[OK] Step submitted: {step_id}")
    return step_id


def wait_for_step(client, cluster_id, step_id, timeout=900):
    """等待 step 完成。"""
    print(f"[..] Waiting for step (timeout: {timeout}s)...")
    start = time.time()
    while time.time() - start < timeout:
        resp = client.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = resp["Step"]["Status"]["State"]
        elapsed = int(time.time() - start)
        print(f"     [{elapsed}s] {state}")
        if state == "COMPLETED":
            return resp["Step"]
        elif state in ["FAILED", "CANCELLED"]:
            details = resp["Step"]["Status"].get("FailureDetails", {})
            print(f"     Failed: {details}")
            return resp["Step"]
        time.sleep(15)
    raise TimeoutError(f"Step not complete within {timeout}s")


def main():
    print("=" * 60)
    print("EMR on EC2 - Unified Dependency Package (spark-submit)")
    print(f"  Release: {RELEASE_LABEL}")
    print(f"  Region:  {REGION}")
    print("=" * 60)

    client = boto3.client("emr", region_name=REGION)
    cluster_id = get_or_create_cluster(client)
    step_id = submit_step(client, cluster_id)
    result = wait_for_step(client, cluster_id, step_id)

    state = result["Status"]["State"]
    print("\n" + "=" * 60)
    print(f"  Result: {state}")
    print(f"  Logs:   {S3_LOGS}/{cluster_id}/steps/{step_id}/")
    print("=" * 60)
    return state == "COMPLETED"


if __name__ == "__main__":
    exit(0 if main() else 1)
