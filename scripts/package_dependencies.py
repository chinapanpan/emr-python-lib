"""
Package ALL dependencies (custom + third-party) into a SINGLE unified archive.

Output:
  - pyspark_deps_all.tar.gz: Contains both custom shared_libs AND third-party packages
    When extracted via --archives, all packages are importable via PYTHONPATH

This single-archive approach ensures:
  1. Unified dependency management
  2. Consistent behavior across EMR Serverless and EMR on EC2
  3. Simple deployment: one archive to upload, one parameter to set

Usage (both platforms):
  spark-submit --archives s3://bucket/pyspark_deps_all.tar.gz#deps \
    --conf spark.emr-serverless.driverEnv.PYTHONPATH=./deps \
    --conf spark.executorEnv.PYTHONPATH=./deps \
    s3://bucket/main_job.py
"""

import os
import sys
import subprocess
import shutil
import tarfile
import zipfile
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
SHARED_LIBS_DIR = BASE_DIR / "shared_libs"
JOBS_DIR = BASE_DIR / "jobs"
BUILD_DIR = BASE_DIR / "build"
S3_BUCKET = os.environ.get("S3_BUCKET", "zpfsingapore")
S3_PREFIX = os.environ.get("S3_PREFIX", "emr/poc")
EMR_PYTHON_VERSION = os.environ.get("EMR_PYTHON_VERSION", "39")
PLATFORM = os.environ.get("TARGET_PLATFORM", "manylinux2014_x86_64")

THIRD_PARTY_PACKAGES = [
    "requests",
]


def clean_build():
    """Clean previous build artifacts."""
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    BUILD_DIR.mkdir(parents=True)
    print(f"[OK] Cleaned build directory: {BUILD_DIR}")


def download_third_party_wheels():
    """Download platform-specific wheels for third-party packages."""
    wheels_dir = BUILD_DIR / "wheels"
    wheels_dir.mkdir(exist_ok=True)

    print(f"[..] Downloading wheels for Python {EMR_PYTHON_VERSION}, platform {PLATFORM}")
    print(f"     Packages: {THIRD_PARTY_PACKAGES}")

    cmd = [
        sys.executable, "-m", "pip", "download",
        "--python-version", EMR_PYTHON_VERSION,
        "--platform", PLATFORM,
        "--only-binary=:all:",
        "--no-deps",
        "-d", str(wheels_dir),
    ] + THIRD_PARTY_PACKAGES

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        # Some packages are pure-python, retry without platform constraint
        cmd_pure = [
            sys.executable, "-m", "pip", "download",
            "--no-deps",
            "-d", str(wheels_dir),
        ] + THIRD_PARTY_PACKAGES
        subprocess.run(cmd_pure, check=True)

    # Also download dependencies
    deps = ["charset_normalizer", "idna", "urllib3", "certifi"]
    for dep in deps:
        cmd = [
            sys.executable, "-m", "pip", "download",
            "--python-version", EMR_PYTHON_VERSION,
            "--platform", PLATFORM,
            "--only-binary=:all:",
            "--no-deps",
            "-d", str(wheels_dir),
            dep,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            # Pure python fallback
            cmd2 = [sys.executable, "-m", "pip", "download", "--no-deps", "-d", str(wheels_dir), dep]
            subprocess.run(cmd2, capture_output=True)

    print(f"[OK] Downloaded wheels:")
    for whl in wheels_dir.glob("*.whl"):
        print(f"     - {whl.name}")

    return wheels_dir


def build_unified_package(wheels_dir):
    """Build a single unified archive containing custom code + third-party packages."""
    package_dir = BUILD_DIR / "unified_package"
    package_dir.mkdir(exist_ok=True)

    # Step 1: Copy custom shared_libs
    print(f"[..] Adding custom libraries from {SHARED_LIBS_DIR}")
    dest_shared = package_dir / "shared_libs"
    shutil.copytree(SHARED_LIBS_DIR, dest_shared, ignore=shutil.ignore_patterns("__pycache__"))

    # Step 2: Extract third-party wheels into the same directory
    print(f"[..] Adding third-party packages from wheels")
    for whl in wheels_dir.glob("*.whl"):
        with zipfile.ZipFile(whl, "r") as zf:
            for member in zf.namelist():
                # Skip .dist-info directories
                if ".dist-info" in member:
                    continue
                zf.extract(member, package_dir)
        print(f"     + {whl.stem.split('-')[0]}")

    # Step 3: Create tar.gz archive
    archive_path = BUILD_DIR / "pyspark_deps_all.tar.gz"
    print(f"[..] Creating unified archive: {archive_path}")
    with tarfile.open(archive_path, "w:gz") as tar:
        for item in package_dir.iterdir():
            tar.add(item, arcname=item.name)

    print(f"[OK] Unified archive created: {archive_path}")
    print(f"     Size: {archive_path.stat().st_size / (1024*1024):.1f} MB")

    # List top-level contents
    print("     Contents (top-level):")
    for item in sorted(package_dir.iterdir()):
        if item.is_dir():
            print(f"       [DIR] {item.name}/")
        else:
            print(f"       [FILE] {item.name}")

    return archive_path


def upload_to_s3(archive_path):
    """Upload artifacts to S3."""
    import boto3
    s3 = boto3.client("s3", region_name="ap-southeast-1")

    artifacts = [
        (archive_path, f"{S3_PREFIX}/libs/pyspark_deps_all.tar.gz"),
        (JOBS_DIR / "main_job.py", f"{S3_PREFIX}/jobs/main_job.py"),
    ]

    for local_path, s3_key in artifacts:
        if local_path.exists():
            print(f"[..] Uploading {local_path.name} -> s3://{S3_BUCKET}/{s3_key}")
            s3.upload_file(str(local_path), S3_BUCKET, s3_key)
            print(f"[OK] Uploaded s3://{S3_BUCKET}/{s3_key}")
        else:
            print(f"[WARN] File not found: {local_path}")

    print(f"\n[OK] All artifacts uploaded to S3:")
    print(f"     s3://{S3_BUCKET}/{S3_PREFIX}/libs/pyspark_deps_all.tar.gz")
    print(f"     s3://{S3_BUCKET}/{S3_PREFIX}/jobs/main_job.py")


def main():
    print("=" * 60)
    print("EMR PySpark Unified Dependency Packaging")
    print("=" * 60)
    print(f"  Target Python: cpython-{EMR_PYTHON_VERSION}")
    print(f"  Target Platform: {PLATFORM}")
    print(f"  S3 Destination: s3://{S3_BUCKET}/{S3_PREFIX}/")
    print("=" * 60)

    clean_build()
    wheels_dir = download_third_party_wheels()
    archive_path = build_unified_package(wheels_dir)
    upload_to_s3(archive_path)

    print("\n" + "=" * 60)
    print("PACKAGING COMPLETE")
    print("=" * 60)
    print("\nUsage (EMR Serverless):")
    print(f"  --archives s3://{S3_BUCKET}/{S3_PREFIX}/libs/pyspark_deps_all.tar.gz#deps")
    print("  --conf spark.emr-serverless.driverEnv.PYTHONPATH=./deps")
    print("  --conf spark.executorEnv.PYTHONPATH=./deps")
    print("\nUsage (EMR on EC2, spark-submit):")
    print(f"  --archives s3://{S3_BUCKET}/{S3_PREFIX}/libs/pyspark_deps_all.tar.gz#deps")
    print("  --conf spark.yarn.appMasterEnv.PYTHONPATH=./deps")
    print("  --conf spark.executorEnv.PYTHONPATH=./deps")


if __name__ == "__main__":
    main()
