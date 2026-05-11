"""
Package Python dependencies using venv + venv-pack for EMR.

Creates a portable Python 3.12 virtual environment containing:
  - Third-party packages (requests, numpy, pandas)
  - Custom shared libraries (shared_libs/)

The venv archive is distributed via --archives and used as the Python interpreter
on both EMR Serverless and EMR on EC2 nodes.

Reference: https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/using-python.html

Usage:
  python3.12 scripts/package_dependencies.py
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
SHARED_LIBS_DIR = BASE_DIR / "shared_libs"
JOBS_DIR = BASE_DIR / "jobs"
BUILD_DIR = BASE_DIR / "build"
REQUIREMENTS_FILE = BASE_DIR / "requirements.txt"
S3_BUCKET = os.environ.get("S3_BUCKET", "zpfsingapore")
S3_PREFIX = os.environ.get("S3_PREFIX", "emr/poc")
VENV_NAME = "pyspark_venv"
VENV_ARCHIVE = f"{VENV_NAME}.tar.gz"
PYTHON_PREFIX = "/home/hadoop/environment"


def clean_build():
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    BUILD_DIR.mkdir(parents=True)
    print(f"[OK] Cleaned build directory: {BUILD_DIR}")


def create_venv():
    """Create a Python 3.12 virtual environment with --copies for portability."""
    venv_dir = BUILD_DIR / VENV_NAME
    print(f"[..] Creating Python 3.12 venv: {venv_dir}")

    subprocess.run(
        [sys.executable, "-m", "venv", str(venv_dir), "--copies"],
        check=True,
    )
    print(f"[OK] Venv created with --copies (no symlinks)")
    return venv_dir


def install_dependencies(venv_dir):
    """Install third-party packages and custom shared_libs into the venv."""
    pip_bin = venv_dir / "bin" / "pip"

    # Upgrade pip first
    print("[..] Upgrading pip in venv")
    subprocess.run([str(pip_bin), "install", "--upgrade", "pip"], check=True)

    # Install third-party packages from requirements.txt
    print(f"[..] Installing third-party packages from {REQUIREMENTS_FILE}")
    subprocess.run(
        [str(pip_bin), "install", "-r", str(REQUIREMENTS_FILE)],
        check=True,
    )

    # Install venv-pack
    print("[..] Installing venv-pack")
    subprocess.run([str(pip_bin), "install", "venv-pack"], check=True)

    # Install custom shared_libs as a package into the venv's site-packages
    site_packages = _get_site_packages(venv_dir)
    dest = site_packages / "shared_libs"
    print(f"[..] Copying shared_libs -> {dest}")
    shutil.copytree(
        SHARED_LIBS_DIR, dest,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )

    # Copy libpython shared library into the venv's lib directory
    _copy_libpython(venv_dir)

    print("[OK] All dependencies installed")
    _list_installed(venv_dir)


def _copy_libpython(venv_dir):
    """Copy libpython*.so and stdlib into the venv for full portability."""
    import glob

    # Copy shared library
    lib_dir = venv_dir / "lib"
    lib_dir.mkdir(exist_ok=True)

    patterns = [
        "/usr/lib64/libpython3.12*.so*",
        "/usr/lib/libpython3.12*.so*",
        "/usr/local/lib/libpython3.12*.so*",
    ]
    for pattern in patterns:
        for src in glob.glob(pattern):
            dest = lib_dir / Path(src).name
            shutil.copy2(src, dest)
            print(f"     Copied shared lib: {src} -> {dest}")

    # Copy Python stdlib into the venv's lib/python3.12 directory
    # This follows the AWS docs approach: "cp -r /usr/local/lib/python3.X/* ./venv/lib/python3.X/"
    stdlib_sources = [
        Path("/usr/lib64/python3.12"),
        Path("/usr/lib/python3.12"),
        Path("/usr/local/lib/python3.12"),
    ]
    venv_lib_python = venv_dir / "lib64" / "python3.12"

    for stdlib_src in stdlib_sources:
        if stdlib_src.exists():
            print(f"     Copying stdlib from {stdlib_src} -> {venv_lib_python}")
            for item in stdlib_src.iterdir():
                dest = venv_lib_python / item.name
                if dest.exists():
                    continue
                if item.is_dir():
                    shutil.copytree(item, dest, ignore=shutil.ignore_patterns("__pycache__"))
                else:
                    shutil.copy2(item, dest)
            print(f"     [OK] Stdlib copied from {stdlib_src}")
            break


def _get_site_packages(venv_dir):
    python_bin = venv_dir / "bin" / "python"
    result = subprocess.run(
        [str(python_bin), "-c",
         "import site; print(site.getsitepackages()[0])"],
        capture_output=True, text=True, check=True,
    )
    return Path(result.stdout.strip())


def _list_installed(venv_dir):
    pip_bin = venv_dir / "bin" / "pip"
    result = subprocess.run(
        [str(pip_bin), "list", "--format=columns"],
        capture_output=True, text=True,
    )
    print("     Installed packages:")
    for line in result.stdout.strip().split("\n"):
        print(f"       {line}")


def pack_venv(venv_dir):
    """Pack the venv into a portable archive using venv-pack."""
    archive_path = BUILD_DIR / VENV_ARCHIVE
    python_bin = venv_dir / "bin" / "python"

    print(f"[..] Packing venv with --python-prefix={PYTHON_PREFIX}")
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = str(venv_dir)
    subprocess.run(
        [str(python_bin), "-m", "venv_pack",
         "-f",
         "-o", str(archive_path),
         "--python-prefix", PYTHON_PREFIX],
        env=env,
        check=True,
    )

    size_mb = archive_path.stat().st_size / (1024 * 1024)
    print(f"[OK] Venv archive created: {archive_path} ({size_mb:.1f} MB)")
    return archive_path


def upload_to_s3(archive_path):
    """Upload venv archive and job script to S3."""
    import boto3
    s3 = boto3.client("s3", region_name="ap-southeast-1")

    artifacts = [
        (archive_path, f"{S3_PREFIX}/libs/{VENV_ARCHIVE}"),
        (JOBS_DIR / "main_job.py", f"{S3_PREFIX}/jobs/main_job.py"),
    ]

    for local_path, s3_key in artifacts:
        if local_path.exists():
            print(f"[..] Uploading {local_path.name} -> s3://{S3_BUCKET}/{s3_key}")
            s3.upload_file(str(local_path), S3_BUCKET, s3_key)
            print(f"[OK] Uploaded s3://{S3_BUCKET}/{s3_key}")
        else:
            print(f"[WARN] Not found: {local_path}")

    print(f"\n[OK] All artifacts uploaded to S3")


def main():
    print("=" * 60)
    print("EMR PySpark Venv Packaging (Python 3.12)")
    print("=" * 60)
    print(f"  Python:      {sys.version}")
    print(f"  Venv Name:   {VENV_NAME}")
    print(f"  Prefix:      {PYTHON_PREFIX}")
    print(f"  S3:          s3://{S3_BUCKET}/{S3_PREFIX}/")
    print("=" * 60)

    clean_build()
    venv_dir = create_venv()
    install_dependencies(venv_dir)
    archive_path = pack_venv(venv_dir)
    upload_to_s3(archive_path)

    print("\n" + "=" * 60)
    print("PACKAGING COMPLETE")
    print("=" * 60)
    print("\nUsage (EMR Serverless):")
    print(f"  --conf spark.archives=s3://{S3_BUCKET}/{S3_PREFIX}/libs/{VENV_ARCHIVE}#environment")
    print("  --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python")
    print("  --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python")
    print("  --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python")
    print("\nUsage (EMR on EC2):")
    print(f"  --archives s3://{S3_BUCKET}/{S3_PREFIX}/libs/{VENV_ARCHIVE}#environment")
    print("  --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python")
    print("  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python")
    print("  --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python")


if __name__ == "__main__":
    main()
