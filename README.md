# EMR PySpark Dependency Management POC

## Overview

This project demonstrates a **unified approach** for managing Python dependencies (both third-party libraries and custom code) when submitting PySpark jobs to:

- **EMR Serverless**
- **EMR on EC2** (via spark-submit)

The key principle: **package all dependencies into a single archive**, use `--archives` to distribute it, and `PYTHONPATH` to make packages importable.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Unified Dependency Package               │
│              (pyspark_deps_all.tar.gz)                    │
│                                                           │
│  ┌─────────────────────┐  ┌───────────────────────────┐ │
│  │  Custom Libraries    │  │  Third-party Packages      │ │
│  │  (shared_libs/)      │  │  (requests, certifi, etc.) │ │
│  │                      │  │                            │ │
│  │  - constants/        │  │  Packaged as platform-     │ │
│  │  - models/           │  │  specific wheels for the   │ │
│  │  - core_data_*_utils/│  │  target EMR Python version │ │
│  │  - utils/            │  │                            │ │
│  └─────────────────────┘  └───────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
          │                              │
          ▼                              ▼
  ┌───────────────┐           ┌───────────────────┐
  │ EMR Serverless │           │   EMR on EC2      │
  │ --archives     │           │   --archives      │
  │ PYTHONPATH     │           │   PYTHONPATH      │
  └───────────────┘           └───────────────────┘
```

## Quick Start

### 1. Package Dependencies

```bash
python3.12 scripts/package_dependencies.py
```

This script:
1. Downloads third-party packages as platform-compatible wheels (targeting EMR's Python 3.9)
2. Copies custom `shared_libs/` code
3. Combines everything into one `pyspark_deps_all.tar.gz`
4. Uploads to S3

### 2. Submit to EMR Serverless

```bash
python3.12 scripts/submit_emr_serverless.py
```

### 3. Submit to EMR on EC2

```bash
python3.12 scripts/submit_emr_on_ec2.py
```

## How It Works

### Submission Parameters (Consistent Across Both Platforms)

| Parameter | EMR Serverless | EMR on EC2 (YARN) |
|-----------|---------------|-------------------|
| Archive | `--archives s3://bucket/pyspark_deps_all.tar.gz#deps` | Same |
| Driver PYTHONPATH | `--conf spark.emr-serverless.driverEnv.PYTHONPATH=./deps` | `--conf spark.yarn.appMasterEnv.PYTHONPATH=./deps` |
| Executor PYTHONPATH | `--conf spark.executorEnv.PYTHONPATH=./deps` | Same |
| Entry point | `s3://bucket/jobs/main_job.py` | Same |

### The Archive Contents

```
pyspark_deps_all.tar.gz
├── shared_libs/           # Custom Python libraries
│   ├── __init__.py
│   ├── constants/
│   ├── models/
│   ├── core_data_common_utils/
│   ├── core_data_source_utils/
│   └── utils/
├── requests/              # Third-party: requests
├── certifi/               # Third-party: certifi  
├── charset_normalizer/    # Third-party: charset_normalizer
├── idna/                  # Third-party: idna
└── urllib3/               # Third-party: urllib3
```

### How Spark Uses It

1. Spark downloads the archive from S3 to each node
2. Extracts it as `./deps/` (the alias after `#`)
3. `PYTHONPATH=./deps` lets Python find all packages inside
4. Both custom (`shared_libs`) and third-party packages are importable

## Project Structure

```
emr/
├── shared_libs/                # Custom Python libraries
│   ├── constants/              # Configuration & type mappings
│   ├── models/                 # Data models (BaseExternalDataSource, etc.)
│   ├── core_data_common_utils/ # Data quality, Spark utilities
│   ├── core_data_source_utils/ # File utils, S3 data loader
│   └── utils/                  # Logging, date utilities
├── jobs/
│   └── main_job.py            # Main PySpark job (demo)
├── scripts/
│   ├── package_dependencies.py # Packaging script
│   ├── submit_emr_serverless.py # EMR Serverless submission
│   └── submit_emr_on_ec2.py   # EMR on EC2 submission
├── requirements.txt            # Third-party dependencies
└── README.md
```

## Configuration (Environment Variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_BUCKET` | `zpfsingapore` | S3 bucket for artifacts |
| `S3_PREFIX` | `emr/poc` | S3 prefix path |
| `EMR_REGION` | `ap-southeast-1` | AWS region for EMR (avoids AWS_REGION conflict) |
| `EMR_RELEASE` | `emr-7.12.0` | EMR release label |
| `EMR_PYTHON_VERSION` | `39` | Target Python version (cpython) |
| `TARGET_PLATFORM` | `manylinux2014_x86_64` | Target platform for wheels |

## Constraints & Notes

### Important Constraints

1. **Python Version Matching**: The third-party packages MUST be built for the same Python version as the EMR runtime. EMR 6.x/7.x uses Python 3.9. Use `--python-version 39` when downloading wheels.

2. **Platform Matching**: Packages with C extensions (numpy, pandas) need platform-specific wheels. Use `--platform manylinux2014_x86_64` for x86 EMR instances.

3. **Pre-installed Packages**: EMR already includes numpy, pandas, boto3, pyarrow. You only need to package libraries NOT pre-installed on EMR. Check with a diagnostic job first.

4. **Archive Size**: Keep the archive reasonable (<200MB). Large archives increase job startup time.

5. **No Python Binary**: Do NOT include a Python interpreter in the archive. Use EMR's built-in Python. Packaging a custom Python binary creates shared library dependency issues.

### EMR Pre-installed Packages (7.12.0)

- numpy 2.0.2
- pandas 2.3.3
- boto3 1.42.27
- pyarrow 21.0.0

### Cross-Account / Cross-Region Reuse

To use this solution in a different AWS account or region:

1. Set environment variables:
   ```bash
   export S3_BUCKET=your-bucket
   export S3_PREFIX=your/prefix
   export EMR_REGION=your-region
   ```

2. Ensure IAM roles exist:
   - EMR Serverless: `EMRServerlessS3RuntimeRole` with S3 access
   - EMR on EC2: `EMR_DefaultRole` (service) + `EMR_EC2_DefaultRole` (instance profile)

3. Run the packaging and submission scripts.

## Test Results

### EMR Serverless (emr-7.12.0) - PASSED

- Re-verified from fresh git clone
- Third-party libraries (numpy, pandas, requests): PASSED
- Custom shared libraries (shared_libs): PASSED
- Full output: `test_results/emr_serverless_output.log`

### EMR on EC2 (emr-6.15.0) - Ready (infrastructure-dependent)

- The submission script (`scripts/submit_emr_on_ec2.py`) uses the same `--archives` + `PYTHONPATH` mechanism
- Only difference from Serverless: `spark.yarn.appMasterEnv.PYTHONPATH` instead of `spark.emr-serverless.driverEnv.PYTHONPATH`
- Prerequisite: VPC must have internet connectivity for EMR bootstrap (download EMR software packages)
- If cluster creation fails with "Time out occurred during bootstrap", check security groups and internet gateway

## Standardized Dependency Packaging Flow

```
Step 1: Develop custom Python libraries
         └── shared_libs/ (your custom code)

Step 2: Define third-party dependencies
         └── requirements.txt (packages not pre-installed on EMR)

Step 3: Run packaging script
         └── python3.12 scripts/package_dependencies.py
             ├── Downloads platform-specific wheels (Python 3.9, manylinux2014_x86_64)
             ├── Extracts wheels into unified directory
             ├── Copies custom shared_libs into same directory
             └── Creates single tar.gz archive

Step 4: Upload archive + job script to S3
         └── s3://bucket/prefix/libs/pyspark_deps_all.tar.gz
             s3://bucket/prefix/jobs/main_job.py

Step 5: Submit job
         EMR Serverless:  python3.12 scripts/submit_emr_serverless.py
         EMR on EC2:      python3.12 scripts/submit_emr_on_ec2.py
```

### Spark Submit Parameters (Core Pattern)

```bash
spark-submit \
  --archives s3://bucket/libs/pyspark_deps_all.tar.gz#deps \
  --conf spark.<platform-specific>.PYTHONPATH=./deps \
  --conf spark.executorEnv.PYTHONPATH=./deps \
  s3://bucket/jobs/main_job.py
```

| Platform | Driver PYTHONPATH Config |
|----------|------------------------|
| EMR Serverless | `spark.emr-serverless.driverEnv.PYTHONPATH=./deps` |
| EMR on EC2 (YARN cluster mode) | `spark.yarn.appMasterEnv.PYTHONPATH=./deps` |
