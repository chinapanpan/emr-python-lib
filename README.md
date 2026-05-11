# EMR PySpark Dependency Management - Venv Approach

## Overview

This project demonstrates packaging Python 3.12 dependencies using **venv + venv-pack** for PySpark jobs on:

- **EMR Serverless**
- **EMR on EC2** (via spark-submit)

The key principle: create a portable Python virtual environment containing all dependencies (third-party + custom libs), pack it with `venv-pack`, and distribute it via `--archives` so Spark uses the venv's Python binary on all nodes.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Python 3.12 Venv Archive                         │
│              (pyspark_venv.tar.gz)                            │
│                                                               │
│  ┌────────────────┐  ┌──────────────┐  ┌──────────────────┐ │
│  │ Python 3.12    │  │  Third-party │  │ Custom Libraries │ │
│  │ Interpreter    │  │  Packages    │  │ (shared_libs/)   │ │
│  │ (bin/python)   │  │  (numpy,     │  │                  │ │
│  │                │  │   pandas,    │  │  - constants/    │ │
│  │ Copied, not    │  │   requests)  │  │  - models/       │ │
│  │ symlinked      │  │              │  │  - utils/        │ │
│  └────────────────┘  └──────────────┘  └──────────────────┘ │
└─────────────────────────────────────────────────────────────┘
          │                              │
          ▼                              ▼
  ┌───────────────┐           ┌───────────────────┐
  │ EMR Serverless │           │   EMR on EC2      │
  │ PYSPARK_PYTHON │           │   PYSPARK_PYTHON  │
  │ =./environment │           │   =./environment  │
  │   /bin/python  │           │     /bin/python   │
  └───────────────┘           └───────────────────┘
```

## Quick Start

### 1. Package Dependencies

```bash
python3.12 scripts/package_dependencies.py
```

This script:
1. Creates a Python 3.12 venv with `--copies` (no symlinks)
2. Installs third-party packages from `requirements.txt`
3. Copies `shared_libs/` into the venv's site-packages
4. Packs the venv with `venv-pack --python-prefix /home/hadoop/environment`
5. Uploads `pyspark_venv.tar.gz` and `main_job.py` to S3

### 2. Submit to EMR Serverless

```bash
python3.12 scripts/submit_emr_serverless.py
```

### 3. Submit to EMR on EC2

```bash
python3.12 scripts/submit_emr_on_ec2.py
```

## How It Works

### Spark Configuration

| Parameter | EMR Serverless | EMR on EC2 (YARN) |
|-----------|---------------|-------------------|
| Archive | `--conf spark.archives=s3://...#environment` | `--archives s3://...#environment` |
| Driver Python | `spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python` | `spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python` |
| Driver PySpark | `spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python` | `spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python` |
| LD_LIBRARY_PATH | `spark.emr-serverless.driverEnv.LD_LIBRARY_PATH=./environment/lib` | `spark.yarn.appMasterEnv.LD_LIBRARY_PATH=./environment/lib` |
| PYTHONHOME | Not required | `spark.yarn.appMasterEnv.PYTHONHOME=./environment` |
| Executor Python | `spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python` | Same |
| Executor LD_LIBRARY_PATH | `spark.executorEnv.LD_LIBRARY_PATH=./environment/lib` | Same |
| Executor PYTHONHOME | Not required | `spark.executorEnv.PYTHONHOME=./environment` |

### Why venv-pack?

1. **Full Python isolation**: Each job uses its own Python 3.12 binary, independent of EMR's system Python
2. **No sys.path hacks**: All packages are in the venv's site-packages, standard imports work
3. **Version control**: Pin exact Python + package versions across all environments
4. **Portability**: `--copies` + `--python-prefix` ensure the archive works on any node

### The `#environment` Alias

The `#environment` suffix in `spark.archives` determines the extraction directory name. This must match the path in `PYSPARK_PYTHON`:
- Archive extracts to `./environment/`
- Python binary is at `./environment/bin/python`
- Site-packages are at `./environment/lib/python3.12/site-packages/`

## Project Structure

```
emr/
├── shared_libs/                # Custom Python libraries
│   ├── constants/              # Configuration & type mappings
│   ├── models/                 # Data models
│   ├── core_data_common_utils/ # Data quality, Spark utilities
│   ├── core_data_source_utils/ # File utils, S3 data loader
│   └── utils/                  # Logging, date utilities
├── jobs/
│   └── main_job.py            # Main PySpark job (verification)
├── scripts/
│   ├── package_dependencies.py # Venv packaging script
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
| `AWS_REGION` | `ap-southeast-1` | AWS region |
| `EMR_RELEASE` | `emr-7.8.0` | EMR release label |
| `EMR_INSTANCE_TYPE` | `m5.xlarge` | EC2 instance type |
| `EMR_INSTANCE_COUNT` | `2` | Number of EC2 instances |

## Important Notes

1. **Build environment must match target**: Build the venv on Amazon Linux 2023 (for EMR 7.x) to ensure binary compatibility with C extensions (numpy, pandas).

2. **`--copies` flag is required**: The venv must use copied binaries, not symlinks, for portability.

3. **`--python-prefix`**: Set to `/home/hadoop/environment` so Python finds its libraries correctly when running on EMR nodes.

4. **libpython shared library**: The packaging script copies `libpython3.12.so` into the venv's `lib/` directory. `LD_LIBRARY_PATH=./environment/lib` must be set on all nodes.

5. **PYTHONHOME on EMR on EC2**: YARN cluster mode requires `PYTHONHOME=./environment` to correctly locate the standard library. EMR Serverless handles this automatically.

6. **stdlib is included**: The packaging script copies the Python 3.12 standard library into the venv for full portability. This ensures `encodings`, `distutils` (via setuptools), and other stdlib modules are available.

7. **Archive size**: The venv archive is ~60MB including Python interpreter + stdlib + all packages.

## Test Results

### EMR Serverless (emr-7.12.0) - PASSED

- Application: emr-poc-venv-7.12.0 (ap-southeast-1)
- Python version: 3.12.12
- Third-party libraries (numpy, pandas, requests): PASSED
- Custom shared libraries (shared_libs): PASSED
- Overall: ALL PASSED

### EMR on EC2 (emr-7.12.0) - PASSED

- Cluster: j-32OD6HO7PS4ZQ (emr-poc-ec2-venv)
- Step: s-00905193K815O4H9I8QT (COMPLETED)
- Deploy mode: cluster
- Python version: 3.12.12
