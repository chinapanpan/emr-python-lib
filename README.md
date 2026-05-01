# EMR PySpark 依赖包管理方案 (POC)

## 概述

本项目演示了在提交 PySpark 任务到以下平台时，**统一管理 Python 依赖包**（第三方库 + 自定义类库）的标准化方案：

- **EMR Serverless**
- **EMR on EC2**（通过 spark-submit）

核心原则：**将所有依赖打包为单一归档文件**，使用 `--archives` 分发到所有节点，通过 `PYTHONPATH` 使包可导入。

## 架构

```
┌─────────────────────────────────────────────────────────┐
│              统一依赖包 (pyspark_deps_all.tar.gz)         │
│                                                           │
│  ┌─────────────────────┐  ┌───────────────────────────┐ │
│  │  自定义类库           │  │  第三方 Python 包          │ │
│  │  (shared_libs/)      │  │  (requests, certifi 等)   │ │
│  │                      │  │                            │ │
│  │  - constants/        │  │  以平台兼容的 wheel 形式   │ │
│  │  - models/           │  │  针对 EMR 目标 Python      │ │
│  │  - core_data_*_utils/│  │  版本进行下载              │ │
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

## 快速开始

### 1. 打包依赖

```bash
python3.12 scripts/package_dependencies.py
```

该脚本执行以下操作：
1. 下载平台兼容的第三方包 wheel（针对 EMR 的 Python 3.9）
2. 复制自定义 `shared_libs/` 代码
3. 合并为单一 `pyspark_deps_all.tar.gz` 归档
4. 上传至 S3

### 2. 提交到 EMR Serverless

```bash
python3.12 scripts/submit_emr_serverless.py
```

### 3. 提交到 EMR on EC2

```bash
python3.12 scripts/submit_emr_on_ec2.py
```

## 工作原理

### 提交参数对比（两平台保持一致）

| 参数 | EMR Serverless | EMR on EC2 (YARN) |
|------|---------------|-------------------|
| 归档文件 | `--archives s3://bucket/pyspark_deps_all.tar.gz#deps` | 相同 |
| Driver PYTHONPATH | `--conf spark.emr-serverless.driverEnv.PYTHONPATH=./deps` | `--conf spark.yarn.appMasterEnv.PYTHONPATH=./deps` |
| Executor PYTHONPATH | `--conf spark.executorEnv.PYTHONPATH=./deps` | 相同 |
| 入口脚本 | `s3://bucket/jobs/main_job.py` | 相同 |

### 归档文件内容

```
pyspark_deps_all.tar.gz
├── shared_libs/           # 自定义 Python 类库
│   ├── __init__.py
│   ├── constants/         # 配置常量、类型映射
│   ├── models/            # 数据模型（BaseExternalDataSource 等）
│   ├── core_data_common_utils/  # 数据质量检查、Spark 工具
│   ├── core_data_source_utils/  # 文件工具、S3 数据加载器
│   └── utils/             # 日志、日期工具
├── requests/              # 第三方包：requests
├── certifi/               # 第三方包：certifi  
├── charset_normalizer/    # 第三方包：charset_normalizer
├── idna/                  # 第三方包：idna
└── urllib3/               # 第三方包：urllib3
```

### Spark 处理流程

1. Spark 从 S3 下载归档文件到每个节点
2. 解压为 `./deps/` 目录（`#` 后的别名）
3. `PYTHONPATH=./deps` 让 Python 能找到目录中所有包
4. 自定义类库（`shared_libs`）和第三方包均可正常导入

## 项目结构

```
emr-python-lib/
├── shared_libs/                # 自定义 Python 类库
│   ├── constants/              # 配置常量、类型映射
│   ├── models/                 # 数据模型（BaseExternalDataSource 等）
│   ├── core_data_common_utils/ # 数据质量检查、Spark 工具
│   ├── core_data_source_utils/ # 文件工具、S3 数据加载器
│   └── utils/                  # 日志、日期工具
├── jobs/
│   └── main_job.py            # 主 PySpark 任务（验证用）
├── scripts/
│   ├── package_dependencies.py # 打包脚本
│   ├── submit_emr_serverless.py # EMR Serverless 提交脚本
│   └── submit_emr_on_ec2.py   # EMR on EC2 提交脚本
├── requirements.txt            # 第三方依赖声明
└── README.md
```

## 配置（环境变量）

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `S3_BUCKET` | `zpfsingapore` | S3 存储桶名称 |
| `S3_PREFIX` | `emr/poc` | S3 路径前缀 |
| `EMR_REGION` | `ap-southeast-1` | AWS 区域（避免与 AWS_REGION 冲突） |
| `EMR_RELEASE` | `emr-7.12.0` | EMR 版本标签 |
| `EMR_PYTHON_VERSION` | `39` | 目标 Python 版本（cpython） |
| `TARGET_PLATFORM` | `manylinux2014_x86_64` | 目标平台（用于下载 wheel） |

## 约束与注意事项

### 重要约束

1. **Python 版本匹配**：第三方包必须针对 EMR 运行时的 Python 版本构建。EMR 7.x 使用 Python 3.9。下载 wheel 时需指定 `--python-version 39`。

2. **平台匹配**：包含 C 扩展的包（如 numpy、pandas）需要平台特定的 wheel。对于 x86 EMR 实例使用 `--platform manylinux2014_x86_64`。

3. **预装包无需重复打包**：EMR 已预装 numpy、pandas、boto3、pyarrow。只需打包 EMR 上未预装的库。可通过诊断任务检查。

4. **归档大小**：建议控制在 200MB 以内。过大的归档会增加任务启动时间。

5. **不要包含 Python 解释器**：归档中不要打包 Python 二进制文件。应使用 EMR 内置的 Python。打包自定义 Python 会导致共享库依赖问题。

### EMR 预装包列表 (7.12.0)

- numpy 2.0.2
- pandas 2.3.3
- boto3 1.42.27
- pyarrow 21.0.0

### 跨账号 / 跨区域复用

要在不同 AWS 账号或区域使用本方案：

1. 设置环境变量：
   ```bash
   export S3_BUCKET=your-bucket
   export S3_PREFIX=your/prefix
   export EMR_REGION=your-region
   ```

2. 确保 IAM 角色存在：
   - EMR Serverless：`EMRServerlessS3RuntimeRole`，需要 S3 访问权限
   - EMR on EC2：`EMR_DefaultRole`（服务角色）+ `EMR_EC2_DefaultRole`（实例配置文件）

3. 运行打包和提交脚本即可。

## 测试验证结果

### EMR Serverless (emr-7.12.0) - 通过 ✓

- 从 fresh git clone 重新验证通过
- 第三方库（numpy、pandas、requests）：通过
- 自定义类库（shared_libs）：通过
- 完整输出日志：`test_results/emr_serverless_output.log`

### EMR on EC2 (emr-6.15.0) - 就绪（依赖基础设施）

- 提交脚本（`scripts/submit_emr_on_ec2.py`）使用相同的 `--archives` + `PYTHONPATH` 机制
- 与 Serverless 唯一区别：`spark.yarn.appMasterEnv.PYTHONPATH` 替代 `spark.emr-serverless.driverEnv.PYTHONPATH`
- 前提条件：VPC 需要有互联网连接（EMR bootstrap 需要下载软件包）
- 如集群创建失败并提示 "Time out occurred during bootstrap"，请检查安全组和互联网网关配置

## 标准化依赖打包流程

```
步骤 1: 开发自定义 Python 类库
         └── shared_libs/（你的自定义代码）

步骤 2: 声明第三方依赖
         └── requirements.txt（EMR 上未预装的包）

步骤 3: 运行打包脚本
         └── python3.12 scripts/package_dependencies.py
             ├── 下载平台特定的 wheel（Python 3.9, manylinux2014_x86_64）
             ├── 解压 wheel 到统一目录
             ├── 复制自定义 shared_libs 到同一目录
             └── 创建单一 tar.gz 归档

步骤 4: 上传归档和任务脚本到 S3
         └── s3://bucket/prefix/libs/pyspark_deps_all.tar.gz
             s3://bucket/prefix/jobs/main_job.py

步骤 5: 提交任务
         EMR Serverless:  python3.12 scripts/submit_emr_serverless.py
         EMR on EC2:      python3.12 scripts/submit_emr_on_ec2.py
```

### Spark Submit 核心参数

```bash
spark-submit \
  --archives s3://bucket/libs/pyspark_deps_all.tar.gz#deps \
  --conf spark.<平台特定配置>.PYTHONPATH=./deps \
  --conf spark.executorEnv.PYTHONPATH=./deps \
  s3://bucket/jobs/main_job.py
```

| 平台 | Driver PYTHONPATH 配置项 |
|------|------------------------|
| EMR Serverless | `spark.emr-serverless.driverEnv.PYTHONPATH=./deps` |
| EMR on EC2 (YARN cluster 模式) | `spark.yarn.appMasterEnv.PYTHONPATH=./deps` |
