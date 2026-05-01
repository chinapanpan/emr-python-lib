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
│  │  (shared_libs/)      │  │  (requests, numpy,        │ │
│  │                      │  │   pandas 等)              │ │
│  │  - constants/        │  │                            │ │
│  │  - models/           │  │  通过 pip install -t       │ │
│  │  - core_data_*_utils/│  │  安装到统一目录            │ │
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
bash scripts/package_dependencies.sh
```

该脚本执行以下操作：
1. `pip install -r requirements.txt -t` 安装第三方包到统一目录
2. `cp -r shared_libs/` 复制自定义类库到同一目录
3. `tar -czf` 创建单一 `pyspark_deps_all.tar.gz` 归档
4. `aws s3 cp` 上传归档和任务脚本至 S3

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
├── numpy/                 # 第三方包：numpy
├── pandas/                # 第三方包：pandas
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
│   ├── package_dependencies.sh  # 打包脚本（Shell）
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
| `EMR_SUBNET_ID` | （自动选择默认子网） | EMR on EC2 使用的子网 |

## 约束与注意事项

### 重要约束

1. **包含所有运行时依赖**：即使 EMR Serverless 预装了 numpy/pandas，EMR on EC2 的 YARN container 中可能无法访问系统 site-packages。建议将所有运行时依赖都打入归档，确保跨平台一致性。

2. **归档大小**：建议控制在 200MB 以内。过大的归档会增加任务启动时间。

3. **不要包含 Python 解释器**：归档中不要打包 Python 二进制文件。应使用 EMR 内置的 Python。打包自定义 Python 会导致共享库依赖问题。

4. **VPC 网络要求**：EMR on EC2 集群的子网必须能访问 S3（通过 Internet Gateway 或 S3 VPC Endpoint）。如 S3 VPC Endpoint 有 IP 限制策略会导致 bootstrap 失败。

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

### EMR on EC2 (emr-7.12.0) - 通过 ✓

- 集群创建 + spark-submit step 提交均通过
- 第三方库（numpy、pandas、requests）：通过
- 自定义类库（shared_libs）：通过
- 使用 `--deploy-mode cluster` + `--archives` + `spark.yarn.appMasterEnv.PYTHONPATH`
- 与 Serverless 唯一区别：`spark.yarn.appMasterEnv.PYTHONPATH` 替代 `spark.emr-serverless.driverEnv.PYTHONPATH`

## 标准化依赖打包流程

```
步骤 1: 开发自定义 Python 类库
         └── shared_libs/（你的自定义代码）

步骤 2: 声明第三方依赖
         └── requirements.txt（所有运行时需要的包）

步骤 3: 运行打包脚本
         └── bash scripts/package_dependencies.sh
             ├── pip install -t: 安装第三方包到统一目录
             ├── cp -r: 复制自定义 shared_libs 到同一目录
             ├── tar -czf: 创建单一 tar.gz 归档
             └── aws s3 cp: 上传到 S3

步骤 4: 提交任务
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
