#!/bin/bash
#
# 将所有依赖（自定义类库 + 第三方包）打包为单一统一归档文件。
#
# 输出：
#   - pyspark_deps_all.tar.gz：包含自定义 shared_libs 和第三方包
#     通过 --archives 解压后，所有包均可通过 PYTHONPATH 导入
#
# 使用方式：
#   bash scripts/package_dependencies.sh

set -euo pipefail

# ============================================================
# 配置（可通过环境变量覆盖）
# ============================================================
S3_BUCKET="${S3_BUCKET:-zpfsingapore}"
S3_PREFIX="${S3_PREFIX:-emr/poc}"
AWS_REGION="${EMR_REGION:-ap-southeast-1}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
SHARED_LIBS_DIR="$BASE_DIR/shared_libs"
JOBS_DIR="$BASE_DIR/jobs"
BUILD_DIR="$BASE_DIR/build"
REQUIREMENTS="$BASE_DIR/requirements.txt"

# ============================================================
echo "============================================================"
echo "EMR PySpark 统一依赖打包"
echo "============================================================"
echo "  S3 目标: s3://${S3_BUCKET}/${S3_PREFIX}/"
echo "============================================================"

# ============================================================
# 步骤 1：清理构建目录
# ============================================================
echo ""
echo "[步骤1] 清理构建目录..."
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/unified_package"
echo "[OK] $BUILD_DIR"

# ============================================================
# 步骤 2：安装第三方包到统一目录
# ============================================================
echo ""
echo "[步骤2] 安装第三方包..."
pip install -r "$REQUIREMENTS" -t "$BUILD_DIR/unified_package" --quiet
echo "[OK] 第三方包已安装到 $BUILD_DIR/unified_package"

# ============================================================
# 步骤 3：复制自定义类库
# ============================================================
echo ""
echo "[步骤3] 复制自定义类库..."
cp -r "$SHARED_LIBS_DIR" "$BUILD_DIR/unified_package/shared_libs"
find "$BUILD_DIR/unified_package" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "$BUILD_DIR/unified_package" -type d -name "*.dist-info" -exec rm -rf {} + 2>/dev/null || true
echo "[OK] shared_libs 已复制"

# ============================================================
# 步骤 4：创建 tar.gz 归档
# ============================================================
echo ""
ARCHIVE_PATH="$BUILD_DIR/pyspark_deps_all.tar.gz"
echo "[步骤4] 创建归档..."
tar -czf "$ARCHIVE_PATH" -C "$BUILD_DIR/unified_package" .
echo "[OK] $ARCHIVE_PATH ($(du -h "$ARCHIVE_PATH" | cut -f1))"

echo ""
echo "     归档内容（顶层）："
ls -1 "$BUILD_DIR/unified_package/" | head -20 | while read -r item; do
    if [ -d "$BUILD_DIR/unified_package/$item" ]; then
        echo "       [DIR]  $item/"
    else
        echo "       [FILE] $item"
    fi
done

# ============================================================
# 步骤 5：上传到 S3
# ============================================================
echo ""
echo "[步骤5] 上传到 S3..."
aws s3 cp "$ARCHIVE_PATH" "s3://${S3_BUCKET}/${S3_PREFIX}/libs/pyspark_deps_all.tar.gz" \
    --region "$AWS_REGION" --quiet
echo "[OK] s3://${S3_BUCKET}/${S3_PREFIX}/libs/pyspark_deps_all.tar.gz"

aws s3 cp "$JOBS_DIR/main_job.py" "s3://${S3_BUCKET}/${S3_PREFIX}/jobs/main_job.py" \
    --region "$AWS_REGION" --quiet
echo "[OK] s3://${S3_BUCKET}/${S3_PREFIX}/jobs/main_job.py"

# ============================================================
echo ""
echo "============================================================"
echo "打包完成"
echo "============================================================"
echo ""
echo "提交任务时使用:"
echo ""
echo "  EMR Serverless:"
echo "    --archives s3://${S3_BUCKET}/${S3_PREFIX}/libs/pyspark_deps_all.tar.gz#deps"
echo "    --conf spark.emr-serverless.driverEnv.PYTHONPATH=./deps"
echo "    --conf spark.executorEnv.PYTHONPATH=./deps"
echo ""
echo "  EMR on EC2 (spark-submit):"
echo "    --archives s3://${S3_BUCKET}/${S3_PREFIX}/libs/pyspark_deps_all.tar.gz#deps"
echo "    --conf spark.yarn.appMasterEnv.PYTHONPATH=./deps"
echo "    --conf spark.executorEnv.PYTHONPATH=./deps"
