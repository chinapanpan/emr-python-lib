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
#
# 使用方式（提交任务时，两平台通用）：
#   spark-submit --archives s3://bucket/pyspark_deps_all.tar.gz#deps \
#     --conf spark.emr-serverless.driverEnv.PYTHONPATH=./deps \
#     --conf spark.executorEnv.PYTHONPATH=./deps \
#     s3://bucket/main_job.py

set -euo pipefail

# ============================================================
# 配置（可通过环境变量覆盖）
# ============================================================
S3_BUCKET="${S3_BUCKET:-zpfsingapore}"
S3_PREFIX="${S3_PREFIX:-emr/poc}"
EMR_PYTHON_VERSION="${EMR_PYTHON_VERSION:-39}"
TARGET_PLATFORM="${TARGET_PLATFORM:-manylinux2014_x86_64}"
AWS_REGION="${EMR_REGION:-ap-southeast-1}"

# 项目目录（脚本所在目录的上一级）
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
SHARED_LIBS_DIR="$BASE_DIR/shared_libs"
JOBS_DIR="$BASE_DIR/jobs"
BUILD_DIR="$BASE_DIR/build"

# 第三方包列表（仅需打包 EMR 未预装的）
PACKAGES=(
    "requests"
    "charset_normalizer"
    "idna"
    "urllib3"
    "certifi"
)

# ============================================================
echo "============================================================"
echo "EMR PySpark 统一依赖打包"
echo "============================================================"
echo "  目标 Python 版本: cpython-${EMR_PYTHON_VERSION}"
echo "  目标平台:         ${TARGET_PLATFORM}"
echo "  S3 目标:          s3://${S3_BUCKET}/${S3_PREFIX}/"
echo "============================================================"

# ============================================================
# 步骤 1：清理构建目录
# ============================================================
echo ""
echo "[..] 清理构建目录..."
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/wheels" "$BUILD_DIR/unified_package"
echo "[OK] 构建目录已清理: $BUILD_DIR"

# ============================================================
# 步骤 2：下载第三方包 wheel
# ============================================================
echo ""
echo "[..] 下载第三方包 wheel（Python ${EMR_PYTHON_VERSION}, 平台 ${TARGET_PLATFORM}）"
echo "     包列表: ${PACKAGES[*]}"

for pkg in "${PACKAGES[@]}"; do
    echo "     下载: $pkg"
    # 优先下载平台特定的二进制 wheel
    if ! pip download \
        --python-version "$EMR_PYTHON_VERSION" \
        --platform "$TARGET_PLATFORM" \
        --only-binary=:all: \
        --no-deps \
        -d "$BUILD_DIR/wheels" \
        "$pkg" 2>/dev/null; then
        # 纯 Python 包回退：不限制平台
        echo "     (回退为纯 Python 下载: $pkg)"
        pip download \
            --no-deps \
            -d "$BUILD_DIR/wheels" \
            "$pkg" 2>/dev/null
    fi
done

echo ""
echo "[OK] 已下载的 wheel 文件:"
ls -1 "$BUILD_DIR/wheels/"*.whl 2>/dev/null | while read -r f; do
    echo "     - $(basename "$f")"
done

# ============================================================
# 步骤 3：复制自定义类库
# ============================================================
echo ""
echo "[..] 添加自定义类库: $SHARED_LIBS_DIR"
cp -r "$SHARED_LIBS_DIR" "$BUILD_DIR/unified_package/shared_libs"
# 清理 __pycache__
find "$BUILD_DIR/unified_package/shared_libs" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
echo "[OK] 自定义类库已复制"

# ============================================================
# 步骤 4：解压第三方 wheel 到统一目录
# ============================================================
echo ""
echo "[..] 解压第三方 wheel 到统一目录"
for whl in "$BUILD_DIR/wheels/"*.whl; do
    [ -f "$whl" ] || continue
    pkg_name=$(basename "$whl" | cut -d'-' -f1)
    echo "     + $pkg_name"
    # wheel 本质上是 zip 文件，解压时排除 .dist-info
    unzip -q -o "$whl" -d "$BUILD_DIR/unified_package" -x "*.dist-info/*" 2>/dev/null
done
echo "[OK] 第三方包已解压"

# ============================================================
# 步骤 5：创建 tar.gz 归档
# ============================================================
echo ""
ARCHIVE_PATH="$BUILD_DIR/pyspark_deps_all.tar.gz"
echo "[..] 创建统一归档: $ARCHIVE_PATH"
tar -czf "$ARCHIVE_PATH" -C "$BUILD_DIR/unified_package" .

ARCHIVE_SIZE=$(du -h "$ARCHIVE_PATH" | cut -f1)
echo "[OK] 归档已创建: $ARCHIVE_PATH ($ARCHIVE_SIZE)"

echo ""
echo "     归档内容（顶层）："
ls -1 "$BUILD_DIR/unified_package/" | while read -r item; do
    if [ -d "$BUILD_DIR/unified_package/$item" ]; then
        echo "       [DIR]  $item/"
    else
        echo "       [FILE] $item"
    fi
done

# ============================================================
# 步骤 6：上传到 S3
# ============================================================
echo ""
echo "[..] 上传归档到 S3..."
aws s3 cp "$ARCHIVE_PATH" "s3://${S3_BUCKET}/${S3_PREFIX}/libs/pyspark_deps_all.tar.gz" \
    --region "$AWS_REGION"
echo "[OK] 已上传: s3://${S3_BUCKET}/${S3_PREFIX}/libs/pyspark_deps_all.tar.gz"

echo ""
echo "[..] 上传任务脚本到 S3..."
aws s3 cp "$JOBS_DIR/main_job.py" "s3://${S3_BUCKET}/${S3_PREFIX}/jobs/main_job.py" \
    --region "$AWS_REGION"
echo "[OK] 已上传: s3://${S3_BUCKET}/${S3_PREFIX}/jobs/main_job.py"

# ============================================================
# 完成
# ============================================================
echo ""
echo "============================================================"
echo "打包完成"
echo "============================================================"
echo ""
echo "提交任务时使用方式:"
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
