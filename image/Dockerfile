# 书籍示例 CI 验证环境：Java 21 + Python 3 + PySpark 3.5.4（与 README 环境要求一致）
# 流水线 .cnb.yml 用 docker.build 引用本文件；镜像自动缓存，仅当本文件变化时重建
FROM eclipse-temurin:21-jdk-jammy
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --no-cache-dir pyspark==3.5.4
