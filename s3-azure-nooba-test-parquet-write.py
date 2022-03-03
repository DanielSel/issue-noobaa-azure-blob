import os
from pyspark.sql import SparkSession

# s3 info contains connection info for noobaa instances
s3_info = lambda: None
s3_info.minio = lambda: None
s3_info.minio.name="noobaa-debug-minio"
s3_info.minio.enabled=True
s3_info.minio.endpoint=""                   # URL of noobaa backed by S3 or 100% S3 compatible storage (e.g. minio)
s3_info.minio.access_key_id=""              
s3_info.minio.access_key_secret=""
s3_info.minio.signature_version="s3v4"
s3_info.azure = lambda: None
s3_info.azure.name="noobaa-debug-azure" 
s3_info.azure.enabled=True
s3_info.azure.endpoint=""                   # URL of noobaa backed by Azure Blob Storage
s3_info.azure.access_key_id=""
s3_info.azure.access_key_secret=""
s3_info.azure.signature_version="s3v4"
s3_info.testdata = lambda: None
s3_info.testdata.common = lambda: None
s3_info.testdata.common.bucket = "test"
s3_info.testdata.parquet = lambda: None
s3_info.testdata.parquet.filekey_write = "test-s-p-w/parquet_write_test.parquet"

def update_s3_info_from_env():
    s3_info.minio.endpoint = os.getenv('MINIO_ENDPOINT')
    s3_info.minio.access_key_id = os.getenv('MINIO_ACCESS_KEY')
    s3_info.minio.access_key_secret = os.getenv('MINIO_SECRET_ACCESS_KEY')
    s3_info.minio.signature_version = os.getenv('MINIO_SIGNATURE_VERSION', s3_info.minio.signature_version)
    if s3_info.minio.endpoint == None:
        s3_info.minio.enabled = False
    
    s3_info.azure.endpoint = os.getenv('AZURE_ENDPOINT')
    s3_info.azure.access_key_id = os.getenv('AZURE_ACCESS_KEY')
    s3_info.azure.access_key_secret = os.getenv('AZURE_SECRET_ACCESS_KEY')
    s3_info.azure.signature_version = os.getenv('AZURE_SIGNATURE_VERSION', s3_info.azure.signature_version)
    if s3_info.azure.endpoint == None:
        s3_info.azure.enabled = False
    
    s3_info.testdata.common.bucket = os.getenv('TESTDATA_BUCKET', s3_info.testdata.common.bucket)
    s3_info.testdata.parquet.filekey_write = os.getenv('TESTDATA_FILEKEY', s3_info.testdata.parquet.filekey_write)

def new_spark(s3_instance_info):
    print(f"[TEST SETUP] Creating new spark instance: {s3_instance_info.name}")
    cwd = os.getcwd()
    spark = SparkSession.builder \
    .master("local") \
    .appName(s3_instance_info.name) \
    .config('spark.submit.deployMode', 'client') \
    .config("spark.hadoop.fs.s3a.endpoint", s3_instance_info.endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", s3_instance_info.access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", s3_instance_info.access_key_secret) \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", True) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.experimental.input.fadvise","sequential") \
    .config("spark.jars", f"{cwd}/jars/hadoop-aws-3.3.1.jar,{cwd}/jars/aws-java-sdk-bundle-1.12.161.jar,{cwd}/jars/delta-core_2.12-1.1.0.jar") \
    .config("spark.driver.extraJavaOptions","-Djava.util.logging.config.file=/tmp/parquet.logging.properties -Dcom.amazonaws.sdk.disableCertChecking=true -Dcom.amazonaws.services.s3.enableV4=true") \
    .config("spark.executor.extraJavaOptions","-Djava.util.logging.config.file=/tmp/parquet.logging.properties -Dcom.amazonaws.sdk.disableCertChecking=true -Dcom.amazonaws.services.s3.enableV4=true") \
    .getOrCreate()
    return spark

def test_parquet_write(s3_instance_info, testdata):
    spark = new_spark(s3_instance_info)
    print("[TEST][PARQUET WRITE] Create DataFrame in memory")
    df = spark.createDataFrame([(-1, 1)], ('C1', 'C2'))

    print("[TEST][PARQUET WRITE] Writing parquet file to S3")
    df.write.format("parquet").save(f"s3a://{testdata.common.bucket}/{testdata.parquet.filekey_write}")
    print("[TEST][PARQUET WRITE] Successfully wrote parquet file to S3")

    spark.sparkContext.stop()
    spark.stop()

def test_parquet_minio_azure(s3_info):
    # noobaa s3
    if s3_info.minio.enabled:
        print("[TEST][PARQUET WRITE] Running test for minio-backed noobaa S3")
        test_parquet_write(s3_info.minio, s3_info.testdata)
    else:
        print("[TEST][PARQUET WRITE] Skipping test for minio-backed noobaa S3 (endpoint not set)")

    # noobaa azure blob
    if s3_info.minio.enabled:
        print("[TEST][PARQUET WRITE] Running test for azure-backed noobaa S3")
        test_parquet_write(s3_info.azure, s3_info.testdata) # FAILS, see noobaa-endpoint.log
    else:
        print("[TEST][PARQUET WRITE] Skipping test for azure-backed noobaa S3 (endpoint not set)")


def run_tests():
    print("[TEST SUITE] Running tests")
    test_parquet_minio_azure(s3_info)

if __name__ == "__main__":
    update_s3_info_from_env()
    run_tests()
