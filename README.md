# Reproducible Example for GitHub issue in repo [noobaa/noobaa-core](https://github.com/noobaa/noobaa-core)

## Issue Description
### Actual behavior
Multipart upload triggered by Spark's *hadoop-aws* library fails after a long time with `HTTP Code 500: Internal Server Error` for Noobaa instances deployed with Azure Blob as storage backend.

### Expected behavior
Writing the file works as it does on Noobaa instances deployed with S3/Minio backends.

## How to run
Fill in environment variables in debug.env


| ENV variable                 | Required / Optional | Default                                   | Description                                                           |
| ---------------------------- | ------------------- | ----------------------------------------- | --------------------------------------------------------------------- |
| **MINIO_ENDPOINT**           | Required            | `""`                                      | URL of Noobaa instance deployed with S3/Minio backend                 |
| **MINIO_ACCESS_KEY**         | Required            | `""`                                      | Access Key of Noobaa instance deployed with S3/Minio backend          |
| **MINIO_SECRET_ACCESS_KEY**  | Required            | `""`                                      | Secret Access Key of Noobaa instance deployed with S3/Minio backend   |
| **MINIO_SIGNATURE_VERSION**  | Optional            | `"s3v4"`                                  | Signature Version of Noobaa instance deployed with S3/Minio backend   |
| **AZURE_ENDPOINT**           | Required            | `""`                                      | URL of Noobaa instance deployed with Azure Blob backend               |
| **AZURE_ACCESS_KEY**         | Required            | `""`                                      | Access Key of Noobaa instance deployed with Azure Blob backend        |
| **AZURE_SECRET_ACCESS_KEY**  | Required            | `""`                                      | Secret Access Key of Noobaa instance deployed with Azure Blob backend |
| **AZURE_SIGNATURE_VERSION**  | Optional            | `"s3v4"`                                  | Signature Version of Noobaa instance deployed with Azure Blob backend |
| **TESTDATA_BUCKET**          | Optional            | `"test"`                                  | Bucket where file will be written                                     |
| **TESTDATA_FILEKEY**         | Optional            | `"test-s-p-w/parquet_write_test.parquet"` | Key of file to write                                                  |

**NOTE**: The example runs a successful case (**MINIO_\***) and a failing case (**AZURE_\***). Either case can be disabled by removing the **_ENDPOINT** variable (or all corresponding variables).