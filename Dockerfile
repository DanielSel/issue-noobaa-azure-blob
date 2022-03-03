FROM docker.io/jupyter/pyspark-notebook:spark-3.2.1
COPY --chown=1000:100 jars jars
COPY --chown=1000:100 requirements.txt .
RUN pip install -r requirements.txt
COPY --chown=1000:100 s3-azure-nooba-test-parquet-write.py .
ENTRYPOINT [ "/opt/conda/bin/python3" ]
CMD [ "/home/jovyan/s3-azure-nooba-test-parquet-write.py" ]

# Documentation of ENV Variables
# ENV MINIO_ENDPOINT                                                            [required for success example]
# ENV MINIO_ACCESS_KEY                                                          [required for success example]
# ENV MINIO_SECRET_ACCESS_KEY                                                   [required for success example]
# ENV MINIO_SIGNATURE_VERSION       "s3v4"                                      [optional]
# ENV AZURE_ENDPOINT                                                            [required]
# ENV AZURE_ACCESS_KEY                                                          [required]
# ENV AZURE_SECRET_ACCESS_KEY                                                   [required]
# ENV AZURE_SIGNATURE_VERSION       "s3v4"                                      [optional]
# ENV TESTDATA_BUCKET               "test"                                      [optional]
# ENV TESTDATA_FILEKEY              "test-s-p-w/parquet_write_test.parquet"     [optional]
