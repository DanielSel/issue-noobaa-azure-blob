ENV_FILE=${1:-"debug.env"}
docker run --env-file $ENV_FILE docker.io/danielrs2/issue-noobaa-azure-blob