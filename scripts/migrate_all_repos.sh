#!/bin/bash

ROOT_PATH=$1
VALID_REPOS_FILE=$2
MIGRATION_NAME=$3
TIMESTAMP=$(date "+%Y%m%d-%H%M%S")
BUCKET_NAME="test-repo-backups"


if [ -z "$ROOT_PATH" ] || [ -z "$MIGRATION_NAME" ] || [ -z "$VALID_REPOS_FILE" ]; then
  echo "Usage: $0 <root_path> <valid_repos_file> <migration_name>"
  exit 1
fi


if [[ "$ROOT_PATH" == /* ]]; then
    ABSOLUTE_ROOT_PATH="$ROOT_PATH"
    ABSOLUTE_ROOT_PATH="$(realpath $ROOT_PATH)"
else
    ABSOLUTE_ROOT_PATH="$(pwd)/$ROOT_PATH"
    ABSOLUTE_ROOT_PATH="$(realpath $ROOT_PATH)"
fi

if [ ! -e "$VALID_REPOS_FILE" ]; then
    echo "$VALID_REPOS_FILE does not exist. Exiting."
    exit 1
fi

# Get latest run in s3
latest_datetime=$(aws s3 ls s3://$BUCKET_NAME/ | sort | awk '{print $NF}' | tail -n 1)
latest_backup_path="s3://$BUCKET_NAME/$(echo $latest_datetime | sed 's/\/$//')"

# Print latest backup 
echo "Found latest s3 backup: $latest_datetime"
echo "Verifying all namespaces are backed up before migration..."

# First check that all namespaces we're about to migrate exist in the latest s3 backup.

while IFS= read -r line; do 
    namespace_name="${line%%/*}"  # Extracting namespace_name
    repository_name="${line##*/}"   

    ABSOLUTE_REPO_PATH="$ABSOLUTE_ROOT_PATH/$namespace_name/$repository_name"

    if [ ! -d "$ABSOLUTE_REPO_PATH" ]; then
        echo "ERROR: $ABSOLUTE_REPO_PATH does not exist, exiting."
        exit 1
    fi

    aws s3 ls "$latest_backup_path/$namespace_name/$repository_name.tar.gz"
      if [ $? -ne 0 ]; then
        echo "ERROR: $repository_name missing from latest S3 backup, exiting."
        exit 1
    fi
done < "$VALID_REPOS_FILE"

echo "Verification complete. Migrating namespaces..."
while IFS= read -r line; do 
    namespace_name="${line%%/*}"  # Extracting namespace_name
    repository_name="${line##*/}"   

    ABSOLUTE_REPO_PATH="$ABSOLUTE_ROOT_PATH/$namespace_name/$repository_name"
    # Run the migration 
    oxen migrate up "$MIGRATION_NAME" "$namespace_name/$repository_name" 

    if [ $? -ne 0 ]; then
      echo "Migration failed, exiting."
      exit 1
    fi  
done < "$VALID_REPOS_FILE"

echo "All migrations complete."   


      




