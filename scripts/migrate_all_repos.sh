#!/bin/bash

ROOT_PATH=$1
MIGRATION_NAME=$2
TIMESTAMP=$(date "+%Y%m%d-%H%M%S")
BUCKET_NAME="test-repo-backups"


if [ -z "$ROOT_PATH" ] || [ -z "$MIGRATION_NAME" ]; then
  echo "Usage: $0 <root_path> <migration_name>"
  exit 1
fi


if [[ "$ROOT_PATH" == /* ]]; then
    ABSOLUTE_ROOT_PATH="$ROOT_PATH"
    ABSOLUTE_ROOT_PATH="$(realpath $ROOT_PATH)"
else
    ABSOLUTE_ROOT_PATH="$(pwd)/$ROOT_PATH"
    ABSOLUTE_ROOT_PATH="$(realpath $ROOT_PATH)"
fi

# Get latest run in s3
latest_datetime=$(aws s3 ls s3://$BUCKET_NAME/ | sort | awk '{print $NF}' | tail -n 1)
latest_backup_path="s3://$BUCKET_NAME/$(echo $latest_datetime | sed 's/\/$//')"

# Print latest backup 
echo "Found latest s3 backup: $latest_datetime"
echo "Verifying all namespaces are backed up before migration..."

# First check that all namespaces we're about to migrate exist in the latest s3 backup.
for namespace in "$ABSOLUTE_ROOT_PATH"/*; do

  if [ -d "$namespace" ]; then
    namespace_name=$(basename "$namespace")

    for repository in "$namespace"/*; do
      if [ -d "$repository" ]; then
        repository_name=$(basename "$repository")

          # Make sure this repository is included in the most recent backup, otherwise exit 
          echo "Checking for path... $latest_backup_path/$namespace_name/$repository_name.tar.gz"
          aws s3 ls "$latest_backup_path/$namespace_name/$repository_name.tar.gz"
          if [ $? -ne 0 ]; then
            echo "ERROR: $repository_name missing from latest S3 backup, exiting."
            exit 1
          fi
      fi  
    done  
  fi  
done  

echo "Verification complete. Migrating namespaces..."

for namespace in "$ABSOLUTE_ROOT_PATH"/*; do

  if [ -d "$namespace" ]; then
    namespace_name=$(basename "$namespace")

    for repository in "$namespace"/*; do
      if [ -d "$repository" ]; then
        repository_name=$(basename "$repository")
          # Run the migration 
          oxen migrate up "$MIGRATION_NAME" "$namespace_name/$repository_name" 

          if [ $? -ne 0 ]; then
            echo "Migration failed, exiting."
            exit 1
        fi  
      fi  
    done  
  fi  
done  

echo "All migrations complete."   


      




