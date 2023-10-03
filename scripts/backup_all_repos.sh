#!/bin/bash

ROOT_PATH=$1
TIMESTAMP=$(date "+%Y%m%d-%H%M%S")
BUCKET_NAME="test-repo-backups"


if [ -z "$ROOT_PATH" ]; then
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



# Backup repositories first exiting if any fail
for namespace in "$ABSOLUTE_ROOT_PATH"/*; do

  if [ -d "$namespace" ]; then
    namespace_name=$(basename "$namespace")

    for repository in "$namespace"/*; do
      if [ -d "$repository" ]; then
        repository_name=$(basename "$repository")

        # Check if the .oxen directory exists in the repository
        if [ -d "$repository/.oxen" ]; then
          S3_DEST_NAME="s3://$BUCKET_NAME/$TIMESTAMP/$namespace_name/$repository_name.tar.gz"
    
          # Save repo as local .tar.gz
          oxen save "$repository" -o "$repository.tar.gz"

          # Upload to s3 
          aws s3 cp "$repository.tar.gz" "$S3_DEST_NAME"

          # Check if aws s3 cp was successful
          if [ $? -ne 0 ]; then
            echo "S3 cp failed, exiting."
            exit 1
          fi

          # Verify tarball uploaded to s3 
          if [ $? -ne 0 ]; then
            echo "Backup failed for $repository. Exiting."
            exit 1
          fi

          aws s3 ls "$S3_DEST_NAME"
          if [ $? -ne 0 ]; then
            echo "Verification failed, tarball not found in S3. Exiting."
            exit 1
          fi

          # Delete the local backup 
          echo "Attempting to delete $repository.tar.gz"
          if [ ! -e "$repository.tar.gz" ]; then
            echo "$repository.tar.gz does not exist. Repo not successfully backed up, exiting."
            exit 1
          fi
          rm -f "$repository.tar.gz"
        fi
      fi
    done
  fi
done






