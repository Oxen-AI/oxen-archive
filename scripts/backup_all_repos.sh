#!/bin/bash

ROOT_PATH=$1
VALID_REPOS_FILE=$2
TIMESTAMP=$(date "+%Y%m%d-%H%M%S")
BUCKET_NAME="test-repo-backups"



if [ -z "$ROOT_PATH" ] || [ -z "$VALID_REPOS_FILE" ]; then
  echo "Usage: $0 <root_path> <valid_repos_txt_file>"
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

while IFS= read -r line; do 
    namespace_name="${line%%/*}"  # Extracting namespace_name
    repository_name="${line##*/}"   

    echo "Got namespace $namespace_name and repo $repository_name"



    ABSOLUTE_REPO_PATH="$ABSOLUTE_ROOT_PATH/$namespace_name/$repository_name"

    if [ ! -d "$ABSOLUTE_REPO_PATH" ]; then
        echo "ERROR: $ABSOLUTE_REPO_PATH does not exist, exiting."
        exit 1
    fi

    echo "Checking path $ABSOLUTE_REPO_PATH/.oxen"

    if [ -d "$ABSOLUTE_REPO_PATH/.oxen" ]; then
          echo "Backing up $namespace_name/$repository_name"
          S3_DEST_NAME="s3://$BUCKET_NAME/$TIMESTAMP/$namespace_name/$repository_name.tar.gz"
    
          # Save repo as local .tar.gz
          oxen save "$ABSOLUTE_REPO_PATH" -o "$repository_name.tar.gz"

          # Upload to s3 
          aws s3 cp "$repository_name.tar.gz" "$S3_DEST_NAME"

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
          echo "Attempting to delete $repository_name.tar.gz"
          if [ ! -e "$repository_name.tar.gz" ]; then
            echo "$repository_name.tar.gz does not exist. Repo not successfully backed up, exiting."
            exit 1
          fi
          rm -f "$repository_name.tar.gz"
    fi
  done < "$VALID_REPOS_FILE"









