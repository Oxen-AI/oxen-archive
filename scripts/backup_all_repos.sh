#!/bin/bash

BUCKET_NAME="test-repo-backups"
POSITIONAL_ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    -b|--bucket)
      BUCKET_NAME="$2"
      shift # past argument
      shift # past value
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      POSITIONAL_ARGS+=("$1") 
      shift # past argument
      ;;
  esac
done

set -- "${POSITIONAL_ARGS[@]}" 

ROOT_PATH="${POSITIONAL_ARGS[0]}"
VALID_REPOS_FILE="${POSITIONAL_ARGS[1]}"
TIMESTAMP=$(date "+%Y%m%d-%H%M%S")


while getopts ":b:" opt; do
  case $opt in
    b)
      BUCKET_NAME="$OPTARG"
      ;;
    \?)
      echo "Usage: $0 [-b bucket_name] <root_path> <valid_repos_txt_file>"
      exit 1
      ;;
  esac
done


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


repo_counter=0

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


  if [ ! -d "$ABSOLUTE_REPO_PATH/.oxen" ]; then
        echo "ERROR: No .oxen subdirectory in $ABSOLUTE_REPO_PATH. Exiting."
        exit 1
    fi
    
  # TODO: remove this if, is redundant
  if [ -d "$ABSOLUTE_REPO_PATH/.oxen" ]; then
        echo "Backing up $namespace_name/$repository_name"
        S3_DEST_NAME="s3://$BUCKET_NAME/$TIMESTAMP/$namespace_name/$repository_name"



        # Upload to s3 
        aws s3 cp --quiet --recursive $ABSOLUTE_REPO_PATH/.oxen $S3_DEST_NAME/.oxen

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

        # Ensure directory structure and files made it - proxied by config.toml
        aws s3 ls "$S3_DEST_NAME/.oxen/config.toml"
        if [ $? -ne 0 ]; then
          echo "Verification failed, new oxen directory not found in S3. Exiting."
          exit 1
        fi

        # Delete the local backup 
        # echo "Attempting to delete $repository_name.tar.gz"
        # if [ ! -e "$repository_name.tar.gz" ]; then
        #   echo "$repository_name.tar.gz does not exist. Repo not successfully backed up, exiting."
        #   exit 1
        # fi
        # rm -f "$repository_name.tar.gz"

        ((repo_counter++))
        echo "Processed repository count: $repo_counter"
  fi
  done < "$VALID_REPOS_FILE"









