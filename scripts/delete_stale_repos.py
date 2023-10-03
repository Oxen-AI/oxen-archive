import os, sys

if len(sys.argv) != 3:
    print("Usage delete_stale_repos.py repo_ids.txt /var/oxen/data")
    exit()


repos_file = sys.argv[1]
sync_dir = sys.argv[2]

valid_repos = set()

with open(repos_file) as f:
    for line in f:
        valid_repos.add(line.strip())

print(f"Got {len(valid_repos)} repos")


total_valid = 0
total_with_oxen_config = 0
total_invalid = 0
for namespace_basename in os.listdir(sync_dir):
    namespace_path = os.path.join(sync_dir, namespace_basename)
    if os.path.isdir(namespace_path):
        print(f"Processing {namespace_path}")
        for repo_path in os.listdir(namespace_path):
            full_path = os.path.join(namespace_path, repo_path)
            check_path = f"{namespace_basename}/{repo_path}"
            print(f"Checking {check_path}")
            if check_path in valid_repos:
                print(f"Is valid: {full_path}")
                if os.path.join(full_path, ".oxen/config.toml"):
                    total_with_oxen_config += 1
                total_valid += 1
            else:
                print(f"Not valid: {full_path}")
                total_invalid += 1

print("\n\n")
print(f"Total Valid: {total_valid}")
print(f"Total Valid With Config: {total_with_oxen_config}")
print(f"Total Invalid: {total_invalid}")
