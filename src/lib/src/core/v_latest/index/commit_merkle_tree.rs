use std::collections::{HashMap, HashSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::{Arc, LazyLock};

use lru::LruCache;
use parking_lot::Mutex;
use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};

use crate::constants::{DIR_HASHES_DIR, HISTORY_DIR};
use crate::core::db;
use crate::core::db::merkle_node::MerkleNodeDB;

use crate::model::merkle_tree::node::EMerkleTreeNode;

use crate::model::merkle_tree::node::FileNode;
use crate::model::merkle_tree::node::MerkleTreeNode;

use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, MerkleHash, MerkleTreeNodeType, PartialNode};

use crate::util::{self, hasher};

use std::str::FromStr;

const NODE_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(20_000_000).unwrap();

/// Metadata about a cached node including how deeply it was loaded
#[derive(Clone)]
struct CachedNode {
    /// The actual node data without children
    hash: MerkleHash,
    node: EMerkleTreeNode,
    parent_id: Option<MerkleHash>,
    /// Hashes of child nodes (for reconstruction)
    child_hashes: Vec<MerkleHash>,
    /// Depth to which this node's children were loaded (-1 for recursive, 0 for no children)
    loaded_depth: i32,
    /// Whether this node was loaded recursively
    is_recursive: bool,
}

impl CachedNode {
    /// Create a CachedNode from a MerkleTreeNode
    fn from_tree_node(node: &MerkleTreeNode, depth: i32, is_recursive: bool) -> Self {
        let child_hashes: Vec<MerkleHash> = node.children.iter().map(|child| child.hash).collect();

        CachedNode {
            hash: node.hash,
            node: node.node.clone(),
            parent_id: node.parent_id,
            child_hashes,
            loaded_depth: depth,
            is_recursive,
        }
    }

    /// Check if this cached node satisfies the requested depth
    fn satisfies_depth(&self, requested_depth: i32, requested_recursive: bool) -> bool {
        if requested_recursive && !self.is_recursive {
            return false;
        }
        if self.is_recursive || self.loaded_depth == -1 {
            return true;
        }
        self.loaded_depth >= requested_depth
    }
}

impl fmt::Display for CachedNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} [depth {} | recursive {}]",
            self.node, self.loaded_depth, self.is_recursive
        )
    }
}

// Static cache of merkle tree nodes per repository
static NODE_CACHES: LazyLock<
    Mutex<HashMap<PathBuf, Arc<Mutex<LruCache<MerkleHash, CachedNode>>>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Removes a repository's node cache from the global cache map.
pub fn remove_node_cache(repository_path: impl AsRef<std::path::Path>) -> Result<(), OxenError> {
    let mut caches = NODE_CACHES.lock();
    let _ = caches.remove(&repository_path.as_ref().to_path_buf());
    Ok(())
}

/// Execute an operation with access to the repository's node cache.
/// The cache is locked for the entire duration of the operation.
fn with_node_cache<F, T>(repository: &LocalRepository, operation: F) -> Result<T, OxenError>
where
    F: FnOnce(&mut LruCache<MerkleHash, CachedNode>) -> Result<T, OxenError>,
{
    // Get or create the cache for this repository
    let cache_arc = {
        let mut caches = NODE_CACHES.lock();
        caches
            .entry(repository.path.clone())
            .or_insert_with(|| Arc::new(Mutex::new(LruCache::new(NODE_CACHE_SIZE))))
            .clone()
    };

    // Lock the cache once for the entire operation
    let mut cache = cache_arc.lock();
    operation(&mut *cache)
}

pub struct CommitMerkleTree {
    pub root: MerkleTreeNode,
    pub dir_hashes: HashMap<PathBuf, MerkleHash>,
}

impl CommitMerkleTree {
    // Commit db is the directories per commit
    // This helps us skip to a directory in the tree
    // .oxen/history/{COMMIT_ID}/dir_hashes
    fn dir_hash_db_path(repo: &LocalRepository, commit: &Commit) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(HISTORY_DIR))
            .join(&commit.id)
            .join(DIR_HASHES_DIR)
    }

    pub fn dir_hash_db_path_from_commit_id(
        repo: &LocalRepository,
        commit_id: MerkleHash,
    ) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(HISTORY_DIR))
            .join(commit_id.to_string())
            .join(DIR_HASHES_DIR)
    }

    pub fn root_with_children(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_hash = MerkleHash::from_str(&commit.id)?;
        CommitMerkleTree::read_node(repo, &node_hash, true)
    }

    pub fn root_without_children(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_hash = MerkleHash::from_str(&commit.id)?;
        // Read the root node at depth 1 to get the directory node as well
        CommitMerkleTree::read_depth(repo, &node_hash, 1)
    }

    // Used in the checkout logic to simultaneously load in the target tree and find all of its dir and vnode hashes
    // Saves an extra tree traversal needed to list these hashes
    pub fn root_with_children_and_hashes(
        repo: &LocalRepository,
        commit: &Commit,
        hashes: &mut HashSet<MerkleHash>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_hash = MerkleHash::from_str(&commit.id)?;
        CommitMerkleTree::read_node_with_hashes(repo, &node_hash, hashes)
    }

    // Used in the checkout logic to simultaneosuly:
    // A: load only the children of the from tree which aren't present in the target tree
    // B: list the shared dir and vnode hashes between the trees
    // C: find all of its unique nodes, reduced to 'PartialNodes' to save memory

    // Saves 2 extra tree traversals needed to list the shared hashes and the unique nodes
    pub fn root_with_unique_children(
        repo: &LocalRepository,
        commit: &Commit,
        base_hashes: &mut HashSet<MerkleHash>,
        shared_hashes: &mut HashSet<MerkleHash>,
        partial_nodes: &mut HashMap<PathBuf, PartialNode>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_hash = MerkleHash::from_str(&commit.id)?;
        CommitMerkleTree::read_unique_nodes(
            repo,
            &node_hash,
            base_hashes,
            shared_hashes,
            partial_nodes,
        )
    }

    pub fn from_commit(repo: &LocalRepository, commit: &Commit) -> Result<Self, OxenError> {
        // This debug log is to help make sure we don't load the tree too many times
        // if you see it in the logs being called too much, it could be why the code is slow.
        log::debug!("Load tree from commit: {} in repo: {:?}", commit, repo.path);
        let node_hash = MerkleHash::from_str(&commit.id)?;
        let root =
            CommitMerkleTree::read_node(repo, &node_hash, true)?.ok_or(OxenError::basic_str(
                format!("Merkle tree hash not found for commit: '{}'", commit.id),
            ))?;
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        Ok(Self { root, dir_hashes })
    }

    pub fn from_path_recursive(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
    ) -> Result<Self, OxenError> {
        let load_recursive = true;
        CommitMerkleTree::from_path(repo, commit, path, load_recursive)
    }

    pub fn from_path_depth(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        depth: i32,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let mut node_path = path.as_ref().to_path_buf();
        if node_path == PathBuf::from(".") {
            node_path = PathBuf::from("");
        }
        log::debug!(
            "Read path {:?} in commit {:?} depth: {}",
            node_path,
            commit,
            depth
        );
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let Some(node_hash) = dir_hashes.get(&node_path).cloned() else {
            log::debug!(
                "dir_hashes {:?} does not contain path: {:?}",
                dir_hashes,
                node_path
            );
            return Err(OxenError::basic_str(format!(
                "Can only load a subtree with an existing directory path: '{}'",
                node_path.to_str().unwrap()
            )));
        };

        let Some(root) = CommitMerkleTree::read_depth(repo, &node_hash, depth)? else {
            return Err(OxenError::basic_str(format!(
                "Merkle tree hash not found for: '{}' hash: {:?}",
                node_path.to_str().unwrap(),
                node_hash
            )));
        };
        Ok(Some(root))
    }

    pub fn from_path(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        load_recursive: bool,
    ) -> Result<Self, OxenError> {
        let node_path = path.as_ref();
        log::debug!("Read path {:?} in commit {:?}", node_path, commit);
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path).cloned();

        let root = if let Some(node_hash) = node_hash {
            // We are reading a node with children
            log::debug!("Look up dir 🗂️ {:?}", node_path);
            CommitMerkleTree::read_node(repo, &node_hash, load_recursive)?.ok_or(
                OxenError::basic_str(format!(
                    "Merkle tree hash not found for dir: '{}' in commit: '{}'",
                    node_path.to_str().unwrap(),
                    commit.id
                )),
            )?
        } else {
            // We are skipping to a file in the tree using the dir_hashes db
            log::debug!("Look up file 📄 {:?}", node_path);
            CommitMerkleTree::read_file(repo, &dir_hashes, node_path)?.ok_or(
                OxenError::basic_str(format!(
                    "Merkle tree hash not found for file: '{}' in commit: '{}'",
                    node_path.to_str().unwrap(),
                    commit.id
                )),
            )?
        };
        Ok(Self { root, dir_hashes })
    }

    /// Read the dir metadata from the path, without reading the children
    pub fn dir_without_children(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_path = path.as_ref();
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path).cloned();
        if let Some(node_hash) = node_hash {
            // We are reading a node with children
            log::debug!("Look up dir 🗂️ {:?}", node_path);
            CommitMerkleTree::read_node(repo, &node_hash, false)
        } else {
            Ok(None)
        }
    }

    pub fn dir_with_children(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_path = path.as_ref();
        log::debug!("Read path {:?} in commit {:?}", node_path, commit);
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path).cloned();
        if let Some(node_hash) = node_hash {
            // We are reading a node with children
            log::debug!("Look up dir {:?}", node_path);
            // Read the node at depth 1 to get VNodes and Sub-Files/Dirs
            // We don't count VNodes in the depth
            CommitMerkleTree::read_depth(repo, &node_hash, 1)
        } else {
            Ok(None)
        }
    }

    pub fn dir_with_children_recursive(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_path = path.as_ref();
        log::debug!("Read path {:?} in commit {:?}", node_path, commit);
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path).cloned();
        if let Some(node_hash) = node_hash {
            // We are reading a node with children
            log::debug!("Look up dir 🗂️ {:?}", node_path);
            // Read the node at depth 2 to get VNodes and Sub-Files/Dirs
            CommitMerkleTree::read_node(repo, &node_hash, true)
        } else {
            Ok(None)
        }
    }

    pub fn read_node(
        repo: &LocalRepository,
        hash: &MerkleHash,
        recurse: bool,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        with_node_cache(repo, |cache| {
            Self::read_node_cached(repo, hash, recurse, cache)
                .map(|opt| opt.map(|arc| (*arc).clone()))
        })
    }

    fn read_node_cached(
        repo: &LocalRepository,
        hash: &MerkleHash,
        recurse: bool,
        cache: &mut LruCache<MerkleHash, CachedNode>,
    ) -> Result<Option<Arc<MerkleTreeNode>>, OxenError> {
        // Build node from cache if possible
        let requested_depth = if recurse { -1 } else { 0 };
        let cached_info = cache.get(hash).cloned();

        let mut node = if let Some(cached_node) = cached_info {
            log::debug!("Cache hit: {}", cached_node);

            // Check if cached version satisfies our requirements
            if cached_node.satisfies_depth(requested_depth, recurse) {
                // Reconstruct the complete node from cache
                return Ok(Some(Arc::new(Self::reconstruct_from_cache(
                    repo,
                    &cached_node,
                    requested_depth,
                    recurse,
                    cache,
                )?)));
            }

            // Need to load more children - start with cached node data
            log::debug!("Cached node exists but needs deeper loading");
            MerkleTreeNode {
                hash: cached_node.hash,
                node: cached_node.node.clone(),
                parent_id: cached_node.parent_id,
                children: Vec::new(),
            }
        } else {
            log::debug!("Cache miss for node hash [{}]", hash);
            if !MerkleNodeDB::exists(repo, hash) {
                return Ok(None);
            }
            MerkleTreeNode::from_hash(repo, hash)?
        };

        if node.is_leaf() {
            log::debug!("Node {} is a leaf node, skipping child loading", hash);
        } else {
            // Load children if not a leaf node
            log::debug!("Loading children for node {}", hash);
            let mut node_db = MerkleNodeDB::open_read_only(repo, hash)?;
            CommitMerkleTree::read_children_from_node_cached(
                repo,
                &mut node_db,
                &mut node,
                recurse,
                cache,
            )?;
        }

        // Cache the node with its children
        log::debug!("caching node {}", &node);
        let depth = if recurse { -1 } else { 0 };
        cache.put(*hash, CachedNode::from_tree_node(&node, depth, recurse));

        Ok(Some(Arc::new(node)))
    }

    /// Reconstruct a MerkleTreeNode from cache data
    fn reconstruct_from_cache(
        repo: &LocalRepository,
        cached: &CachedNode,
        requested_depth: i32,
        requested_recursive: bool,
        cache: &mut LruCache<MerkleHash, CachedNode>,
    ) -> Result<MerkleTreeNode, OxenError> {
        let mut node = MerkleTreeNode {
            hash: cached.hash,
            node: cached.node.clone(),
            parent_id: cached.parent_id,
            children: Vec::new(),
        };

        // Reconstruct children if needed
        // depth >= 0 means load at least immediate children
        // depth == -1 means recursive
        // We never use -2 in practice, so always reconstruct some children
        if requested_depth == -1 || requested_depth >= 0 {
            let child_hashes = cached.child_hashes.clone();
            for child_hash in child_hashes {
                let child_depth = if requested_depth == -1 {
                    -1
                } else if node.node.node_type() == MerkleTreeNodeType::Dir {
                    // When reconstructing, we don't decrement here so we correctly load the VNodes
                    requested_depth
                } else {
                    requested_depth - 1
                };
                let child_recursive = requested_recursive;

                // Try to get child from cache first
                if let Some(child_cached) = cache.get(&child_hash).cloned() {
                    if child_cached.satisfies_depth(child_depth, child_recursive) {
                        let child = Self::reconstruct_from_cache(
                            repo,
                            &child_cached,
                            child_depth,
                            child_recursive,
                            cache,
                        )?;
                        node.children.push(child);
                        continue;
                    }
                }

                // Need to load child from disk
                if let Some(child) =
                    Self::read_node_cached(repo, &child_hash, child_recursive, cache)?
                {
                    node.children.push((*child).clone());
                }
            }
        }

        Ok(node)
    }

    pub fn read_node_with_hashes(
        repo: &LocalRepository,
        hash: &MerkleHash,
        hashes: &mut HashSet<MerkleHash>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // log::debug!("Read node hash [{}]", hash);
        if !MerkleNodeDB::exists(repo, hash) {
            // log::debug!("read_node merkle node db does not exist for hash: {}", hash);
            return Ok(None);
        }

        let mut node = MerkleTreeNode::from_hash(repo, hash)?;
        let mut node_db = MerkleNodeDB::open_read_only(repo, hash)?;
        CommitMerkleTree::load_children_with_hashes(repo, &mut node_db, &mut node, hashes)?;
        Ok(Some(node))
    }

    pub fn read_unique_nodes(
        repo: &LocalRepository,
        hash: &MerkleHash,
        base_hashes: &mut HashSet<MerkleHash>,
        shared_hashes: &mut HashSet<MerkleHash>,
        partial_nodes: &mut HashMap<PathBuf, PartialNode>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // log::debug!("Read node hash [{}]", hash);
        if !MerkleNodeDB::exists(repo, hash) {
            // log::debug!("read_node merkle node db does not exist for hash: {}", hash);
            return Ok(None);
        }

        let mut node = MerkleTreeNode::from_hash(repo, hash)?;
        let mut node_db = MerkleNodeDB::open_read_only(repo, hash)?;

        let start_path = PathBuf::new();

        CommitMerkleTree::load_unique_children(
            repo,
            &mut node_db,
            &mut node,
            &start_path,
            base_hashes,
            shared_hashes,
            partial_nodes,
        )?;
        Ok(Some(node))
    }

    /// Read the node at the given depth
    /// If depth is 0, we load the node and its immediate children
    pub fn read_depth(
        repo: &LocalRepository,
        hash: &MerkleHash,
        depth: i32,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        with_node_cache(repo, |cache| {
            Self::read_depth_cached(repo, hash, depth, cache)
                .map(|opt| opt.map(|arc| (*arc).clone()))
        })
    }

    fn read_depth_cached(
        repo: &LocalRepository,
        hash: &MerkleHash,
        depth: i32,
        cache: &mut LruCache<MerkleHash, CachedNode>,
    ) -> Result<Option<Arc<MerkleTreeNode>>, OxenError> {
        // Build node from cache if possible
        let cached_info = cache.get(hash).cloned();

        let mut node = if let Some(cached_node) = cached_info {
            log::debug!("Cache hit: {}", cached_node);

            // Check if cached version satisfies our requirements
            if cached_node.satisfies_depth(depth, false) {
                // Reconstruct the complete node from cache
                return Ok(Some(Arc::new(Self::reconstruct_from_cache(
                    repo,
                    &cached_node,
                    depth,
                    false,
                    cache,
                )?)));
            }

            // Need to load more children - start with cached node data
            log::debug!("Cached node exists but needs deeper loading");
            MerkleTreeNode {
                hash: cached_node.hash,
                node: cached_node.node.clone(),
                parent_id: cached_node.parent_id,
                children: Vec::new(),
            }
        } else {
            log::debug!("Cache miss for node hash [{}]", hash);
            if !MerkleNodeDB::exists(repo, hash) {
                log::debug!(
                    "read_depth merkle node db does not exist for hash: {}",
                    hash
                );
                return Ok(None);
            }
            MerkleTreeNode::from_hash(repo, hash)?
        };

        if node.is_leaf() {
            log::debug!("Node {} is a leaf node, skipping child loading", hash);
        } else {
            log::debug!("Loading children for node {}", hash);
            // Load children to requested depth
            let mut node_db = MerkleNodeDB::open_read_only(repo, hash)?;
            CommitMerkleTree::read_children_until_depth_cached(
                repo,
                &mut node_db,
                &mut node,
                depth,
                0,
                cache,
            )?;
        }

        // Cache the node
        log::debug!("caching node {} at depth {}", &node, depth);
        cache.put(*hash, CachedNode::from_tree_node(&node, depth, false));

        Ok(Some(Arc::new(node)))
    }

    /// The dir hashes allow you to skip to a directory in the tree
    pub fn dir_hashes(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<HashMap<PathBuf, MerkleHash>, OxenError> {
        let node_db_dir = CommitMerkleTree::dir_hash_db_path(repo, commit);
        log::debug!("loading dir_hashes from: {:?}", node_db_dir);
        let opts = db::key_val::opts::default();
        let node_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only(&opts, node_db_dir, false)?;
        let mut dir_hashes = HashMap::new();
        let iterator = node_db.iterator(IteratorMode::Start);
        for item in iterator {
            match item {
                Ok((key, value)) => {
                    let key = str::from_utf8(&key)?;
                    let value = str::from_utf8(&value)?;
                    let hash = MerkleHash::from_str(value)?;
                    dir_hashes.insert(PathBuf::from(key), hash);
                }
                _ => {
                    return Err(OxenError::basic_str(
                        "Could not read iterate over db values",
                    ));
                }
            }
        }
        // log::debug!(
        //     "read {} dir_hashes from commit: {}",
        //     dir_hashes.len(),
        //     commit
        // );
        Ok(dir_hashes)
    }

    pub fn read_nodes(
        repo: &LocalRepository,
        commit: &Commit,
        paths: &[PathBuf],
    ) -> Result<HashMap<PathBuf, MerkleTreeNode>, OxenError> {
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        // log::debug!(
        //     "read_nodes dir_hashes from commit: {} count: {}",
        //     commit,
        //     dir_hashes.len()
        // );
        // for (path, hash) in &dir_hashes {
        //     log::debug!("read_nodes dir_hashes path: {:?} hash: {:?}", path, hash);
        // }

        let mut nodes = HashMap::new();
        for path in paths.iter() {
            // Skip to the nodes
            let Some(hash) = dir_hashes.get(path) else {
                continue;
            };

            // log::debug!("Loading node for path: {:?} hash: {}", path, hash);
            let Some(node) = CommitMerkleTree::read_depth(repo, hash, 1)? else {
                log::warn!(
                    "Merkle tree hash not found for parent: {:?} hash: {:?}",
                    path,
                    hash
                );
                continue;
            };
            nodes.insert(path.clone(), node);
        }
        Ok(nodes)
    }

    pub fn has_dir(&self, path: impl AsRef<Path>) -> bool {
        // log::debug!("has_dir path: {:?}", path.as_ref());
        // log::debug!("has_dir dir_hashes: {:?}", self.dir_hashes);
        let path = path.as_ref();
        // println!("Path for has_dir: {path:?}");
        // println!("Dir hashes: {:?}", self.dir_hashes);
        self.dir_hashes.contains_key(path)
    }

    pub fn has_path(&self, path: impl AsRef<Path>) -> Result<bool, OxenError> {
        let path = path.as_ref();
        let node = self.root.get_by_path(path)?;
        Ok(node.is_some())
    }

    pub fn get_by_path(&self, path: impl AsRef<Path>) -> Result<Option<MerkleTreeNode>, OxenError> {
        let path = path.as_ref();
        let node = self.root.get_by_path(path)?;
        Ok(node)
    }

    pub fn get_vnodes_for_dir(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Vec<MerkleTreeNode>, OxenError> {
        let path = path.as_ref();
        let nodes = self.root.get_vnodes_for_dir(path)?;
        Ok(nodes)
    }

    pub fn list_dir_paths(&self) -> Result<Vec<PathBuf>, OxenError> {
        self.root.list_dir_paths()
    }

    pub fn files_and_folders(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<HashSet<MerkleTreeNode>, OxenError> {
        let path = path.as_ref();
        let node = self
            .root
            .get_by_path(path)?
            .ok_or(OxenError::basic_str(format!(
                "Merkle tree hash not found for parent: {:?}",
                path
            )))?;
        let mut children = HashSet::new();
        for child in &node.children {
            children.extend(child.children.iter().cloned());
        }
        Ok(children)
    }

    pub fn node_files_and_folders(node: &MerkleTreeNode) -> Result<Vec<MerkleTreeNode>, OxenError> {
        if MerkleTreeNodeType::Dir != node.node.node_type() {
            return Err(OxenError::basic_str(format!(
                "node_files_and_folders Merkle tree node is not a directory: '{:?}'",
                node.node.node_type()
            )));
        }

        // The dir node will have vnode children
        let mut children = Vec::new();
        for child in &node.children {
            if let EMerkleTreeNode::VNode(_) = &child.node {
                children.extend(child.children.iter().cloned());
            }
        }
        Ok(children)
    }

    pub fn total_vnodes(&self) -> usize {
        self.root.total_vnodes()
    }

    pub fn dir_entries(node: &MerkleTreeNode) -> Result<Vec<FileNode>, OxenError> {
        let mut file_entries = Vec::new();

        match &node.node {
            EMerkleTreeNode::Directory(_) | EMerkleTreeNode::VNode(_) => {
                for child in &node.children {
                    match &child.node {
                        EMerkleTreeNode::File(file_node) => {
                            file_entries.push(file_node.clone());
                        }
                        EMerkleTreeNode::Directory(_) | EMerkleTreeNode::VNode(_) => {
                            file_entries.extend(Self::dir_entries(child)?);
                        }
                        _ => {}
                    }
                }
                Ok(file_entries)
            }
            EMerkleTreeNode::File(file_node) => Ok(vec![file_node.clone()]),
            _ => Err(OxenError::basic_str(format!(
                "Unexpected node type: {:?}",
                node.node.node_type()
            ))),
        }
    }

    /// This uses the dir_hashes db to skip right to a file in the tree
    pub fn read_file(
        repo: &LocalRepository,
        dir_hashes: &HashMap<PathBuf, MerkleHash>,
        path: impl AsRef<Path>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // Get the directory from the path
        let path = path.as_ref();
        let Some(parent_path) = path.parent() else {
            return Ok(None);
        };
        let file_name = path.file_name().unwrap().to_str().unwrap();

        log::debug!(
            "read_file path {:?} parent_path {:?} file_name {:?}",
            path,
            parent_path,
            file_name
        );

        // Look up the directory hash
        let node_hash: Option<MerkleHash> = dir_hashes.get(parent_path).cloned();
        let Some(node_hash) = node_hash else {
            log::debug!("read_file could not find parent dir: {:?}", parent_path);
            return Ok(None);
        };

        log::debug!("read_file parent node_hash: {:?}", node_hash);

        // Read the directory node at depth 0 to get all the vnodes
        let dir_merkle_node = CommitMerkleTree::read_depth(repo, &node_hash, 0)?;
        let Some(dir_merkle_node) = dir_merkle_node else {
            return Ok(None);
        };

        let EMerkleTreeNode::Directory(dir_node) = &dir_merkle_node.node else {
            return Err(OxenError::basic_str(format!(
                "Expected directory node, got {:?}",
                dir_merkle_node.node.node_type()
            )));
        };

        // log::debug!("read_file merkle_node: {:?}", dir_merkle_node);

        let vnodes = dir_merkle_node.children;

        // log::debug!("read_file vnodes: {}", vnodes.len());

        // Calculate the total number of children in the vnodes
        // And use this to skip to the correct vnode
        // log::debug!("read_file dir_node {:?}", dir_node);
        let total_children = dir_node.num_entries();
        let vnode_size = repo.vnode_size();
        let num_vnodes = (total_children as f32 / vnode_size as f32).ceil() as u128;

        log::debug!("read_file total_children: {}", total_children);
        log::debug!("read_file vnode_size: {}", vnode_size);
        log::debug!("read_file num_vnodes: {}", num_vnodes);

        if num_vnodes == 0 {
            log::debug!("read_file num_vnodes is 0, returning None");
            return Ok(None);
        }

        // Calculate the bucket to skip to based on the path and the number of vnodes
        let path_hash = hasher::hash_buffer_128bit(path.to_str().unwrap().as_bytes());
        let bucket = path_hash % num_vnodes;

        log::debug!("read_file bucket: {}", bucket);

        // We did not load recursively, so we need to load the children for the specific vnode
        let vnode_without_children = &vnodes[bucket as usize];

        // Load the children for the vnode
        let vnode_with_children =
            CommitMerkleTree::read_depth(repo, &vnode_without_children.hash, 0)?;
        // log::debug!("read_file vnode_with_children: {:?}", vnode_with_children);
        let Some(vnode_with_children) = vnode_with_children else {
            return Ok(None);
        };

        // Get the file node from the vnode, which does a binary search under the hood
        vnode_with_children.get_by_path(file_name)
    }

    fn read_children_until_depth_cached(
        repo: &LocalRepository,
        node_db: &mut MerkleNodeDB,
        node: &mut MerkleTreeNode,
        requested_depth: i32,
        traversed_depth: i32,
        cache: &mut LruCache<MerkleHash, CachedNode>,
    ) -> Result<(), OxenError> {
        let dtype = node.node.node_type();
        // log::debug!(
        //     "read_children_until_depth requested_depth {} traversed_depth {} node {}",
        //     requested_depth,
        //     traversed_depth,
        //     node
        // );

        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
        {
            return Ok(());
        }

        let children: Vec<(MerkleHash, MerkleTreeNode)> = node_db.map()?;
        log::debug!(
            "read_children_until_depth requested_depth node {} traversed_depth {} Got {} children",
            node,
            traversed_depth,
            children.len()
        );

        for (_key, child) in children {
            // Check if child is cached and has the right depth
            if let Some(cached_child) = cache.get(&child.hash).cloned() {
                if cached_child.satisfies_depth(requested_depth - traversed_depth, false) {
                    // We don't need to go deeper, so we can use the cached version
                    log::debug!("Child node cache hit: {}", cached_child);
                    let reconstructed = Self::reconstruct_from_cache(
                        repo,
                        &cached_child,
                        requested_depth - traversed_depth,
                        false,
                        cache,
                    )?;
                    node.children.push(reconstructed);
                    continue;
                }
            }

            let mut child = child.to_owned();
            // log::debug!(
            //     "Processing child: {} (type: {:?})",
            //     child,
            //     child.node.node_type()
            // );
            // log::debug!(
            //     "read_children_until_depth {} child: {} -> {}",
            //     depth,
            //     key,
            //     child
            // );
            match &child.node.node_type() {
                // Commits, Directories, and VNodes have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    // Calculate the depth at which this child should be cached
                    let child_depth = if child.node.node_type() == MerkleTreeNodeType::VNode {
                        traversed_depth
                    } else {
                        traversed_depth + 1
                    };

                    if requested_depth >= traversed_depth || requested_depth == -1 {
                        // Here we have to not panic on error, because if we clone a subtree we might not have all of the children nodes of a particular dir
                        // given that we are only loading the nodes that are needed.
                        if let Ok(mut node_db) = MerkleNodeDB::open_read_only(repo, &child.hash) {
                            Self::read_children_until_depth_cached(
                                repo,
                                &mut node_db,
                                &mut child,
                                requested_depth,
                                child_depth,
                                cache,
                            )?;
                        }
                    }
                    // Cache the child with the correct depth
                    // The depth stored is how deeply the child's children were loaded
                    let children_loaded_depth =
                        if requested_depth > child_depth || requested_depth == -1 {
                            requested_depth - child_depth
                        } else {
                            0
                        };
                    let child_hash = child.hash;
                    cache.put(
                        child_hash,
                        CachedNode::from_tree_node(&child, children_loaded_depth, false),
                    );
                    node.children.push(child);
                }
                // FileChunks and Schemas are leaf nodes
                MerkleTreeNodeType::FileChunk | MerkleTreeNodeType::File => {
                    // Cache leaf nodes too
                    // Leaf nodes have no children, so loaded_depth is 0
                    let child_hash = child.hash;
                    cache.put(child_hash, CachedNode::from_tree_node(&child, 0, false));
                    node.children.push(child);
                }
            }
        }

        Ok(())
    }

    pub fn walk_tree(&self, f: impl FnMut(&MerkleTreeNode)) {
        self.root.walk_tree(f);
    }

    pub fn walk_tree_without_leaves(&self, f: impl FnMut(&MerkleTreeNode)) {
        self.root.walk_tree_without_leaves(f);
    }

    fn read_children_from_node_cached(
        repo: &LocalRepository,
        node_db: &mut MerkleNodeDB,
        node: &mut MerkleTreeNode,
        recurse: bool,
        cache: &mut LruCache<MerkleHash, CachedNode>,
    ) -> Result<(), OxenError> {
        let dtype = node.node.node_type();
        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
            || !recurse
        {
            return Ok(());
        }

        let children: Vec<(MerkleHash, MerkleTreeNode)> = node_db.map()?;
        // log::debug!("read_children_from_node Got {} children", children.len());

        for (_key, child) in children {
            // Check if child is cached
            if let Some(cached_child) = cache.get(&child.hash).cloned() {
                log::debug!("Child node cache hit: {}", &cached_child);
                let depth = if recurse { -1 } else { 0 };
                let reconstructed =
                    Self::reconstruct_from_cache(repo, &cached_child, depth, recurse, cache)?;
                node.children.push(reconstructed);
                continue;
            }

            let mut child = child.to_owned();
            // log::debug!("read_children_from_node child: {} -> {}", key, child);
            match &child.node.node_type() {
                // Directories, VNodes, and Commits have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    if recurse {
                        // log::debug!("read_children_from_node recurse: {:?}", child.hash);
                        let Ok(mut node_db) = MerkleNodeDB::open_read_only(repo, &child.hash)
                        else {
                            log::warn!("no child node db: {:?}", child.hash);
                            return Ok(());
                        };
                        // log::debug!("read_children_from_node opened node_db: {:?}", child.hash);
                        Self::read_children_from_node_cached(
                            repo,
                            &mut node_db,
                            &mut child,
                            recurse,
                            cache,
                        )?;
                    }
                    // Cache the child after loading its children
                    let child_hash = child.hash;
                    cache.put(child_hash, CachedNode::from_tree_node(&child, 0, recurse));
                    node.children.push(child);
                }
                // FileChunks and Schemas are leaf nodes
                MerkleTreeNodeType::FileChunk | MerkleTreeNodeType::File => {
                    // Cache leaf nodes too
                    let child_hash = child.hash;
                    cache.put(child_hash, CachedNode::from_tree_node(&child, 0, recurse));
                    node.children.push(child);
                }
            }
        }

        // log::debug!("read_children_from_node done: {:?}", node.hash);

        Ok(())
    }

    fn load_children_with_hashes(
        repo: &LocalRepository,
        node_db: &mut MerkleNodeDB,
        node: &mut MerkleTreeNode,
        hashes: &mut HashSet<MerkleHash>,
    ) -> Result<(), OxenError> {
        let dtype = node.node.node_type();
        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
        {
            return Ok(());
        }

        hashes.insert(node.hash);
        let children: Vec<(MerkleHash, MerkleTreeNode)> = node_db.map()?;
        // log::debug!("load_children_with_hashes Got {} children", children.len());

        for (_key, child) in children {
            let mut child = child.to_owned();
            // log::debug!("load_children_with_hashes child: {} -> {}", key, child);
            match &child.node.node_type() {
                // Directories, VNodes, and Commits have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    // log::debug!("load_children_with_hashes recurse: {:?}", child.hash);
                    let Ok(mut node_db) = MerkleNodeDB::open_read_only(repo, &child.hash) else {
                        log::warn!("no child node db: {:?}", child.hash);
                        return Ok(());
                    };
                    // log::debug!("load_children_with_hashes opened node_db: {:?}", child.hash);
                    CommitMerkleTree::load_children_with_hashes(
                        repo,
                        &mut node_db,
                        &mut child,
                        hashes,
                    )?;
                    node.children.push(child);
                }
                // FileChunks and Schemas are leaf nodes
                MerkleTreeNodeType::FileChunk | MerkleTreeNodeType::File => {
                    node.children.push(child);
                }
            }
        }

        // log::debug!("load_children_with_hashes done: {:?}", node.hash);

        Ok(())
    }

    fn load_unique_children(
        repo: &LocalRepository,
        node_db: &mut MerkleNodeDB,
        node: &mut MerkleTreeNode,
        current_path: &PathBuf,
        base_hashes: &mut HashSet<MerkleHash>,
        shared_hashes: &mut HashSet<MerkleHash>,
        partial_nodes: &mut HashMap<PathBuf, PartialNode>,
    ) -> Result<(), OxenError> {
        let dtype = node.node.node_type();

        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
        {
            return Ok(());
        }

        if base_hashes.contains(&node.hash) {
            shared_hashes.insert(node.hash);
            return Ok(());
        }

        let children: Vec<(MerkleHash, MerkleTreeNode)> = node_db.map()?;
        // log::debug!("load_unique_children Got {} children", children.len());
        for (_key, child) in children {
            let mut child = child.to_owned();
            // log::debug!("load_unique_children child: {} -> {}", key, child);
            match &child.node.node_type() {
                // Directories, VNodes, and Commits have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    // log::debug!("load_unique_children  recurse: {:?}", child.hash);
                    let Ok(mut node_db) = MerkleNodeDB::open_read_only(repo, &child.hash) else {
                        log::warn!("no child node db: {:?}", child.hash);
                        return Ok(());
                    };

                    let new_path = if let EMerkleTreeNode::Directory(dir_node) = &child.node {
                        let name = PathBuf::from(dir_node.name());
                        &current_path.join(name)
                    } else {
                        current_path
                    };

                    // log::debug!("load_unique_children  opened node_db: {:?}", child.hash);
                    CommitMerkleTree::load_unique_children(
                        repo,
                        &mut node_db,
                        &mut child,
                        new_path,
                        base_hashes,
                        shared_hashes,
                        partial_nodes,
                    )?;
                    node.children.push(child);
                }
                // FileChunks and Schemas are leaf nodes
                MerkleTreeNodeType::FileChunk | MerkleTreeNodeType::File => {
                    // TODO: Is this the wrong function? THe wrong check?
                    if let EMerkleTreeNode::File(file_node) = &child.node {
                        let file_path = current_path.join(PathBuf::from(file_node.name()));
                        // println!("Adding path {file_path:?} to partial_nodes");
                        let partial_node = PartialNode::from(
                            *file_node.hash(),
                            file_node.last_modified_seconds(),
                            file_node.last_modified_nanoseconds(),
                        );
                        partial_nodes.insert(file_path, partial_node);
                    }

                    node.children.push(child);
                }
            }
        }

        // log::debug!("load_unique_children " done: {:?}", node.hash);

        Ok(())
    }

    pub fn print(&self) {
        CommitMerkleTree::print_node(&self.root);
    }

    pub fn print_depth(&self, depth: i32) {
        CommitMerkleTree::print_node_depth(&self.root, depth);
    }

    pub fn print_node_depth(node: &MerkleTreeNode, depth: i32) {
        CommitMerkleTree::r_print(node, 0, depth);
    }

    pub fn print_node(node: &MerkleTreeNode) {
        // print all the way down
        CommitMerkleTree::r_print(node, 0, -1);
    }

    fn r_print(node: &MerkleTreeNode, indent: i32, depth: i32) {
        // log::debug!("r_print depth {:?} indent {:?}", depth, indent);
        // log::debug!(
        //     "r_print node dtype {:?} hash {} data.len() {} children.len() {}",
        //     node.dtype,
        //     node.hash,
        //     node.data.len(),
        //     node.children.len()
        // );
        if depth != -1 && depth > 0 && indent >= depth {
            return;
        }

        println!("{}{}", "  ".repeat(indent as usize), node);

        for child in &node.children {
            CommitMerkleTree::r_print(child, indent + 1, depth);
        }
    }
}

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use crate::core::v_latest::index::CommitMerkleTree;
    use crate::core::versions::MinOxenVersion;
    use crate::error::OxenError;
    use crate::model::merkle_tree::node::EMerkleTreeNode;
    use crate::model::MerkleTreeNodeType;
    use crate::repositories;
    use crate::test;
    use crate::test::add_n_files_m_dirs;

    #[test]
    fn test_load_dir_nodes() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::LATEST)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 10, 3)?;
            let status = repositories::status(&repo)?;
            status.print();

            // Commit the data
            let commit = repositories::commits::commit(&repo, "First commit")?;

            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;
            tree.print();

            /*
            The tree will look something like this

            [Commit] d9fc5c49451ad18335f9f8c1e1c8ac0b -> First commit parent_ids ""
                [Dir]  -> 172861146a4a0f5f0250f117ce93ef1e 60 B (1 nodes) (10 files)
                    [VNode] 3a5d6d3bdc8bf1f3fddcabaa3afcd821 (3 children)
                    [File] README.md -> beb36f69f0b6efd87dbe3bb3dcea661c 18 B
                    [Dir] files -> aefe7cf4ad104b759e46c13cb304ba16 60 B (1 nodes) (10 files)
                        [VNode] affcd15c283c42524ee3f2dc300b90fe (3 children)
                        [Dir] dir_0 -> ee97a66ee8498caa67605c50e9b24275 0 B (1 nodes) (0 files)
                            [VNode] 1756daa4caa26d51431b925250529838 (4 children)
                            [File] file0.txt -> 82d44cc82d2c1c957aeecb14293fb5ec 6 B
                            [File] file3.txt -> 9c8fe1177e78b0fe5ec104db52b5e449 6 B
                            [File] file6.txt -> 3cba14134797f8c246ee520c808817b4 6 B
                            [File] file9.txt -> ab8e4cdc8e9df49fb8d7bc1940df811f 6 B
                        [Dir] dir_1 -> 24467f616e4fba7beacb18b71b87652d 0 B (1 nodes) (0 files)
                            [VNode] 382eb89abe00193ed680c6a541f4b0c4 (3 children)
                            [File] file1.txt -> aab67365636cc292a767ad9e48ca6e1f 6 B
                            [File] file4.txt -> f8d4169182a41cc63bb7ed8fc36de960 6 B
                            [File] file7.txt -> b0335dcbf55c6c08471d8ebefbbf5de9 6 B
                        [Dir] dir_2 -> 7e2fbcd5b9e62847e1aaffd7e9d1aa8 0 B (1 nodes) (0 files)
                            [VNode] b87cfea40ada7cc374833ab2eca4636d (3 children)
                            [File] file2.txt -> 2101009797546bf98de2b0bbcbd59f0 6 B
                            [File] file5.txt -> 253badb52f99edddf74d1261b8c5f03a 6 B
                            [File] file8.txt -> 13fa116ba84c615eda1759b5e6ae5d6e 6 B
                    [File] files.csv -> 152b60b41558d5bfe80b7e451de7b276 151 B
            */

            // Make sure we have written the dir_hashes db
            let dir_hashes = CommitMerkleTree::dir_hashes(&repo, &commit)?;

            println!("Got {} dir_hashes", dir_hashes.len());
            for (key, value) in &dir_hashes {
                println!("dir: {:?} hash: {}", key, value);
            }

            // Should have ["", "files", "files/dir_0", "files/dir_1", "files/dir_2"]
            assert_eq!(dir_hashes.len(), 5);
            assert!(dir_hashes.contains_key(&PathBuf::from("")));
            assert!(dir_hashes.contains_key(&PathBuf::from("files")));
            assert!(dir_hashes.contains_key(&PathBuf::from("files/dir_0")));
            assert!(dir_hashes.contains_key(&PathBuf::from("files/dir_1")));
            assert!(dir_hashes.contains_key(&PathBuf::from("files/dir_2")));

            // Only load the root and files/dir_1
            let paths_to_load: Vec<PathBuf> =
                vec![PathBuf::from(""), PathBuf::from("files").join("dir_1")];
            let loaded_nodes = CommitMerkleTree::read_nodes(&repo, &commit, &paths_to_load)?;

            println!("loaded {} nodes", loaded_nodes.len());
            for (_, node) in loaded_nodes {
                println!("node: {}", node);
                CommitMerkleTree::print_node_depth(&node, 1);
                assert!(node.node.node_type() == MerkleTreeNodeType::Dir);
                assert!(node.parent_id.is_some());
                assert!(!node.children.is_empty());
                let dir = node.dir().unwrap();
                assert!(dir.num_files() > 0);
                assert!(dir.num_entries() > 0);
            }

            Ok(())
        })
    }

    #[test]
    fn test_node_cache_performance() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::LATEST)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 1000, 10)?;
            let _status = repositories::status(&repo)?;

            // Commit the data
            let commit = repositories::commits::commit(&repo, "First commit")?;

            // Clear the cache to start fresh
            crate::core::v_latest::index::commit_merkle_tree::remove_node_cache(&repo.path)?;

            // Load the tree multiple times
            use std::time::Instant;

            // First load (cache miss)
            let start = Instant::now();
            let tree1 = CommitMerkleTree::from_commit(&repo, &commit)?;
            let first_load_time = start.elapsed();
            println!("First load time: {:?}", first_load_time);

            // Count nodes
            let mut node_count = 0;
            tree1.walk_tree(|_node| {
                node_count += 1;
            });
            println!("Total nodes in tree: {}", node_count);

            // Second load (should have cache hits)
            let start = Instant::now();
            let tree2 = CommitMerkleTree::from_commit(&repo, &commit)?;
            let second_load_time = start.elapsed();
            println!("Second load time: {:?}", second_load_time);

            // Third load (should be fully cached)
            let start = Instant::now();
            let tree3 = CommitMerkleTree::from_commit(&repo, &commit)?;
            let third_load_time = start.elapsed();
            println!("Third load time: {:?}", third_load_time);

            // The cached loads should be faster
            // Note: This might not always be true in CI environments, so we're being lenient
            println!(
                "Speed improvement: {:.2}x faster",
                first_load_time.as_nanos() as f64 / third_load_time.as_nanos().max(1) as f64
            );

            // Verify all trees are identical
            assert_eq!(tree1.root.hash, tree2.root.hash);
            assert_eq!(tree2.root.hash, tree3.root.hash);

            Ok(())
        })
    }

    #[test]
    fn test_node_cache_shared_nodes() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::LATEST)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 10, 3)?;
            let _status = repositories::status(&repo)?;

            // Commit the data
            let commit = repositories::commits::commit(&repo, "First commit")?;

            // Clear the cache to start fresh
            crate::core::v_latest::index::commit_merkle_tree::remove_node_cache(&repo.path)?;

            // First, load the root directory WITHOUT children (depth 0)
            println!("Loading root directory without children...");
            let root_no_children = CommitMerkleTree::root_without_children(&repo, &commit)?
                .expect("Root should exist");

            println!("Root loaded without children: {}", root_no_children);
            println!("Number of children: {}", root_no_children.children.len());

            // Verify it has children (vnodes) but not grandchildren
            assert!(
                !root_no_children.children.is_empty(),
                "Root should have VNode children"
            );
            for child in &root_no_children.children {
                if matches!(child.node, EMerkleTreeNode::VNode(_)) {
                    assert!(
                        child.children.is_empty(),
                        "VNodes should not have children loaded"
                    );
                }
            }

            // Now load the root WITH children (recursive)
            println!("\nLoading root directory with children (recursive)...");
            let root_with_children =
                CommitMerkleTree::root_with_children(&repo, &commit)?.expect("Root should exist");

            println!("Root loaded with children: {}", root_with_children);

            // Verify the root hashes match
            assert_eq!(
                root_no_children.hash, root_with_children.hash,
                "Root nodes should have matching hashes"
            );

            // Verify the full tree now has grandchildren loaded
            let mut has_grandchildren = false;
            for child in &root_with_children.children {
                println!("Child: {}", child);
                println!("  Node type: {:?}", child.node.node_type());
                println!("  Number of children: {}", child.children.len());

                if matches!(child.node, EMerkleTreeNode::VNode(_)) {
                    if !child.children.is_empty() {
                        has_grandchildren = true;
                        println!("VNode has {} children", child.children.len());
                    }
                } else if !child.children.is_empty() {
                    has_grandchildren = true;
                    println!(
                        "Non-VNode {:?} has {} children",
                        child.node.node_type(),
                        child.children.len()
                    );
                }
            }

            // Also check if the tree is structured differently than expected
            if !has_grandchildren && root_with_children.children.len() == 1 {
                // Check if we have a commit -> dir structure
                if let Some(first_child) = root_with_children.children.first() {
                    println!("Checking first child for grandchildren...");
                    for grandchild in &first_child.children {
                        println!("  Grandchild: {}", grandchild);
                        if !grandchild.children.is_empty() {
                            has_grandchildren = true;
                        }
                    }
                }
            }

            assert!(
                has_grandchildren,
                "Root with children should have grandchildren loaded"
            );

            // Load a subdirectory without children first
            let subdir_path = PathBuf::from("files");
            println!(
                "\nLoading subdirectory '{}' without children...",
                subdir_path.display()
            );
            let subdir_no_children =
                CommitMerkleTree::dir_without_children(&repo, &commit, &subdir_path)?
                    .expect("Subdirectory should exist");

            println!("Subdirectory loaded: {}", subdir_no_children);
            println!("Number of children: {}", subdir_no_children.children.len());
            assert_eq!(
                subdir_no_children.children.len(),
                0,
                "Should have no children loaded"
            );

            // Now load the same subdirectory WITH children
            println!(
                "\nLoading subdirectory '{}' with children...",
                subdir_path.display()
            );
            let subdir_with_children =
                CommitMerkleTree::dir_with_children(&repo, &commit, &subdir_path)?
                    .expect("Subdirectory should exist");

            println!(
                "Subdirectory loaded with children: {}",
                subdir_with_children
            );
            println!(
                "Number of children: {}",
                subdir_with_children.children.len()
            );

            // Verify hashes match but children are different
            assert_eq!(
                subdir_no_children.hash, subdir_with_children.hash,
                "Subdirectory nodes should have matching hashes"
            );
            assert!(
                !subdir_with_children.children.is_empty(),
                "Subdirectory with children should have children loaded"
            );

            println!("\nCache depth-aware loading is working correctly!");

            Ok(())
        })
    }

    #[test]
    fn test_read_leaf_node_directly() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::LATEST)?;

            // Write a simple file
            let file_path = repo.path.join("test.txt");
            std::fs::write(&file_path, "test content")?;

            // Add and commit
            repositories::add(&repo, &file_path)?;
            let commit = repositories::commits::commit(&repo, "Test commit")?;

            // Load the tree to get the file node
            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;

            // Find the file node in the tree
            let mut file_hash = None;
            tree.walk_tree(|node| {
                if let EMerkleTreeNode::File(file_node) = &node.node {
                    if file_node.name() == "test.txt" {
                        file_hash = Some(node.hash);
                        println!("Found file node: {}", node);
                        println!("File hash: {}", node.hash);
                    }
                }
            });

            let file_hash = file_hash.expect("Should find test.txt in tree");

            // Clear the cache to ensure we're testing disk loading
            crate::core::v_latest::index::commit_merkle_tree::remove_node_cache(&repo.path)?;

            // Try to read the file node directly - this should fail or return None
            // because file nodes don't have their own database files
            println!("\nTrying to read file node directly...");
            let result = CommitMerkleTree::read_node(&repo, &file_hash, false);

            match result {
                Ok(None) => {
                    println!("Correctly returned None for leaf node");
                    // This is expected - leaf nodes don't have their own DB files
                }
                Ok(Some(node)) => {
                    println!("Unexpectedly loaded node: {}", node);
                    // This would indicate the node was cached or loaded somehow
                    panic!("Should not be able to load leaf node directly from disk");
                }
                Err(e) => {
                    println!("Got error trying to load leaf node: {}", e);
                    // This is also acceptable - indicates the DB doesn't exist
                }
            }

            // Now demonstrate the correct way: load the parent and access the file
            println!("\nLoading parent directory to access file...");
            let root_with_children =
                CommitMerkleTree::root_with_children(&repo, &commit)?.expect("Root should exist");

            // Find the file node through its parent
            let mut found_file = false;
            root_with_children.walk_tree(|node| {
                if let EMerkleTreeNode::File(file_node) = &node.node {
                    if file_node.name() == "test.txt" {
                        found_file = true;
                        println!("Found file through parent: {}", node);
                    }
                }
            });

            assert!(found_file, "Should find file when loading through parent");

            Ok(())
        })
    }

    #[test]
    fn test_cache_leaf_node_handling() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::LATEST)?;

            // Create a directory structure with files
            let subdir = repo.path.join("subdir");
            std::fs::create_dir(&subdir)?;
            std::fs::write(repo.path.join("file1.txt"), "content1")?;
            std::fs::write(subdir.join("file2.txt"), "content2")?;

            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commits::commit(&repo, "Test commit")?;

            // Clear cache to ensure clean test
            crate::core::v_latest::index::commit_merkle_tree::remove_node_cache(&repo.path)?;

            // Load the tree recursively - this should trigger caching of all nodes
            println!("Loading tree recursively...");
            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;

            // Find a file node hash
            let mut file_hash = None;
            tree.walk_tree(|node| {
                if let EMerkleTreeNode::File(file_node) = &node.node {
                    if file_node.name() == "file1.txt" {
                        file_hash = Some(node.hash);
                        println!(
                            "Found file node: {} with hash {}",
                            file_node.name(),
                            node.hash
                        );
                    }
                }
            });
            let file_hash = file_hash.expect("Should find file1.txt");

            // Now try to load just this file node from cache
            // This should work because it's cached, even though it doesn't have a DB
            println!("\nTrying to read cached file node...");
            let cached_result = CommitMerkleTree::read_node(&repo, &file_hash, true)?;

            if let Some(node) = cached_result {
                println!("Successfully loaded file node from cache: {}", node);
                assert!(matches!(node.node, EMerkleTreeNode::File(_)));
            } else {
                panic!("Failed to load file node from cache");
            }

            // Clear cache and try again - should fail
            println!("\nClearing cache and trying again...");
            crate::core::v_latest::index::commit_merkle_tree::remove_node_cache(&repo.path)?;

            let uncached_result = CommitMerkleTree::read_node(&repo, &file_hash, false)?;
            assert!(
                uncached_result.is_none(),
                "Should not be able to load uncached file node"
            );

            // Test that we don't try to open DB for cached leaf nodes when reconstructing
            println!("\nTesting reconstruction with leaf nodes...");

            // // First, load a directory with its files
            // let _subdir_tree = CommitMerkleTree::from_path_recursive(&repo, &commit, "subdir")?;

            // Now clear cache and load the directory without children
            crate::core::v_latest::index::commit_merkle_tree::remove_node_cache(&repo.path)?;

            let _dir_with_children = CommitMerkleTree::dir_with_children(&repo, &commit, "")?;
            println!("loaded dir with children (depth 1)");

            // Load with children - this should use cache and not try to open DBs for files
            let dir_with_children_recursive =
                CommitMerkleTree::dir_with_children_recursive(&repo, &commit, "")?;
            assert!(dir_with_children_recursive.is_some());

            // Verify the structure
            let dir_node = dir_with_children_recursive.unwrap();
            CommitMerkleTree::print_node_depth(&dir_node, 2);

            // Count file nodes
            let mut file_count = 0;
            dir_node.walk_tree(|node| {
                if matches!(node.node, EMerkleTreeNode::File(_)) {
                    file_count += 1;
                }
            });
            assert!(file_count > 0, "Should have found file nodes");

            Ok(())
        })
    }

    #[test]
    fn test_cache_reconstruct_with_leaf_nodes() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::LATEST)?;

            // Create a simple directory structure for easier testing
            let dir1 = repo.path.join("dir1");
            std::fs::create_dir(&dir1)?;

            // Add files at different levels
            std::fs::write(repo.path.join("root.txt"), "root file")?;
            std::fs::write(dir1.join("file1.txt"), "file in dir1")?;

            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commits::commit(&repo, "Test commit")?;

            // Clear cache
            crate::core::v_latest::index::commit_merkle_tree::remove_node_cache(&repo.path)?;

            // Step 1: Load at depth 0 (just the root, no children)
            println!("Step 1: Loading tree at depth 0...");
            let tree_depth0 = CommitMerkleTree::from_path_depth(&repo, &commit, "", 0)?
                .expect("Should load root");
            println!("Tree at depth 0:");
            CommitMerkleTree::print_node(&tree_depth0);

            // At depth 0, root has VNode but VNode should have no children
            assert_eq!(
                tree_depth0.children.len(),
                1,
                "Root should have 1 VNode child at depth 0"
            );
            let vnode = &tree_depth0.children[0];
            assert!(
                matches!(vnode.node, EMerkleTreeNode::VNode(_)),
                "Child should be VNode"
            );
            assert_eq!(
                vnode.children.len(),
                0,
                "VNode should have no children at depth 0"
            );

            // Step 2: Load at depth 1 (root + immediate children including VNodes)
            println!("\nStep 2: Loading tree at depth 1...");
            let tree_depth1 = CommitMerkleTree::from_path_depth(&repo, &commit, "", 1)?
                .expect("Should load root");
            println!("Tree at depth 1:");
            CommitMerkleTree::print_node(&tree_depth1);

            // At depth 1, we should see root's VNode and its contents (root.txt and dir1)
            let mut found_root_txt = false;
            let mut found_dir1 = false;
            let mut found_file1_txt = false;

            tree_depth1.walk_tree(|node| {
                if let EMerkleTreeNode::File(file) = &node.node {
                    if file.name() == "root.txt" {
                        found_root_txt = true;
                    } else if file.name() == "file1.txt" {
                        found_file1_txt = true;
                    }
                }
                if let EMerkleTreeNode::Directory(dir) = &node.node {
                    if dir.name() == "dir1" {
                        found_dir1 = true;
                        // At depth 1, dir1 has VNode but VNode has no children
                        assert_eq!(
                            node.children.len(),
                            1,
                            "dir1 should have 1 VNode child at depth 1"
                        );
                        let dir_vnode = &node.children[0];
                        assert!(matches!(dir_vnode.node, EMerkleTreeNode::VNode(_)));
                        assert_eq!(
                            dir_vnode.children.len(),
                            0,
                            "dir1's VNode should have no children at depth 1"
                        );
                    }
                }
            });

            assert!(found_root_txt, "Should find root.txt at depth 1");
            assert!(found_dir1, "Should find dir1 at depth 1");
            assert!(!found_file1_txt, "Should NOT find file1.txt at depth 1");

            // Step 3: Test depth 2 - should show file1.txt since VNodes don't count
            println!("\nStep 3: Loading tree at depth 2...");
            let tree_depth2 = CommitMerkleTree::from_path_depth(&repo, &commit, "", 2)?
                .expect("Should load root");
            println!("Tree at depth 2:");
            CommitMerkleTree::print_node(&tree_depth2);

            // At depth 2, we should find file1.txt
            let mut found_file1_txt_d2 = false;
            tree_depth2.walk_tree(|node| {
                if let EMerkleTreeNode::File(file) = &node.node {
                    println!("Found file at depth 2: {}", file.name());
                    if file.name() == "file1.txt" {
                        found_file1_txt_d2 = true;
                    }
                }
            });

            // Let's also check what's in dir1's vnode
            tree_depth2.walk_tree(|node| {
                if let EMerkleTreeNode::Directory(dir) = &node.node {
                    if dir.name() == "dir1" {
                        println!("dir1 has {} children", node.children.len());
                        for (i, child) in node.children.iter().enumerate() {
                            println!(
                                "  Child {}: {:?} with {} children",
                                i,
                                child.node.node_type(),
                                child.children.len()
                            );
                        }
                    }
                }
            });

            // Now it should work correctly - file1.txt should be visible at depth 2
            assert!(
                found_file1_txt_d2,
                "Should find file1.txt at depth 2 (directories traversed: root -> dir1)"
            );

            // Step 4: Test caching - clear cache and load a file node
            println!("\nStep 4: Testing cached file node access...");
            crate::core::v_latest::index::commit_merkle_tree::remove_node_cache(&repo.path)?;

            // Load dir1 with its children
            let dir1_tree = CommitMerkleTree::from_path_depth(&repo, &commit, "dir1", 1)?
                .expect("Should load dir1");

            // Find file1.txt hash
            let mut file_hash = None;
            dir1_tree.walk_tree(|node| {
                if let EMerkleTreeNode::File(file) = &node.node {
                    if file.name() == "file1.txt" {
                        file_hash = Some(node.hash);
                    }
                }
            });
            let file_hash = file_hash.expect("Should find file1.txt");

            // Try to read this file node directly - should be in cache
            let cached_file = CommitMerkleTree::read_node(&repo, &file_hash, false)?;
            assert!(
                cached_file.is_some(),
                "File should be in cache after parent was loaded"
            );

            // Clear cache and try again - should fail
            crate::core::v_latest::index::commit_merkle_tree::remove_node_cache(&repo.path)?;
            let uncached_file = CommitMerkleTree::read_node(&repo, &file_hash, false)?;
            assert!(
                uncached_file.is_none(),
                "File should not be loadable when not cached"
            );

            Ok(())
        })
    }
}
