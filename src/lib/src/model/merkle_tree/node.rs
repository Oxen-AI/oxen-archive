use std::fmt;

pub mod commit_node;
pub mod dir_node;
pub mod dir_node_with_path;
pub mod file_chunk_node;
pub mod file_node;
pub mod file_node_types;
pub mod file_node_with_dir;
pub mod merkle_tree_node;
pub mod staged_merkle_tree_node;
pub mod vnode;

pub use commit_node::CommitNode;
pub use dir_node::DirNode;
pub use dir_node_with_path::DirNodeWithPath;
pub use file_chunk_node::FileChunkNode;
pub use file_node::FileNode;
pub use file_node_types::{FileChunkType, FileStorageType};
pub use file_node_with_dir::FileNodeWithDir;
pub use merkle_tree_node::MerkleTreeNode;
pub use staged_merkle_tree_node::StagedMerkleTreeNode;
pub use vnode::VNode;

use crate::model::metadata::generic_metadata::GenericMetadata;
pub use crate::model::{MerkleTreeNodeType, TMerkleTreeNode};
use serde::{Deserialize, Serialize};

use super::MerkleHash;

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize)]
pub enum EMerkleTreeNode {
    File(FileNode),
    Directory(DirNode),
    VNode(VNode),
    FileChunk(FileChunkNode),
    Commit(CommitNode),
}

impl EMerkleTreeNode {
    pub fn node_type(&self) -> MerkleTreeNodeType {
        match self {
            EMerkleTreeNode::File(_) => MerkleTreeNodeType::File,
            EMerkleTreeNode::Directory(_) => MerkleTreeNodeType::Dir,
            EMerkleTreeNode::VNode(_) => MerkleTreeNodeType::VNode,
            EMerkleTreeNode::FileChunk(_) => MerkleTreeNodeType::FileChunk,
            EMerkleTreeNode::Commit(_) => MerkleTreeNodeType::Commit,
        }
    }

    pub fn hash(&self) -> &MerkleHash {
        match self {
            EMerkleTreeNode::File(file) => file.hash(),
            EMerkleTreeNode::Directory(dir) => dir.hash(),
            EMerkleTreeNode::VNode(vnode) => vnode.hash(),
            EMerkleTreeNode::FileChunk(file_chunk) => &file_chunk.hash,
            EMerkleTreeNode::Commit(commit) => commit.hash(),
        }
    }

    pub fn metadata(&self) -> Option<GenericMetadata> {
        match self {
            EMerkleTreeNode::File(file) => file.metadata(),
            _ => None,
        }
    }

    pub fn is_leaf(&self) -> bool {
        matches!(
            &self,
            EMerkleTreeNode::File(_) | EMerkleTreeNode::FileChunk(_)
        )
    }
}

impl fmt::Display for EMerkleTreeNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            EMerkleTreeNode::Commit(commit) => {
                write!(f, "[{:?}] {} {}", self.node_type(), self.hash(), commit)
            }
            EMerkleTreeNode::VNode(vnode) => {
                write!(
                    f,
                    "[{:?}] {} {} ({} entries)",
                    self.node_type(),
                    self.hash().to_short_str(),
                    vnode,
                    vnode.num_entries()
                )
            }
            EMerkleTreeNode::Directory(dir) => {
                write!(
                    f,
                    "[{:?}] {} {} ({} entries)",
                    self.node_type(),
                    self.hash().to_short_str(),
                    dir,
                    dir.num_entries()
                )
            }
            EMerkleTreeNode::File(file) => {
                write!(
                    f,
                    "[{:?}] {} {}",
                    self.node_type(),
                    self.hash().to_short_str(),
                    file
                )
            }
            EMerkleTreeNode::FileChunk(file_chunk) => {
                write!(
                    f,
                    "[{:?}] {} {}",
                    self.node_type(),
                    self.hash().to_short_str(),
                    file_chunk
                )
            }
        }
    }
}
