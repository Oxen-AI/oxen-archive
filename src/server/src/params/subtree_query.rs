use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct SubtreeQuery {
    pub subtrees: Option<String>,
}
