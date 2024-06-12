use serde::Deserialize;

#[derive(Deserialize)]
pub struct GetDataFrameOptsQuery {
    pub page: Option<usize>,
    pub page_size: Option<usize>,
    pub editable: Option<bool>,
}
