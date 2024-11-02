use crate::util::constant::Constant;

pub struct KeyPath;

impl KeyPath {
    pub fn root_path(cluster_id: u64) -> String {
        format!("{}/{}", Constant::ROOT_PATH, cluster_id)
    }
}
