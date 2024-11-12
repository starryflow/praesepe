use std::fmt::Debug;

pub struct EtcdShimDatabase;

impl Debug for EtcdShimDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("EtcdShimDatabase",))
    }
}
