use anyhow::Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TsoError {
    #[error("ErrEtcdTxnConflict")]
    EtcdTxnConflict,
    #[error("ErrEtcdTxnInternal, cause: {0}")]
    EtcdTxnInternal(Error),
    #[error("EtcdGrantLease, cause: {0}")]
    EtcdGrantLease(Error),
    #[error("ErrCheckCampaign")]
    CheckCampaign,
    #[error("ErrBytesToU64")]
    BytesToU64,
    #[error("TaskCancel")]
    TaskCancel,
    #[error("TaskTimeout")]
    TaskTimeout,
}
