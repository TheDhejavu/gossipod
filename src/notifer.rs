use anyhow::{anyhow, Result};
use tokio::sync::{mpsc, RwLock};

pub(crate) struct NotifierInner {
    channel_tx: mpsc::Sender<Vec<u8>>,
}

pub(crate) struct Notifier {
    inner: RwLock<Option<NotifierInner>>,
}

impl Notifier {
    pub(crate) fn new() -> Self {
        Self {
            inner: RwLock::new(None),
        }
    }

    pub(crate) async fn with_receiver(&self, buffer: usize) -> mpsc::Receiver<Vec<u8>> {
        let (channel_tx, channel_rx) = mpsc::channel(buffer);
        let mut inner = self.inner.write().await;
        *inner = Some(NotifierInner { channel_tx });
        channel_rx
    }

    pub(crate) async fn notify(&self, msg: Vec<u8>) -> Result<()> {
        let inner = self.inner.read().await;
        if let Some(channel) = &*inner {
            channel.channel_tx.send(msg).await?;
            Ok(())
        } else {
            Err(anyhow!("notfier is not initialized"))
        }
    }

    pub(crate) async fn is_initialized(&self) -> bool {
        self.inner.read().await.is_some()
    }
}

