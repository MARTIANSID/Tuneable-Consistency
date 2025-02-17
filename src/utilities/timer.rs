use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Duration};

pub struct Timer {
    duration: Duration,
    cancel_tx: Option<oneshot::Sender<()>>,
    expired_tx: mpsc::Sender<()>,
}

impl Timer {
    pub fn new(duration: Duration, expired_tx: mpsc::Sender<()>) -> Self {
        Timer {
            duration,
            cancel_tx: None,
            expired_tx,
        }
    }

    pub fn start(&mut self) {
        self.stop();

        let (cancel_tx, cancel_rx) = oneshot::channel();
        let expired_tx = self.expired_tx.clone();
        let duration = self.duration;

        tokio::spawn(async move {
            tokio::select! {
                _ = sleep(duration) => {
                    let _ = expired_tx.send(()).await;
                }
                _ = cancel_rx => {
                }
            }
        });

        self.cancel_tx = Some(cancel_tx);
    }

    pub fn stop(&mut self) {
        if let Some(cancel_tx) = self.cancel_tx.take() {
            let _ = cancel_tx.send(());
        }
    }

    pub fn restart(&mut self) {
        self.stop();
        self.start();
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        self.stop();
    }
}