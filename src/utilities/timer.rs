use tokio::time::{sleep, Duration};
use std::sync::Arc;

pub struct Timer {
    duration: Duration,
    callback: Arc<dyn Fn() + Send + Sync>,
    is_running: Arc<std::sync::atomic::AtomicBool>,
}

impl Timer {
    pub fn new<F>(duration: Duration, callback: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        Timer {
            duration,
            callback: Arc::new(callback),
            is_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub async fn start(&self) {
        if self.is_running.load(std::sync::atomic::Ordering::SeqCst) {
            println!("Timer is already running.");
            return;
        }
        self.is_running.store(true, std::sync::atomic::Ordering::SeqCst);

        let callback = Arc::clone(&self.callback);
        let duration = self.duration;

        tokio::spawn(async move {
            sleep(duration).await;
            callback();
        });

        println!("Timer started for {:?}", duration);
    }

    pub async fn stop(&self) {
        if !self.is_running.load(std::sync::atomic::Ordering::SeqCst) {
            println!("Timer is not running.");
            return;
        }
        self.is_running.store(false, std::sync::atomic::Ordering::SeqCst);
        println!("Timer stopped.");
    }

    pub async fn restart(&self) {
        println!("Restarting the timer...");
        self.stop().await;
        self.start().await;
    }
}

#[tokio::main]
async fn main() {
    let timer = Timer::new(Duration::from_secs(5), || {
        println!("Timer completed!");
    });

    timer.start().await;

    // Simulate waiting before stopping
    tokio::time::sleep(Duration::from_secs(2)).await;
    timer.stop().await;

    // Restart the timer
    timer.restart().await;

    // Allow time for the timer to finish
    tokio::time::sleep(Duration::from_secs(10)).await;
}
