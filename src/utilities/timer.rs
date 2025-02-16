use tokio::time::{sleep, Duration};
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

pub struct Timer<F>
where
    F: Fn() + Send + 'static,
{
    duration: Duration,
    callback: F,
    handle: Option<JoinHandle<()>>,
    is_running: Arc<Mutex<bool>>,
}

impl<F> Timer<F>
where
    F: Fn() + Send + 'static,
{
    pub fn new(duration: Duration, callback: F) -> Self {
        Self {
            duration,
            callback,
            handle: None,
            is_running: Arc::new(Mutex::new(false)),
        }
    }

    pub fn start(&mut self) {
        let is_running = Arc::clone(&self.is_running);
        let callback = &self.callback;
        let duration = self.duration;
        *is_running.lock().unwrap() = true;

        self.handle = Some(tokio::spawn(async move {
            sleep(duration).await;
            if *is_running.lock().unwrap() {
                callback();
            }
        }));
    }

    pub fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        *self.is_running.lock().unwrap() = false;
    }

    pub fn restart(&mut self) {
        self.stop();
        self.start();
    }
}

// Example Usage:
#[tokio::main]
async fn main() {
    let mut timer = Timer::new(
        Duration::from_secs(5),
        || println!("Timer completed!"),
    );

    timer.start();
    sleep(Duration::from_secs(2)).await;
    println!("Restarting timer...");
    timer.restart();
    sleep(Duration::from_secs(6)).await;
    println!("Stopping timer...");
    timer.stop();
}
