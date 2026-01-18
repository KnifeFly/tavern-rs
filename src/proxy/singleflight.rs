use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, Notify};

pub struct Group<T> {
    inner: Arc<Mutex<HashMap<String, Arc<Call<T>>>>>,
}

impl<T: Clone + Send + Sync + 'static> Group<T> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn do_call<F, Fut>(&self, key: String, f: F) -> T
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = T> + Send + 'static,
    {
        let call = {
            let mut map = self.inner.lock().await;
            if let Some(call) = map.get(&key) {
                call.clone()
            } else {
                let call = Arc::new(Call::new());
                map.insert(key.clone(), call.clone());
                tokio::spawn(run_call(self.inner.clone(), key.clone(), call.clone(), f));
                call
            }
        };

        call.wait().await
    }
}

struct Call<T> {
    notify: Notify,
    result: Mutex<Option<T>>,
}

impl<T: Clone + Send + Sync + 'static> Call<T> {
    fn new() -> Self {
        Self {
            notify: Notify::new(),
            result: Mutex::new(None),
        }
    }

    async fn wait(&self) -> T {
        loop {
            if let Some(val) = self.result.lock().await.clone() {
                return val;
            }
            self.notify.notified().await;
        }
    }

    async fn set(&self, val: T) {
        let mut slot = self.result.lock().await;
        *slot = Some(val);
        self.notify.notify_waiters();
    }
}

async fn run_call<T, F, Fut>(
    map: Arc<Mutex<HashMap<String, Arc<Call<T>>>>>,
    key: String,
    call: Arc<Call<T>>,
    f: F,
) where
    T: Clone + Send + Sync + 'static,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let result = f().await;
    call.set(result).await;
    let mut map = map.lock().await;
    map.remove(&key);
}
