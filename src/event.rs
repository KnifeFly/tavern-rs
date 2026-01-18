use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, OnceLock};

pub type Kind = String;

pub struct EventContext;

pub struct TopicKey<T> {
    name: Kind,
    _marker: PhantomData<T>,
}

impl<T> TopicKey<T> {
    pub fn name(&self) -> &str {
        &self.name
    }
}

pub fn new_topic_key<T>(name: impl Into<Kind>) -> TopicKey<T> {
    TopicKey {
        name: name.into(),
        _marker: PhantomData,
    }
}

type Listener = Arc<dyn Fn(&EventContext, &dyn Any) + Send + Sync>;

fn registry() -> &'static Mutex<HashMap<Kind, Vec<Listener>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<Kind, Vec<Listener>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn new_publisher<T: Any + Send + Sync + 'static>(
    topic: &TopicKey<T>,
) -> impl Fn(&EventContext, T) + Send + Sync + 'static {
    let name = topic.name.clone();
    {
        let mut map = registry().lock().expect("event registry");
        map.entry(name.clone()).or_default();
    }
    move |ctx, payload| publish_by_name(&name, ctx, &payload)
}

pub fn subscribe<T, F>(topic: &TopicKey<T>, handler: F) -> anyhow::Result<()>
where
    T: Any + Send + Sync + 'static,
    F: Fn(&EventContext, &T) + Send + Sync + 'static,
{
    let name = topic.name.clone();
    let mut map = registry().lock().expect("event registry");
    let listeners = map.get_mut(&name).ok_or_else(|| anyhow::anyhow!("topic {} not found", name))?;
    let listener: Listener = Arc::new(move |ctx, payload| {
        if let Some(typed) = payload.downcast_ref::<T>() {
            handler(ctx, typed);
        }
    });
    listeners.push(listener);
    Ok(())
}

pub fn publish<T: Any + Send + Sync + 'static>(topic: &TopicKey<T>, ctx: &EventContext, payload: T) {
    publish_by_name(&topic.name, ctx, &payload);
}

fn publish_by_name(name: &str, ctx: &EventContext, payload: &dyn Any) {
    let listeners = {
        let map = registry().lock().expect("event registry");
        map.get(name).cloned()
    };
    if let Some(listeners) = listeners {
        for listener in listeners {
            listener(ctx, payload);
        }
    }
}

pub const CACHE_COMPLETED_KEY: &str = "cache.completed";

#[derive(Clone, Debug)]
pub struct CacheCompletedPayload {
    pub store_url: String,
    pub store_key: String,
    pub store_path: String,
    pub content_length: i64,
    pub last_modified: String,
    pub chunk_count: usize,
    pub chunk_size: u64,
    pub report_ratio: i32,
}

impl CacheCompletedPayload {
    pub fn report_ratio(&self) -> i32 {
        self.report_ratio
    }
}
