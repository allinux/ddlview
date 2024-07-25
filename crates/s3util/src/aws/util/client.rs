use once_cell::sync::Lazy;
use rayon::ThreadPool;
use tokio::{
    self,
    runtime::{Builder, Runtime},
};

pub static BUILDER: Lazy<Runtime> =
    Lazy::new(|| Builder::new_multi_thread().enable_all().build().unwrap());

pub static POOL: Lazy<ThreadPool> = Lazy::new(|| {
    rayon::ThreadPoolBuilder::new()
        .num_threads(
            std::thread::available_parallelism()
                .unwrap_or(std::num::NonZeroUsize::new(1).unwrap())
                .get(),
        )
        .build()
        .unwrap()
});
