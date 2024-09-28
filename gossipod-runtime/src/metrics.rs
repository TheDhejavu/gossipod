use metrics::{Counter, Gauge, Histogram, Unit, describe_counter, describe_gauge, describe_histogram};
use lazy_static::lazy_static;
use std::sync::Arc;

pub struct RuntimeMetrics {
    // Actor lifecycle metrics
    pub total_actors: Gauge,
    
    // Actor performance metrics
    pub actor_command_throughput: Counter,
    pub actor_command_latency: Histogram,
    
    // Error metrics
    pub error_rate: Counter,
    
    // Runtime health metrics
    pub thread_pool_utilization: Gauge,
}

impl RuntimeMetrics {
    pub fn new() -> Self {
        let metrics = Self {
            total_actors: metrics::gauge!("runtime.total_actors"),
            actor_command_throughput: metrics::counter!("runtime.actor_command_throughput"),
            actor_command_latency: metrics::histogram!("runtime.actor_command_latency"),
            error_rate: metrics::counter!("runtime.error_rate"),
            thread_pool_utilization: metrics::gauge!("runtime.thread_pool_utilization"),
        };

        // Metrics Description
        describe_gauge!("runtime.total_actors", "Total number of active actors");
        describe_counter!("runtime.actor_command_throughput", "Number of commands executed by actors");
        describe_histogram!("runtime.actor_command_latency", Unit::Seconds, "Latency of command execution by actors");
        describe_counter!("runtime.error_rate", "Number of errors occurred");
        describe_gauge!("runtime.thread_pool_utilization", "Percentage of thread pool in use");

        metrics
    }
}

lazy_static! {
    pub static ref METRICS: Arc<RuntimeMetrics> = Arc::new(RuntimeMetrics::new());
}
