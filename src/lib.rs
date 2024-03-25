//! # Scheduler Module
//! ## License
//! MIT License
//! ## Disclaimer
//! This module is provided "as is", without warranty of any kind, express or implied. Use at your own risk.
//! ## Author
//! Andrzej Sulkowski
//!
//! This module provides a flexible scheduling system for asynchronous tasks,
//! allowing for tasks to be scheduled based on specific intervals and conditions.
//! It utilizes Rust's powerful concurrency primitives from the `tokio` and `std` libraries
//! to manage and execute tasks in an efficient and controlled manner.
//!
//! ## Overview
//!
//! The core components of the scheduler module are:
//!
//! - `Schedulable`: A trait that defines how tasks can be scheduled, processed, and handled upon timeouts.
//!   It requires implementations to specify how tasks are identified, processed, and what actions to take
//!   when tasks timeout or complete their execution cycle.
//!
//! - `ScheduleInterval`: A struct that represents the scheduling criteria for tasks, specifying the upper limit
//!   for task execution and the frequency at which the tasks should be executed within that limit.
//!
//! - `ScheduleConfig`: A configuration struct for the scheduler, containing the scheduling intervals,
//!   a flag to determine if task data should be cloned for processing, and a context object for shared state.
//!
//! - `RunningTask`: Represents a task currently being managed by the scheduler. It contains the task's unique identifier,
//!   the task data, scheduling configuration, and synchronization primitives for gracefully stopping the task.
//!
//! - `StaleTask`: Represents a task that has completed its execution cycle and is awaiting further actions,
//!   such as rescheduling or cleanup.
//!
//! - `Scheduler`: The main scheduler struct that manages tasks. It allows for adding, removing, inspecting,
//!   stopping, and running tasks based on the provided `ScheduleConfig`. The scheduler ensures tasks are executed
//!   according to their scheduling criteria and manages the lifecycle of tasks through `RunningTask` and `StaleTask` states.
//!
//! ## Features
//!
//! - **Dynamic Scheduling**: Tasks can be scheduled dynamically with varying frequencies and conditions,
//!   allowing for complex scheduling logic that can adapt to the runtime state of the application.
//!
//! - **Conditional Cloning**: The scheduler supports conditional cloning of task data, enabling efficient
//!   handling of tasks whether or not they require cloning based on the task's requirements.
//!
//! - **Graceful Task Handling**: Provides mechanisms for gracefully stopping tasks and handling tasks
//!   that have exceeded their execution limits through the use of `Notify` and `oneshot` channels.
//!
//!
//! ## Usage
//!
//! To use the scheduler, define tasks that implement the `Schedulable` trait, create a `ScheduleConfig`
//! with the desired scheduling intervals, and use the `Scheduler` to manage and execute tasks.
//! The scheduler module is designed to be flexible, supporting various use cases from simple periodic tasks
//! to complex, conditionally scheduled tasks requiring shared state and dynamic scheduling logic.
//!
//! ## Scheduler Module Example
//!
//! This example demonstrates how to use the Scheduler module to schedule tasks.
//!
//! ### Example
//!
//! ```rust,no_run
//! use async_trait::async_trait;
//! use std::sync::Arc;
//! use std::time::{Duration, SystemTime, UNIX_EPOCH};
//! use tokio::sync::Mutex;
//! use scheduler_module::{Scheduler, Schedulable, ScheduleConfig, ScheduleInterval, Ctx2}; // Adjust the path according to your module's structure.
//!
//! #[derive(Clone, Debug)]
//! struct MyTask {
//!     id: String,
//!     counter: u32,
//!     is_completed: bool,
//! }
//!
//! impl MyTask {
//!     pub fn new(id: String) -> MyTask {
//!         MyTask {
//!             id,
//!             counter: 0,
//!             is_completed: false,
//!         }
//!     }
//! }
//!
//! #[async_trait]
//! impl Schedulable<String, MyTask, Ctx2> for MyTask {
//!     fn id(&self) -> String {
//!         self.id.clone()
//!     }
//!
//!     async fn process(mut self, ctx: Arc<Ctx2>) -> MyTask {
//!         // Simulate some work.
//!         self.counter += 1;
//!         println!("Processing task: {}, counter: {}", self.id, self.counter);
//!         self
//!     }
//!
//!     async fn time_out(self, ctx: Arc<Ctx2>) {
//!         println!("Task timed out: {}", self.id);
//!     }
//!     async fn completed(self, ctx: Arc<Ctx2>){
//!         println!("Task completed: {}", self.id);
//!     }
//! }
//!
//! #[derive(Clone, Debug)]
//! struct Ctx2;
//!
//! impl Ctx2 {
//!     pub fn new() -> Arc<Ctx2> {
//!         Arc::new(Ctx2)
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let ctx = Ctx2::new();
//!     let schedule_intervals = vec![
//!         ScheduleInterval::new(Duration::from_secs(60 * 60), Duration::from_secs(5)),
//!     ];
//!     let config = ScheduleConfig::new(schedule_intervals, false, Arc::new(ctx));
//!
//!     let mut scheduler: Scheduler<String, MyTask, Ctx2> = Scheduler::new(config);
//!     let my_task = MyTask::new("task_id".into()); // Create your task
//!
//!     let task_id = scheduler.add(my_task);
//!     scheduler.run(task_id).await;
//!     // Task is now scheduled according to the defined intervals and will be processed accordingly.
//! }
//! ```
//! Ensure all the necessary types (`Scheduler`, `Schedulable`, `ScheduleConfig`, `ScheduleInterval`, `Ctx2`) are correctly defined and accessible in your module. This example assumes these types are part of the `scheduler_module`, and you should adjust the import paths to fit your project's structure.
//! 
//! 
use async_trait::async_trait;
use chrono::{DateTime, Local};
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, hash::Hash};
use thiserror::Error;
use tokio::spawn;
use tokio::sync::{mpsc, oneshot, Mutex, Notify, RwLock, RwLockReadGuard};
use tracing;

/// A trait defining the behavior of schedulable tasks within the scheduler system.
///
/// The `Schedulable` trait outlines the basic structure and necessary operations for tasks that can be scheduled, processed, and timed out within a scheduler. It allows for tasks to be uniquely identified, processed asynchronously with context, and handled upon timing out.
///
/// # Type Parameters
///
/// - `K`: The type used to uniquely identify tasks. It must support basic traits such as `Clone`, `PartialEq`, `Eq`, `Hash`, `Send`, `Sync`, and `Debug`.
/// - `V`: The type representing the task itself, which must support `Clone`, `Send`, `Sync`, and `Debug`. The requirement for `Clone` can be conditional based on the scheduler configuration.
/// - `Ctx`: The type representing the context passed to tasks during processing and on time out. It must support `Clone`, `Send`, `Sync`, and `Debug`.
///
/// # Methods
///
/// - `id`: Returns the unique identifier for the task. This ID is used by the scheduler to track and manage tasks.
/// - `process`: An asynchronous method called by the scheduler to process the task. It takes a shared context (`Arc<Ctx>`) and returns a new or updated version of the task (`V`). The implementation should contain the task's logic.
/// - `time_out`: An asynchronous method invoked when the task times out according to its scheduling criteria. Like `process`, it takes a shared context for handling any required cleanup or finalization.
///
/// Implementors of this trait are responsible for defining how tasks are identified, processed, and handled upon timing out. This flexibility allows for a wide range of task types and processing logic to be integrated into the scheduler system.
#[async_trait]
pub trait Schedulable<K, V, Ctx>
where
    K: Clone + PartialEq + Eq + Hash + Send + Sync + 'static + Debug,
    V: Clone + Send + Sync + 'static + Debug, // Note: `Clone` on `V` may not be necessary depending on scheduler configuration
    Ctx: Clone + Send + Sync + 'static + Debug,
{
    /// Returns the unique identifier of the task.
    fn id(&self) -> K;

    /// Asynchronously processes the task.
    ///
    /// # Parameters
    ///
    /// - `ctx`: A shared context (`Arc<Ctx>`) providing necessary data or services for task processing.
    ///
    /// # Returns
    ///
    /// - A new or modified instance of the task after processing.
    async fn process(mut self, ctx: Arc<Ctx>) -> (V, TaskStatus);

    /// Asynchronously handles task timeout.
    ///
    /// This method is called when the task exceeds its allocated time frame without completion. It can be used for cleanup, notifications, or other finalization logic.
    ///
    /// # Parameters
    ///
    /// - `ctx`: A shared context (`Arc<Ctx>`) for performing any required actions on timeout.
    async fn time_out(self, ctx: Arc<Ctx>);
    async fn complete(self, ctx: Arc<Ctx>);
}

/// Represents a scheduling interval with an upper limit and a frequency.
///
/// This struct defines the time frame and frequency at which scheduled tasks should be executed. Each `ScheduleInterval` consists of an `duration`, representing the duration after which the frequency changes, and a `frequency`, indicating how often within that period a task should be processed.
///
/// # Usage
///
/// `ScheduleInterval` is used within `ScheduleConfig` to define various intervals and frequencies for task execution. By setting multiple intervals, the scheduler can adjust task processing frequency based on elapsed time or other criteria.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use your_crate::ScheduleInterval;
///
/// let interval = ScheduleInterval::new(Duration::from_secs(3600), Duration::from_secs(60));
/// // This interval implies that for the first hour (3600 seconds), tasks should be processed every minute (60 seconds).
/// ```
///
/// # Fields
///
/// - `duration`: A `Duration` after which this interval's frequency no longer applies. It effectively sets the maximum duration for which the specified `frequency` is valid.
/// - `frequency`: The `Duration` between task executions within the bounds set by `duration`.
#[derive(Clone, Debug)]
pub struct ScheduleInterval {
    /// The maximum duration for this interval's frequency to be valid.
    pub duration: Duration,
    /// The duration between task executions within this interval.
    pub frequency: Duration,
}

impl ScheduleInterval {
    /// Creates a new `ScheduleInterval` with duration and frequency.
    ///
    /// # Parameters
    ///
    /// - `duration`: A `Duration` after which this interval's frequency no longer applies. It effectively sets the maximum duration for which the specified `frequency` is valid.
    /// - `frequency`: The `Duration` between task executions within the bounds set by `duration`.
    ///
    /// # Returns
    ///
    /// A new instance of `ScheduleInterval`.
    pub fn new(duration: Duration, frequency: Duration) -> ScheduleInterval {
        ScheduleInterval {
            duration,
            frequency,
        }
    }
}

/// Configuration for scheduling tasks with varying frequencies over time.
///
/// This struct allows defining multiple scheduling intervals, each with its own frequency of task execution. It supports adjusting task processing frequency dynamically based on the elapsed time or specific conditions.
///
/// # Usage
///
/// `ScheduleConfig` is used to initialize a scheduler, where it dictates how often tasks should be processed. The configuration includes multiple `ScheduleInterval` instances, each specifying the frequency for a given period.
///
/// The `clone_data` flag indicates whether the data should be cloned when being processed. Cloning enables concurrent reads during processing but introduces overhead.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::time::Duration;
/// use your_crate::{ScheduleConfig, ScheduleInterval};
///
/// let intervals = vec![
///     ScheduleInterval::new(Duration::from_secs(3600), Duration::from_secs(60)),
///     ScheduleInterval::new(Duration::from_secs(7200), Duration::from_secs(120)),
/// ];
///
/// let config = ScheduleConfig::new(intervals, true, Arc::new(MyContext::new()));
/// // This configuration sets up two intervals:
/// // - For the first hour, tasks are processed every minute.
/// // - After the first hour and up to two hours, tasks are processed every two minutes.
/// ```
///
/// # Fields
///
/// - `ranges`: A list of `ScheduleInterval` defining the frequencies at which tasks should be executed.
/// - `clone_data`: A boolean indicating whether data should be cloned when being processed. Useful for allowing concurrent reads.
/// - `ctx`: The context shared across all tasks, typically containing configuration or state relevant to task processing.
#[derive(Clone, Debug, Error)]
pub struct ScheduleConfig<Ctx> {
    /// A vector of `ScheduleInterval` specifying different scheduling frequencies.
    pub ranges: Vec<ScheduleInterval>,
    /// Indicates whether data should be cloned for processing, enabling concurrent reads.
    pub clone_data: bool,
    /// Wether to perform a processing session on the start of the scheduler
    pub init_processing: bool,
    /// A shared context for all tasks, providing necessary configuration or state.
    pub ctx: Arc<Ctx>,
}

impl<Ctx> ScheduleConfig<Ctx> {
    /// Creates a new `ScheduleConfig` with specified intervals, cloning behavior, and context.
    ///
    /// # Parameters
    ///
    /// - `ranges`: A list of intervals each with a defined frequency for task execution.
    /// - `clone_data`: Determines if task data should be cloned during processing.
    /// - `ctx`: The shared context across tasks.
    ///
    /// # Returns
    ///
    /// A new instance of `ScheduleConfig`.
    pub fn new(ranges: Vec<ScheduleInterval>, ctx: Arc<Ctx>) -> Self {
        ScheduleConfig {
            ranges,
            clone_data: true,
            init_processing: true,
            ctx,
        }
    }

    pub fn set_is_clone(mut self, v: bool) -> Self {
        self.clone_data = v;
        self
    }
    pub fn set_is_init_processing(mut self, v: bool) -> Self {
        self.init_processing = v;
        self
    }

    /// Calculates the frequency for task execution based on elapsed time.
    ///
    /// This method iterates through the defined intervals, determining the current frequency based on the total elapsed time.
    ///
    /// # Parameters
    ///
    /// - `elapsed_milliseconds`: The total elapsed time in milliseconds.
    ///
    /// # Returns
    ///
    /// An `Option<Duration>` representing the current frequency for task execution, or `None` if beyond the defined intervals.
    fn get_frequency_for_elapsed(&self, elapsed_milliseconds: u128) -> Option<Duration> {
        let mut sum_milliseconds = 0;
        for interval in &self.ranges {
            sum_milliseconds += interval.duration.as_millis();
            if elapsed_milliseconds <= sum_milliseconds {
                return Some(interval.frequency);
            }
        }
        None
    }
}

#[derive(Debug, Error, Clone, PartialEq)]
pub enum TaskStatus {
    Running,
    Canceled,
    Completed,
    Timeout,
}
impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskStatus::Running => write!(f, "Running"),
            TaskStatus::Canceled => write!(f, "Canceled"),
            TaskStatus::Completed => write!(f, "Completed"),
            TaskStatus::Timeout => write!(f, "Timeout"),
        }
    }
}

/// Represents a task scheduled for execution, handling its lifecycle and processing.
///
/// This struct encapsulates the details and mechanisms required to manage a task's execution within a scheduler. It includes mechanisms for starting, processing, stopping, and inspecting the task. Additionally, it supports asynchronous operation and communication with a scheduler to notify task completion or termination.
///
/// # Generics
///
/// - `K`: The type used as the unique identifier for the task.
/// - `V`: The type of the task's data, which must implement the `Schedulable` trait.
/// - `Ctx`: The type of the shared context used across all tasks within the scheduler.
///
/// # Usage
///
/// A `RunningTask` is typically created by a `Scheduler` and not directly instantiated in client code. It requires a task data `V`, a scheduler notifier channel, and a scheduling configuration.
///
/// # Examples
///
/// This example outlines the creation and lifecycle management of a `RunningTask` within the context of a scheduler implementation:
///
/// ```rust
/// #[tokio::main]
/// async fn main() {
///     let task_data = MyTask::new(); // Assuming MyTask implements Schedulable
///     let (scheduler_notifier_tx, scheduler_notifier_rx) = tokio::sync::mpsc::channel(10);
///     let config = Arc::new(ScheduleConfig::new(vec![], false, Arc::new(MyContext::new())));
///
///     let running_task = RunningTask::new(task_data, scheduler_notifier_tx.clone(), config);
///     // The task is now initialized and will be processed according to its schedule.
///
///     // To stop the task and retrieve its final state:
///     match running_task.stop().await {
///         Ok(stale_task) => println!("Task completed: {:?}", stale_task.data),
///         Err(e) => eprintln!("Error stopping task: {:?}", e),
///     }
/// }
/// ```
///
/// # Important Methods
///
/// - `new()`: Creates a new instance of `RunningTask` with the provided task data, scheduler notifier, and configuration.
/// - `stop()`: Attempts to stop the task gracefully, returning a `StaleTask` representing the task's final state if successful.
/// - `inspect()`: Allows inspecting the current state of the task's data in a read-only fashion.
///
/// # Task Processing
///
/// `RunningTask` handles its own asynchronous processing by spawning a new task that awaits for its execution time according to the schedule. It supports dynamic frequency adjustment, optional data cloning for processing, and notifies the scheduler upon completion or when explicitly stopped.
#[derive(Error, Debug)]
pub struct RunningTask<K, V, Ctx> {
    /// Unique identifier for the task, provided by the `Schedulable` implementation.
    pub id: K,
    /// The task's data wrapped in an `Arc<RwLock>` for concurrent access and optional cloning.
    pub data: Arc<RwLock<Option<V>>>,
    /// Configuration for the scheduler, shared across all tasks.
    config: Arc<ScheduleConfig<Ctx>>,
    /// A channel for sending a shutdown signal to the running task.
    shutdown_tx: Option<oneshot::Sender<TaskStatus>>,
    /// The timestamp marking the start of the task's execution.
    start_time: DateTime<Local>,
    /// A notification mechanism used to signal that processing of the task's data is complete.
    processing_notify: Arc<Notify>,
}

impl<K, V, Ctx> RunningTask<K, V, Ctx>
where
    K: Clone + PartialEq + Eq + Hash + Send + Sync + 'static + Debug,
    V: Schedulable<K, V, Ctx> + Clone + Send + Sync + 'static + Debug,
    Ctx: Clone + Send + Sync + 'static + Debug,
{
    /// Creates a new running task with provided data, scheduler notifier, and configuration.
    ///
    /// - `data`: The task data.
    /// - `scheduler_notifier`: A channel to notify the scheduler about task completion or errors.
    /// - `config`: Shared scheduler configuration.
    pub fn new(
        data: V,
        scheduler_notifier: mpsc::Sender<(K, TaskStatus)>,
        config: Arc<ScheduleConfig<Ctx>>,
    ) -> Self {
        let (tx, rx) = oneshot::channel();

        let running_task = RunningTask {
            id: data.id(),
            data: Arc::new(RwLock::new(Some(data))),
            config,
            shutdown_tx: Some(tx),
            start_time: Local::now(),
            processing_notify: Arc::new(Notify::new()), //state: Arc::new(RwLock::new(TaskState::Healthy)),
        };
        running_task.run_task(rx, scheduler_notifier);
        running_task
    }

    /// Attempts to gracefully stop the running task, returning the stale task if successful.
    pub async fn stop(mut self) -> Result<StaleTask<K, V, Ctx>, SchedulerError> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(TaskStatus::Timeout);
        }

        loop {
            let data_lock = self.data.read().await;
            if data_lock.is_some() {
                break;
            }
            drop(data_lock);
            self.processing_notify.notified().await;
        }
        // data can not be taken for processing during this short lock release because we send out the shutdown signal
        let data = {
            let mut data_lock = self.data.write().await;
            data_lock
                .take()
                .expect("Data should be available after notification")
        };

        Ok(StaleTask::new(data, self.config.clone()))
    }
    /// Allows inspecting the task's data in a read-only fashion.
    ///
    /// - `f`: Function to apply to the task's data.
    pub async fn inspect<F>(&self, f: F)
    where
        F: Fn(RwLockReadGuard<'_, Option<V>>),
    {
        f(self.data.read().await)
    }

    fn run_task(
        &self,
        mut rx: oneshot::Receiver<TaskStatus>,
        scheduler_notifier: mpsc::Sender<(K, TaskStatus)>,
    ) {
        let data = self.data.clone();
        let config = self.config.clone();
        let start_time = self.start_time.clone();
        let ctx = self.config.ctx.clone();
        let id = self.id.clone();
        let notify = self.processing_notify.clone();
        let clone_data = self.config.clone_data;

        tokio::spawn(async move {
            let mut first_run = true;

            loop {
                let elapsed_milliseconds = Local::now()
                    .signed_duration_since(start_time)
                    .num_milliseconds() as u128;
                let duration = config.get_frequency_for_elapsed(elapsed_milliseconds);

                if duration.is_none() {
                    let _ = scheduler_notifier.send((id, TaskStatus::Timeout)).await;
                    break;
                }

                if !first_run || !config.init_processing {
                    let duration = duration.unwrap();
                    tokio::select! {
                        _ = tokio::time::sleep(duration) => {},
                        _ = &mut rx => {
                            break;
                        }
                    }
                }

                if let Some(v) = if clone_data {
                    data.read().await.clone()
                } else {
                    data.write().await.take()
                } {
                    let (new_v, status) = v.process(ctx.clone()).await;
                    let id = new_v.id().clone();

                    let mut data_lock = data.write().await;
                    *data_lock = Some(new_v);
                    notify.notify_waiters();

                    if TaskStatus::Running != status {
                        let _ = scheduler_notifier.send((id, status)).await;
                    }
                }

                first_run = false;

                if rx.try_recv().is_ok() {
                    // During processing we received the shutdown signal
                    break;
                }
            }
        });
    }
}

/// Represents a task that has completed its execution cycle but remains to be processed or acted upon.
///
/// A `StaleTask` holds the data of a task that has either finished its scheduled execution or was
/// stopped manually. It is called "stale" because it is no longer actively being scheduled or processed,
/// but it still holds valuable state that can be inspected or used to resume operation.
///
/// `StaleTask` is a transitional state before a task is either fully concluded or restarted. It contains
/// the task's last known data state and its configuration, allowing for inspection or further actions.
///
/// # Generics
///
/// - `K`: A unique key type used to identify the task.
/// - `V`: The type of the task's data. Must implement the `Schedulable` trait.
/// - `Ctx`: The context type shared across tasks, providing necessary environmental or configuration details.
///
/// # Examples
///
/// Assuming the definition of a `MyTask` struct that implements `Schedulable` and a context struct `MyContext`,
/// you could transition a `RunningTask` to a `StaleTask` like so:
///
/// ```rust
/// # use std::sync::Arc;
/// # use tokio::sync::mpsc;
/// # struct MyTask { id: String };
/// # struct MyContext;
/// # impl MyTask {
/// #     fn id(&self) -> String { self.id.clone() }
/// # }
/// # #[async_trait::async_trait]
/// # impl Schedulable<String, MyTask, MyContext> for MyTask {
/// #     async fn process(self, ctx: Arc<MyContext>) -> MyTask { self }
/// #     async fn time_out(self, ctx: Arc<MyContext>) {}
/// # }
/// # let scheduler_notifier: mpsc::Sender<String> = todo!();
/// # let task_data = MyTask { id: "task_1".to_string() };
/// # let task_config = Arc::new(ScheduleConfig::new(vec![], false, Arc::new(MyContext)));
/// let stale_task = StaleTask::new(task_data, task_config);
/// let running_task = stale_task.run(scheduler_notifier);
/// ```
///
/// In this example, a `StaleTask` is created with some `MyTask` data and configuration, then immediately transitioned back
/// into a `RunningTask` for further processing.
#[derive(Debug)]
pub struct StaleTask<K, V, Ctx> {
    /// Unique identifier for the task.
    pub id: K,
    /// The task's data at the time of becoming stale.
    pub data: V,
    /// Configuration shared across all tasks within the scheduler.
    config: Arc<ScheduleConfig<Ctx>>,
}

impl<K, V, Ctx> StaleTask<K, V, Ctx>
where
    K: Clone + PartialEq + Eq + Hash + Send + Sync + 'static + Debug,
    V: Schedulable<K, V, Ctx> + Clone + Send + Sync + 'static + Debug,
    Ctx: Clone + Send + Sync + 'static + Debug,
{
    /// Constructs a new `StaleTask` with the given data and configuration.
    ///
    /// # Arguments
    ///
    /// * `data`: The task data.
    /// * `config`: The scheduler configuration shared among tasks.
    pub fn new(data: V, config: Arc<ScheduleConfig<Ctx>>) -> Self {
        StaleTask {
            id: data.id(),
            data,
            config,
        }
    }
    /// Transitions the `StaleTask` back into a `RunningTask` for further processing or rescheduling.
    ///
    /// # Arguments
    ///
    /// * `scheduler_notifier`: A channel sender used to notify the scheduler of task completion or errors.
    ///
    /// # Returns
    ///
    /// A `RunningTask` instance, initialized with the `StaleTask`'s data and the provided scheduler notifier.
    pub fn run(self, scheudler_notifier: mpsc::Sender<(K, TaskStatus)>) -> RunningTask<K, V, Ctx> {
        RunningTask::new(self.data, scheudler_notifier, self.config)
    }
}

/// Manages the scheduling, execution, and lifecycle of tasks.
///
/// The `Scheduler` is the central component of the scheduling system, responsible for adding tasks,
/// running them according to their schedule, transitioning tasks between active and stale states,
/// and providing mechanisms for inspecting and stopping tasks.
///
/// # Generics
///
/// - `K`: A unique key type used to identify tasks. This is used to manage and reference tasks within the scheduler.
/// - `V`: The type of the task's data. Tasks must implement the `Schedulable` trait.
/// - `Ctx`: The context type shared across tasks, providing necessary environmental or configuration details for tasks.
///
/// # Fields
///
/// - `running_tasks`: A thread-safe collection of tasks that are currently being scheduled and executed.
/// - `stale_tasks`: A collection of tasks that have completed their execution cycle but remain for potential further action.
/// - `config`: Shared configuration for the scheduler and tasks, including scheduling intervals and contextual data.
/// - `running_task_tx_sender`: A channel sender used to notify the scheduler from within tasks about various events (e.g., task completion, errors).
///
/// # Examples
///
/// ```rust
/// # use std::sync::Arc;
/// # use tokio::sync::{mpsc, Mutex};
/// # struct MyTask;
/// # struct MyContext;
/// # impl MyTask {
/// #     fn new() -> Self { MyTask }
/// #     fn id(&self) -> String { "task_id".to_string() }
/// # }
/// # #[async_trait::async_trait]
/// # impl Schedulable<String, MyTask, MyContext> for MyTask {
/// #     async fn process(self, ctx: Arc<MyContext>) -> MyTask { self }
/// #     async fn time_out(self, ctx: Arc<MyContext>) {}
/// # }
/// # let (tx, rx) = mpsc::channel(100);
/// let scheduler = Scheduler::new(Arc::new(ScheduleConfig::new(vec![], Arc::new(MyContext))));
///
/// let my_task = MyTask::new();
/// let task_id = scheduler.add(my_task);
/// scheduler.run(task_id).await; // Start the task according to its schedule
/// ```
///
/// This example demonstrates the creation of a `Scheduler` with a simple task. The task is added to the scheduler,
/// which then starts executing it according to the defined scheduling rules.
pub struct Scheduler<K, V, Ctx> {
    /// A collection of tasks that are currently active and being scheduled.
    pub running_tasks: Arc<Mutex<HashMap<K, RunningTask<K, V, Ctx>>>>,
    /// Tasks that have completed their execution and are in a stale state, awaiting further action.
    pub stale_tasks: HashMap<K, StaleTask<K, V, Ctx>>,
    /// Shared scheduler configuration, including task scheduling intervals and contextual data.
    pub config: Arc<ScheduleConfig<Ctx>>,
    /// Internal channel sender for notifying the scheduler from running tasks.
    running_task_tx_sender: mpsc::Sender<(K, TaskStatus)>,
}

impl<K, V, Ctx> Scheduler<K, V, Ctx>
where
    K: Clone + PartialEq + Eq + Hash + Send + Sync + 'static + Debug,
    V: Schedulable<K, V, Ctx> + Clone + Send + Sync + 'static + Debug,
    Ctx: Clone + Send + Sync + 'static + Debug,
{
    /// Creates a new scheduler with the given configuration.
    ///
    /// This initializes the scheduler with an empty set of running and stale tasks, ready to schedule and manage tasks
    /// based on the provided `ScheduleConfig`.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration settings for the scheduler, including scheduling intervals and contextual data.
    ///
    /// # Returns
    ///
    /// A new instance of `Scheduler`.
    pub fn new(config: ScheduleConfig<Ctx>) -> Self {
        let running_tasks: Arc<Mutex<HashMap<K, RunningTask<K, V, Ctx>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let config = Arc::new(config);
        let stale_tasks = HashMap::new();

        let (tx_sender, tx_receiver) = mpsc::channel::<(K, TaskStatus)>(100);
        Scheduler::listener(tx_receiver, running_tasks.clone(), config.clone());

        Scheduler {
            running_tasks,
            stale_tasks,
            config,
            running_task_tx_sender: tx_sender,
        }
    }
    /// Adds a task to the scheduler.
    ///
    /// This stages the task for scheduling and execution based on the scheduler's configuration. The task is initially
    /// added as a stale task, ready to be transitioned to a running state.
    ///
    /// # Arguments
    ///
    /// * `item` - The task to add to the scheduler.
    ///
    /// # Returns
    ///
    /// The unique identifier (`K`) associated with the added task.
    pub fn add(&mut self, item: V) -> K {
        let stale_task = StaleTask::new(item.clone(), self.config.clone());
        let id = item.id();
        self.stale_tasks.insert(id.clone(), stale_task);
        id
    }
    /// Removes a task from the scheduler.
    ///
    /// This method attempts to remove a task, identified by its unique key, from the scheduler. If the task is found,
    /// it is removed and returned to the caller.
    ///
    /// # Arguments
    ///
    /// * `id` - The unique identifier of the task to remove.
    ///
    /// # Returns
    ///
    /// An `Option` containing the removed task's data if found, or `None` if the task was not found.
    pub async fn remove(&mut self, id: K) -> Option<V> {
        self.stale_tasks
            .remove(&id)
            .map(|stale_task| stale_task.data)
    }
    /// Inspects a task, identified by its unique key, and performs an operation on its data.
    ///
    /// This method allows for reading the state of a task's data without modifying the scheduler's state.
    ///
    /// # Arguments
    ///
    /// * `id` - The unique identifier of the task to inspect.
    /// * `f` - A closure that takes a read lock guard of the task's data for inspection.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure in finding and inspecting the task.
    pub async fn inspect(
        &self,
        id: &K,
        f: impl Fn(RwLockReadGuard<'_, Option<V>>),
    ) -> Result<(), SchedulerError> {
        let running_tasks_lock = self.running_tasks.lock().await;
        let running_task = running_tasks_lock
            .get(id)
            .ok_or(SchedulerError::InvalidId)?;
        running_task.inspect(f).await;
        Ok(())
    }
    /// Stops a running task, transitioning it to a stale state.
    ///
    /// This method attempts to gracefully stop a task's execution and transition it to a stale state for potential
    /// further action or cleanup.
    ///
    /// # Arguments
    ///
    /// * `id` - The unique identifier of the task to stop.
    ///
    /// # Returns
    ///
    /// A `Result` indicating the success or failure of the operation.
    pub async fn stop(&mut self, id: K) -> Result<(), SchedulerError> {
        let mut running_tasks_lock = self.running_tasks.lock().await;
        let mut running_task = running_tasks_lock
            .remove(&id)
            .ok_or(SchedulerError::InvalidId)?;
        drop(running_tasks_lock);

        if let Some(tx) = running_task.shutdown_tx.take() {
            let _ = tx.send(TaskStatus::Canceled);
        }
        let stale_task = running_task.stop().await?;
        self.stale_tasks.insert(id, stale_task);

        Ok(())
    }

    /// Starts the execution of a task based on its unique identifier.
    ///
    /// This method transitions a task from the stale state to the running state, scheduling it according to the
    /// scheduler's configuration. It ensures that the task's execution is managed and can be stopped or inspected.
    ///
    /// # Arguments
    ///
    /// * `id` - The unique identifier of the task to run.
    ///
    /// # Returns
    ///
    /// A `Result` indicating the success or failure of starting the task.
    ///
    /// # Errors
    ///
    /// Returns an `Err` if no task with the given identifier exists in the stale task pool.
    pub async fn run(&mut self, id: K) -> Result<(), SchedulerError> {
        let stale_task = self
            .stale_tasks
            .remove(&id)
            .ok_or(SchedulerError::InvalidId)?;
        let running_task = stale_task.run(self.running_task_tx_sender.clone());
        let mut running_tasks_lock = self.running_tasks.lock().await;
        running_tasks_lock.insert(id, running_task);
        Ok(())
    }
    /// Asynchronously listens for task completion notifications and handles cleanup operations.
    ///
    /// This internal method starts a background task that listens for notifications from running tasks. When a task
    /// signals completion, it's removed from the running tasks pool, and any necessary cleanup or transition actions
    /// are performed.
    ///
    /// # Arguments
    ///
    /// * `receiver` - A channel receiver for receiving task completion notifications.
    /// * `running_tasks` - A shared reference to the running tasks pool.
    /// * `config` - A shared reference to the scheduler's configuration.
    ///
    /// # Notes
    ///
    /// The listener is intended to run indefinitely as a background task, monitoring for task completion signals
    /// and handling them appropriately. It ensures that the scheduler's state remains consistent and that resources
    /// are properly managed.
    fn listener(
        mut receiver: mpsc::Receiver<(K, TaskStatus)>,
        running_tasks: Arc<Mutex<HashMap<K, RunningTask<K, V, Ctx>>>>,
        config: Arc<ScheduleConfig<Ctx>>,
    ) {
        spawn(async move {
            while let Some((key, status)) = receiver.recv().await {
                let mut running_tasks_lock = running_tasks.lock().await;

                match running_tasks_lock.remove(&key) {
                    None => {
                        tracing::error!(
                            key = ?key,
                            running_tasks = ?*running_tasks_lock,
                            "Error in listener - Could not find emitted task key"
                        );
                    }
                    Some(task) => {
                        drop(running_tasks_lock);
                        match status {
                            TaskStatus::Timeout => {
                                let stale_task = task.stop().await.expect("The Data inside this task should have been present as the task itself notified");
                                stale_task.data.time_out(config.ctx.clone()).await;
                            }
                            TaskStatus::Completed => {
                                let stale_task = task.stop().await.expect("The Data inside this task should have been present as the task itself notified");
                                stale_task.data.complete(config.ctx.clone()).await;
                            }
                            _ => (),
                        }
                    }
                }
            }
        });
    }
}

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("Processing Error")]
    ProcessingError,
    #[error("Invalid Id")]
    InvalidId,
    #[error("Is Being Processed")]
    BeingProcessed,
    #[error("Internal Error")]
    InternalError,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::marker::Send;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};
    use test_log::test;
    use tokio::time::sleep;

    #[derive(Clone, Debug)]
    struct Ctx {
        timed_out_tasks: Arc<Mutex<Vec<DummyData>>>,
    }
    impl Ctx {
        pub fn new() -> Arc<Ctx> {
            Arc::new(Ctx {
                timed_out_tasks: Arc::new(Mutex::new(Vec::new())),
            })
        }
    }

    #[derive(Clone, Debug)]
    struct DummyData {
        pub id: String,
        pub counter: i32,
        pub is_completed: bool,
    }
    impl DummyData {
        pub fn new() -> DummyData {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos(); // Using current time in nanoseconds to seed our "random" generator.

            let id = format!("{:x}", now); // Convert to a hexadecimal string.

            DummyData {
                id,
                counter: 0,
                is_completed: false,
            }
        }
    }

    #[async_trait]
    impl Schedulable<String, DummyData, Ctx> for DummyData {
        fn id(&self) -> String {
            self.id.clone()
        }
        async fn time_out(mut self, ctx: Arc<Ctx>) {
            self.is_completed = true;
            let mut timed_out_tasks_lock = ctx.timed_out_tasks.lock().await;
            timed_out_tasks_lock.push(self);
        }
        async fn process(mut self, ctx: Arc<Ctx>) -> (DummyData, TaskStatus) {
            self.counter += 1;
            (self, TaskStatus::Running)
        }
        async fn complete(mut self, ctx: Arc<Ctx>) {
            todo!("");
        }
    }

    #[test(tokio::test)]
    async fn test_create_scheduler() {
        let ctx = Ctx::new();

        let config = ScheduleConfig::new(vec![], ctx).set_is_clone(false);
        let mut scheduler: Scheduler<String, DummyData, Ctx> = Scheduler::new(config);
        let dummy1 = DummyData::new();

        let key = scheduler.add(dummy1);
        assert_eq!(scheduler.stale_tasks.len(), 1);
    }

    #[test(tokio::test)]
    async fn start_tasks() {
        let ctx = Ctx::new();

        let intervals = vec![ScheduleInterval::new(
            Duration::from_secs(60 * 60),
            Duration::from_secs(1),
        )];
        let config = ScheduleConfig::new(intervals, ctx).set_is_clone(true);
        let mut scheduler: Scheduler<String, DummyData, Ctx> = Scheduler::new(config);
        let dummy1 = DummyData::new();
        let key = scheduler.add(dummy1);
        scheduler.run(key.clone()).await.unwrap();
        let running_tasks_lock = scheduler.running_tasks.lock().await;
        assert_eq!(running_tasks_lock.len(), 1);
    }

    #[test(tokio::test)]
    async fn test_stop_tasks() {
        let ctx = Ctx::new();

        let intervals = vec![ScheduleInterval::new(
            Duration::from_secs(60 * 60),
            Duration::from_secs(1),
        )];
        let config = ScheduleConfig::new(intervals, ctx).set_is_clone(false);
        let mut scheduler = Scheduler::new(config);
        let dummy1 = DummyData::new();
        let key = scheduler.add(dummy1);
        scheduler.run(key.clone()).await.unwrap();
        scheduler.stop(key.clone()).await.unwrap();

        let option_stale_task = scheduler.stale_tasks.get(&key);
        assert!(option_stale_task.is_some());

        let running_tasks_lock = scheduler.running_tasks.lock().await;
        assert_eq!(running_tasks_lock.len(), 0);
    }

    #[test(tokio::test)]
    async fn test_periodic_execution() {
        let ctx = Ctx::new();

        let check_frequency = Duration::from_secs(1);
        let intervals = vec![ScheduleInterval::new(
            Duration::from_secs(60 * 60),
            check_frequency,
        )];

        let config = ScheduleConfig::new(intervals, ctx).set_is_clone(false);
        let mut scheduler = Scheduler::new(config);
        let dummy1 = DummyData::new();
        let key = scheduler.add(dummy1);
        scheduler.run(key.clone()).await.unwrap();
        sleep(check_frequency * 5).await;
        sleep(Duration::from_millis(500)).await; // Wait 500 milliseconds grace period
        scheduler.stop(key.clone()).await.unwrap();

        let stale_task = scheduler
            .stale_tasks
            .get(&key)
            .expect("schedulable data has been added previously under the 'key'");
        assert_eq!(stale_task.data.counter, 6);
    }

    #[test(tokio::test)]
    #[should_panic]
    async fn test_stop_wrong_id_tasks() {
        let ctx = Ctx::new();

        let intervals = vec![ScheduleInterval::new(
            Duration::from_secs(60 * 60),
            Duration::from_secs(1),
        )];

        let config = ScheduleConfig::new(intervals, ctx).set_is_clone(false);
        let mut scheduler: Scheduler<String, DummyData, Ctx> = Scheduler::new(config);

        scheduler
            .stop(String::from("Wrong Key"))
            .await
            .expect("Wrong Key and thus this should panic!");
    }

    #[test(tokio::test)]
    async fn test_task_time_out() {
        let max_sec = 5;
        let ctx = Ctx::new();

        let intervals = vec![ScheduleInterval::new(
            Duration::from_secs(max_sec),
            Duration::from_secs(1),
        )];

        let config = ScheduleConfig::new(intervals, ctx).set_is_clone(false);
        let mut scheduler = Scheduler::new(config);
        let dummy1 = DummyData::new();
        let key = scheduler.add(dummy1);
        scheduler.run(key.clone()).await.unwrap();
        sleep(Duration::from_secs(max_sec + 2)).await;

        let timed_out_tasks_lock = scheduler.config.ctx.timed_out_tasks.lock().await;
        assert_eq!(timed_out_tasks_lock.len(), 1);
    }

    #[derive(Clone, Debug)]
    struct Ctx2 {
        timed_out_tasks: Arc<Mutex<Vec<DummyData2>>>,
    }
    impl Ctx2 {
        pub fn new() -> Arc<Ctx2> {
            Arc::new(Ctx2 {
                timed_out_tasks: Arc::new(Mutex::new(Vec::new())),
            })
        }
    }

    #[derive(Clone, Debug)]
    struct DummyData2 {
        pub id: String,
        pub counter: i32,
        pub is_completed: bool,
    }
    impl DummyData2 {
        pub fn new() -> DummyData2 {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos(); // Using current time in nanoseconds to seed our "random" generator.

            let id = format!("{:x}", now); // Convert to a hexadecimal string.

            DummyData2 {
                id,
                counter: 0,
                is_completed: false,
            }
        }
    }

    #[async_trait]
    impl Schedulable<String, DummyData2, Ctx2> for DummyData2 {
        fn id(&self) -> String {
            self.id.clone()
        }
        async fn time_out(mut self, ctx: Arc<Ctx2>) {
            self.is_completed = true;
            let mut timed_out_tasks_lock = ctx.timed_out_tasks.lock().await;
            timed_out_tasks_lock.push(self);
        }
        async fn process(mut self, ctx: Arc<Ctx2>) -> (DummyData2, TaskStatus) {
            sleep(Duration::from_secs(10)).await;
            self.counter += 1;
            (self, TaskStatus::Running)
        }
        async fn complete(mut self, ctx: Arc<Ctx2>) {
            todo!("");
        }
    }

    #[test(tokio::test)]
    async fn test_stop_while_processing() {
        let ctx = Ctx2::new();

        let check_frequency = Duration::from_secs(2);
        let intervals = vec![ScheduleInterval::new(
            Duration::from_secs(60 * 60),
            check_frequency,
        )];

        let config = ScheduleConfig::new(intervals, ctx).set_is_clone(false);
        let mut scheduler = Scheduler::new(config);
        let dummy1 = DummyData2::new();
        let key = scheduler.add(dummy1);

        scheduler.run(key.clone()).await.unwrap();
        sleep(Duration::from_secs(1)).await; // Grace period of one second for the task really to start
        let start_time = Instant::now();
        scheduler.stop(key.clone()).await.unwrap();
        let stop_time = Instant::now();

        let elapsed = (stop_time - start_time);
        assert!(elapsed >= Duration::from_secs(9));
        assert!(elapsed <= Duration::from_secs(10));

        assert!(scheduler.stale_tasks.get(&key).unwrap().data.counter == 1);
    }
}
