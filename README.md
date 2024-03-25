# Generic Task Scheduler
A powerful, flexible scheduling system for asynchronous tasks in Rust, leveraging tokio and standard Rust concurrency primitives for efficient, controlled task management.


## Features
- Dynamic Scheduling: Schedule tasks with varying frequencies and conditions.
- Conditional Cloning: Optimize performance by conditionally cloning task data.
- Graceful Task Management: Stop tasks gracefully and handle timeouts with ease.

## Usage
1) Implement the Schedulable trait for your task.
2) Create a ScheduleConfig with your desired scheduling intervals.
2) Use the Scheduler to manage and execute tasks.

## Example
See the provided documentation and example code for a detailed guide on how to integrate and use the scheduler in your projects.

## License
MIT License
