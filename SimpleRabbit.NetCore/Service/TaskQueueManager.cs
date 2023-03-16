using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleRabbit.NetCore.Service
{
    /// <summary>
    /// A simple thread-safe construct that manages tasks, offering parallelism by the key, but ordering within the same key.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    internal class TaskQueueManager<TKey>
    {
        private readonly Dictionary<TKey, Task> _tasks;
        private readonly object _lock = new object();

        /// <summary>
        /// Default constructor.
        /// </summary>
        public TaskQueueManager() : this(new Dictionary<TKey, Task>())
        {
        }

        /// <summary>
        /// Constructor that can use a pre-built dictionary of tasks as it's backing store. 
        /// It is not recommended to manipulate this dictionary once the <see cref="TaskQueueManager{TKey}"/> has been created; instead use functions within the <see cref="TaskQueueManager{TKey}"/>
        /// </summary>
        /// <param name="tasks">The existing collection to use as a backing store.</param>
        public TaskQueueManager(Dictionary<TKey, Task> tasks)
        {
            _tasks = tasks;
        }

        /// <summary>
        /// Add a task to the queue for the given key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="continuation"></param>
        public void EnqueueTask(TKey key, Func<Task, Task> continuation)
        {
            lock (_lock)
            {
                // Clean up the existing tasks
                var completedTaskKeys = _tasks
                    .Where(t => t.Value?.IsCompleted ?? true)
                    .Select(t => t.Key)
                    .ToArray();
                foreach (var completedTask in completedTaskKeys)
                {
                    _tasks.Remove(completedTask);
                }

                if (!_tasks.TryGetValue(key, out var currentTask))
                    currentTask = Task.CompletedTask;

                var newTask = currentTask.ContinueWith(continuation);
                _tasks[key] = newTask;
            }
        }
    }
}
