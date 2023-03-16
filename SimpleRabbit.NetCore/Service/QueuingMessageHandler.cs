using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleRabbit.NetCore.Service
{
    /// <summary>
    /// A message handler that enqueues messages based on a given key, offering parallelism across individual keys, but ensuring that messages for the same key are handled in order.
    /// </summary>
    /// <typeparam name="TKey">The type of the key to be used, usually a <see cref="string"/>.</typeparam>
    /// <typeparam name="TContext">The type of a context that can be passed through the <see cref="IProcessor"/>, between <see cref="IProcessor.GetKeyAsync(BasicMessage, out TContext)"/> and <see cref="IProcessor.ProcessAsync(BasicMessage, ref TContext)"/>.</typeparam>
    public class QueuingMessageHandler<TKey, TContext> : IMessageHandler
    {
        /// <summary>
        /// This provides the logic for the <see cref="QueuingMessageHandler{TKey, TContext}"/> when it receives a message.
        /// </summary>
        public interface IProcessor
        {
            /// <inheritdoc cref="IMessageHandler.CanProcess(string)"/>
            bool CanProcess(string tag);

            /// <summary>
            /// Read a message and determine the key for the message, to facilitate ordered processing.
            /// This key will allow the <see cref="QueuingMessageHandler{TKey, TContext}"/> to ensure that messages for the same key are handled in order.
            /// </summary>
            /// <param name="message">The RabbitMQ message.</param>
            /// <param name="context">A context that can be created for this message, which will be passed to the <see cref="ProcessAsync(BasicMessage, ref TContext)"/> function.</param>
            /// <returns>An awaitable <see cref="Task"/> that contains the key.</returns>
            Task<TKey> GetKeyAsync(BasicMessage message, out TContext context);

            /// <summary>
            /// Process the given message.
            /// </summary>
            /// <param name="message">The RabbitMQ message. It is possible to <see cref="BasicMessage.Ack"/> or <see cref="BasicMessage.Nack(bool)"/> the message at this point, and the <see cref="QueuingMessageHandler{TKey, TContext}"/> will skip sending an ACK (or NACK) to RabbitMQ.</param>
            /// <param name="context">The context for this message, created during the <see cref="GetKeyAsync(BasicMessage, out TContext)"/> call.</param>
            /// <returns>An awaitable <see cref="Task"/> that will complete when the work has been performed.</returns>
            Task ProcessAsync(BasicMessage message, TContext context);
        }

        private readonly TaskQueueManager<TKey> _queueManager;
        private protected readonly IProcessor _handler; // Private protected to allow the AsyncMessageHandler to get at it
        private readonly ILogger _logger;

        /// <summary>
        /// Create an instance of the <see cref="QueuingMessageHandler{TKey, TContext}"/> using the default <see cref="TaskQueueManager{TKey}"/>.
        /// </summary>
        /// <param name="handler">The handler instance that contains the logic for the message handler.</param>
        /// <param name="logger">A logger to use in the event of errors.</param>
        public QueuingMessageHandler(IProcessor handler, ILogger<QueuingMessageHandler<TKey, TContext>> logger)
        {
            _queueManager = new TaskQueueManager<TKey>();
            _handler = handler;
            _logger = logger;
        }

        /// <summary>
        /// Create an instance of the <see cref="QueuingMessageHandler{TKey, TContext}"/> using a <see cref="TaskQueueManager{TKey}"/> instantiated with the provided dictionary.
        /// </summary>
        /// <param name="handler">The handler instance that contains the logic for the message handler.</param>
        /// <param name="logger">A logger to use in the event of errors.</param>
        public QueuingMessageHandler(IProcessor handler, ILogger<QueuingMessageHandler<TKey, TContext>> logger, Dictionary<TKey, Task> tasks)
        {
            _queueManager = new TaskQueueManager<TKey>(tasks);
            _handler = handler;
            _logger = logger;
        }

        /// <inheritdoc/>
        public bool CanProcess(string tag) => _handler.CanProcess(tag);

        /// <inheritdoc/>
        public bool Process(BasicMessage message)
        {
            Task.Run(async () =>
            {
                var key = await _handler.GetKeyAsync(message, out var context);
                if (key == null)
                {
                    if (!message.IsHandled)
                    {
                        _logger.LogInformation($"Message ignored {message.Properties?.MessageId} -> {message.Body}, no key");
                        message.Ack();
                    }
                    return;
                }

                _queueManager.EnqueueTask(key, previousTask => ProcessMessage(previousTask, message, key, context));
            });
            return false;
        }

        /// <summary>
        /// This is the delegate that will be invoked by the <see cref="TaskQueueManager{TKey}"/> when it is time to handle a message.
        /// </summary>
        /// <param name="previousTask">The previous task for the given key.</param>
        /// <param name="message">The RabbitMQ message.</param>
        /// <param name="key">The key for the given invocation, determined by the <see cref="IProcessor.GetKeyAsync(BasicMessage, out TContext)"/> function.</param>
        /// <param name="context">A context that can be used to share details between the <see cref="IProcessor.GetKeyAsync(BasicMessage, out TContext)"/> and <see cref="IProcessor.ProcessAsync(BasicMessage, ref TContext)"/> functions.</param>
        /// <returns>An awaitable task, that will be complete once all processing has finished for a given message.</returns>
        /// <exception cref="Exception">This function will log an error in the event of an exception within the handler or communicating with Rabbit, and will rethrow it.</exception>
        private async Task ProcessMessage(Task previousTask, BasicMessage message, TKey key, TContext context)
        {
            await Task.Yield();

            if (!previousTask.IsCompletedSuccessfully)
            {
                if (!message.IsHandled)
                    message.Nack();
                throw new Exception($"Processing chain aborted for {key}");
            }

            try
            {
                await _handler.ProcessAsync(message, context);
                if (!message.IsHandled)
                    message.Ack();
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"Couldn't process: {e.Message} key: {key} tag: ({message.DeliveryTag})");
                if (e is AggregateException agg)
                {
                    foreach (var ex in agg.InnerExceptions)
                    {
                        _logger.LogError(ex, ex.Message);
                    }
                }

                message.ErrorAction();
                throw;
            }
        }
    }
}
