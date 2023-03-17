using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using SimpleRabbit.NetCore.Service;

namespace SimpleRabbit.NetCore
{
    

    /// <summary>
    ///     A task queue based ordered dispatcher
    /// </summary>
    /// <typeparam name="TKey">The key to use for ordering</typeparam>
    /// <typeparam name="TValue">The value to work on</typeparam>
    public abstract class AsyncMessageHandler<TKey, TValue> : QueuingMessageHandler<TKey, AsyncMessageHandler<TKey, TValue>.Context>
    {
        private class DelegatingHandler : IProcessor
        {
            public AsyncMessageHandler<TKey, TValue> Delegation { get; set; }


            public bool CanProcess(string tag)
            {
                return Delegation.CanProcess(tag);
            }

            public TKey GetKey(BasicMessage message, out Context context)
            {
                var item = Delegation.Get(message);
                var key = Delegation.GetKey(item);
                context = new Context()
                {
                    Item = item,
                };
                return key;
            }

            public Task ProcessAsync(BasicMessage message, Context context)
            {
                return Delegation.ProcessAsync(context.Item);
            }
        }

        public class Context
        {
            public TValue Item { get; set; }
        }

        protected AsyncMessageHandler(ILogger<AsyncMessageHandler<TKey, TValue>> logger) :base(new DelegatingHandler(), logger)
        {
            (_handler as DelegatingHandler).Delegation = this;
        }

        protected AsyncMessageHandler(ILogger<AsyncMessageHandler<TKey, TValue>> logger, Dictionary<TKey, Task> dictionary) : base(new DelegatingHandler(), logger)
        {
            (_handler as DelegatingHandler).Delegation = this;
        }

        /// <summary>
        ///     Must be provided to decompose the message to a TValue e.g. perform any deserialisation or object creation.
        /// </summary>
        /// <param name="message"></param>
        /// <returns>The decomposed message</returns>
        protected abstract TValue Get(BasicMessage message);

        /// <summary>
        ///     Must be provided to extract the Key from the (decomposed) item.
        /// </summary>
        /// <param name="item"></param>
        /// <returns>The decomposed message</returns>
        protected abstract TKey GetKey(TValue item);

        protected abstract Task ProcessAsync(TValue item);
    }
}
