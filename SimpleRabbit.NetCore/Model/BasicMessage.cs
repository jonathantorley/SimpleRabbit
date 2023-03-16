﻿using System;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace SimpleRabbit.NetCore
{
    public class BasicMessage
    {

        public BasicMessage(BasicDeliverEventArgs deliveryArgs, IModel channel, string queue, Action registerError)
        {
            DeliveryArgs = deliveryArgs;

            // client 6.0 - this needs to be read immediately, and stored as it is no longer thread safe.
            RawBody = DeliveryArgs?.Body.ToArray();
            Channel = channel;
            Queue = queue;
            ErrorAction = registerError;
        }

        public BasicDeliverEventArgs DeliveryArgs { get; }
        public IModel Channel { get; }
        public string Queue { get; }
        public Action ErrorAction { get; }

        public byte[] RawBody { get; }
        public string Body => Encoding.UTF8.GetString(RawBody);
        public IBasicProperties Properties => DeliveryArgs?.BasicProperties;
        public ulong DeliveryTag => DeliveryArgs?.DeliveryTag ?? 0;
        public string ConsumerTag => DeliveryArgs?.ConsumerTag;
        public string MessageId => Properties?.MessageId;
        public string CorrelationId => Properties?.CorrelationId;
        public IDictionary<string, object> Headers => Properties?.Headers;
        
        /// <summary>
        /// True if the message has been ACK'd or NACK'd (i.e. "handled"), false otherwise.
        /// </summary>
        public bool IsHandled { get; private set; } = false;

        public void Ack()
        {
            Channel?.BasicAck(DeliveryTag, false);
            IsHandled = true;
        }

        public void Nack(bool requeue = true)
        {
            Channel?.BasicNack(DeliveryTag, false, requeue);
            IsHandled = true;
        }
    }
}
