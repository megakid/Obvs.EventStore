using System;
using System.Collections.Generic;
using EventStore.ClientAPI;
using Obvs.Serialization;

namespace Obvs.EventStore.Configuration
{
    internal static class PublisherFactory
    {
        public static MessagePublisher<TMessage> Create<TMessage>(
            Lazy<IEventStoreConnection> lazyConnection,
            string streamName,
            IMessageSerializer messageSerializer,
            Func<TMessage, Dictionary<string, string>> propertyProvider)
            where TMessage : class
        {
            return new MessagePublisher<TMessage>(
                lazyConnection,
                streamName,
                messageSerializer,
                propertyProvider);
        }
    }
}