using System;
using EventStore.ClientAPI;
using Obvs.MessageProperties;
using Obvs.Serialization;

namespace Obvs.EventStore.Configuration
{
    internal static class PublisherFactory
    {
        public static MessagePublisher<TMessage> Create<TMessage>(
            Lazy<IEventStoreConnection> lazyConnection,
            string streamName,
            IMessageSerializer messageSerializer,
            IMessagePropertyProvider<TMessage> propertyProvider = null)
            where TMessage : class
        {
            return new MessagePublisher<TMessage>(
                lazyConnection,
                streamName,
                messageSerializer,
                propertyProvider ?? new DefaultPropertyProvider<TMessage>());
        }
    }
}