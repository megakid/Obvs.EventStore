using System;
using System.Reactive.Concurrency;
using System.Reflection;
using EventStore.ClientAPI;
using Obvs.MessageProperties;
using Obvs.Serialization;

namespace Obvs.EventStore.Configuration
{
    public static class DestinationFactory
    {
        public static MessagePublisher<TMessage> CreatePublisher<TMessage>(
            AsyncLazy<IEventStoreConnection> lazyConnection, 
            EventStoreProducerConfiguration producerConfiguration,
            string topic, 
            IMessageSerializer messageSerializer, 
            IScheduler scheduler,
            IMessagePropertyProvider<TMessage> propertyProvider = null) 
            where TMessage : class
        {
            return new MessagePublisher<TMessage>(
                lazyConnection,
                producerConfiguration,
                topic,
                messageSerializer,
                propertyProvider ?? new DefaultPropertyProvider<TMessage>());
        }

        public static MessageSource<TMessage> CreateSource<TMessage, TServiceMessage>(
            AsyncLazy<IEventStoreConnection> lazyConnection,
            EventStoreSourceConfiguration sourceConfiguration,
            string topic, 
            IMessageDeserializerFactory deserializerFactory,
            Func<Assembly, bool> assemblyFilter = null, 
            Func<Type, bool> typeFilter = null)
            where TMessage : class
            where TServiceMessage : class
        {
            return new MessageSource<TMessage>(
                lazyConnection,
                sourceConfiguration,
                topic,
                deserializerFactory.Create<TMessage, TServiceMessage>(assemblyFilter, typeFilter));
        }
    }
}