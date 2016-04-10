using System;
using System.Collections.Generic;
using System.Reflection;
using EventStore.ClientAPI;
using Obvs.Serialization;

namespace Obvs.EventStore.Configuration
{
    internal static class SourceFactory
    {
        public static MessageSource<TMessage> Create<TMessage, TServiceMessage>(Lazy<IEventStoreConnection> lazyConnection, string streamName, IMessageDeserializerFactory deserializerFactory, 
            Func<Dictionary<string, string>, bool> propertyFilter, Func<Assembly, bool> assemblyFilter = null, Func<Type, bool> typeFilter = null)
            where TMessage : class
            where TServiceMessage : class
        {
            return new MessageSource<TMessage>(
                lazyConnection,
                streamName,
                deserializerFactory.Create<TMessage, TServiceMessage>(assemblyFilter, typeFilter),
                propertyFilter);
        }
    }
}