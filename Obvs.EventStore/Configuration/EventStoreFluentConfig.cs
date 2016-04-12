using System;
using System.Collections.Generic;
using System.Reflection;
using Obvs.Configuration;
using Obvs.Serialization;

namespace Obvs.EventStore.Configuration
{
    public interface ICanSpecifyEventStoreServiceName<TMessage, TCommand, TEvent, TRequest, TResponse>
        where TMessage : class
        where TCommand : class, TMessage
        where TEvent : class, TMessage
        where TRequest : class, TMessage
        where TResponse : class, TMessage
    {
        ICanSpecifyEventStoreConnection<TMessage, TCommand, TEvent, TRequest, TResponse> Named(string serviceName);
    }

    public interface ICanSpecifyEventStoreConnection<TMessage, TCommand, TEvent, TRequest, TResponse> : 
        ICanSpecifyEventStoreServiceName<TMessage, TCommand, TEvent, TRequest, TResponse>, 
        ICanSpecifyEndpointSerializers<TMessage, TCommand, TEvent, TRequest, TResponse>,
        ICanSpecifyEventStoreMessageFiltering<TMessage, TCommand, TEvent, TRequest, TResponse>,
        ICanSpecifyEventStoreProjectionSubscribe<TMessage, TCommand, TEvent, TRequest, TResponse>
        where TMessage : class
        where TCommand : class, TMessage
        where TEvent : class, TMessage
        where TRequest : class, TMessage
        where TResponse : class, TMessage
    {
        ICanSpecifyEndpointSerializers<TMessage, TCommand, TEvent, TRequest, TResponse> ConnectToEventStore(string connectionString);
    }

    public interface ICanSpecifyEventStoreMessageFiltering<TMessage, TCommand, TEvent, TRequest, TResponse>
        where TMessage : class
        where TCommand : class, TMessage
        where TEvent : class, TMessage
        where TRequest : class, TMessage
        where TResponse : class, TMessage
    {
        ICanSpecifyEventStoreConnection<TMessage, TCommand, TEvent, TRequest, TResponse> FilterReceivedMessages(
            Func<Dictionary<string, string>, bool> propertyFilter);

        ICanSpecifyEventStoreConnection<TMessage, TCommand, TEvent, TRequest, TResponse> AppendMessageProperties(
            Func<TMessage, Dictionary<string, string>> propertyProvider);
    }

    public interface ICanSpecifyEventStoreProjectionSubscribe<TMessage, TCommand, TEvent, TRequest, TResponse>
        where TMessage : class
        where TCommand : class, TMessage
        where TEvent : class, TMessage
        where TRequest : class, TMessage
        where TResponse : class, TMessage
    {
        /// <summary>
        /// Subscribe to an EventStore projection stream and merge it into the Events observable.
        /// </summary>
        /// <typeparam name="T">Type to deserialize to. This must be a TEvent as it is merged into Events observable.</typeparam>
        /// <param name="streamName">Name of the projection stream in EventStore</param>
        /// <returns></returns>
        ICanSpecifyEventStoreConnection<TMessage, TCommand, TEvent, TRequest, TResponse> SubscribeToProjectionStream<T>(string streamName) where T : TEvent;
    }

    internal class EventStoreFluentConfig<TServiceMessage, TMessage, TCommand, TEvent, TRequest, TResponse> :
        ICanSpecifyEventStoreConnection<TMessage, TCommand, TEvent, TRequest, TResponse>,
        ICanCreateEndpointAsClientOrServer<TMessage, TCommand, TEvent, TRequest, TResponse>
        where TMessage : class
        where TServiceMessage : class 
        where TCommand : class, TMessage 
        where TEvent : class, TMessage
        where TRequest : class, TMessage 
        where TResponse : class, TMessage
    {
        private readonly ICanAddEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse> _canAddEndpoint;
        private string _serviceName;
        private string _connectiongString;
        private IMessageSerializer _serializer;
        private IMessageDeserializerFactory _deserializerFactory;
        private Func<Assembly, bool> _assemblyFilter;
        private Func<Type, bool> _typeFilter;
        private Func<Dictionary<string, string>, bool> _propertyFilter;
        private Func<TMessage, Dictionary<string, string>> _propertyProvider;
        private readonly List<Tuple<string, Type>> _projectionStreams = new List<Tuple<string, Type>>();

        public EventStoreFluentConfig(ICanAddEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse> canAddEndpoint)
        {
            _canAddEndpoint = canAddEndpoint;
        }

        public ICanSpecifyEventStoreConnection<TMessage, TCommand, TEvent, TRequest, TResponse> Named(string serviceName)
        {
            _serviceName = serviceName;
            return this;
        }

        public ICanCreateEndpointAsClientOrServer<TMessage, TCommand, TEvent, TRequest, TResponse> FilterMessageTypeAssemblies(Func<Assembly, bool> assemblyFilter = null, Func<Type, bool> typeFilter = null)
        {
            _assemblyFilter = assemblyFilter;
            _typeFilter = typeFilter;
            return this;
        }

        public ICanAddEndpointOrLoggingOrCorrelationOrCreate<TMessage, TCommand, TEvent, TRequest, TResponse> AsClient()
        {
            return _canAddEndpoint.WithClientEndpoints(CreateProvider());
        }

        public ICanAddEndpointOrLoggingOrCorrelationOrCreate<TMessage, TCommand, TEvent, TRequest, TResponse> AsServer()
        {
            return _canAddEndpoint.WithServerEndpoints(CreateProvider());
        }

        public ICanAddEndpointOrLoggingOrCorrelationOrCreate<TMessage, TCommand, TEvent, TRequest, TResponse> AsClientAndServer()
        {
            return _canAddEndpoint.WithEndpoints(CreateProvider());
        }

        private EventStoreServiceEndpointProvider<TServiceMessage, TMessage, TCommand, TEvent, TRequest, TResponse> CreateProvider()
        {
            return new EventStoreServiceEndpointProvider<TServiceMessage, TMessage, TCommand, TEvent, TRequest, TResponse>(
                _serviceName, 
                new EventStoreConfiguration(_connectiongString),
                _serializer, 
                _deserializerFactory, 
                _assemblyFilter, 
                _typeFilter,
                _propertyFilter,
                _propertyProvider,
                _projectionStreams,
                EventStoreFluentConfigContext.SharedConnection);
        }

        public ICanSpecifyEndpointSerializers<TMessage, TCommand, TEvent, TRequest, TResponse> ConnectToEventStore(string connectionString)
        {
            _connectiongString = connectionString;
            return this;
        }
        
        public ICanCreateEndpointAsClientOrServer<TMessage, TCommand, TEvent, TRequest, TResponse> SerializedWith(IMessageSerializer serializer, IMessageDeserializerFactory deserializerFactory)
        {
            _serializer = serializer;
            _deserializerFactory = deserializerFactory;
            return this;
        }

        public ICanSpecifyEventStoreConnection<TMessage, TCommand, TEvent, TRequest, TResponse> FilterReceivedMessages(Func<Dictionary<string, string>, bool> propertyFilter)
        {
            _propertyFilter = propertyFilter;
            return this;
        }

        public ICanSpecifyEventStoreConnection<TMessage, TCommand, TEvent, TRequest, TResponse> AppendMessageProperties(Func<TMessage, Dictionary<string, string>> propertyProvider)
        {
            _propertyProvider = propertyProvider;
            return this;
        }

        public ICanSpecifyEventStoreConnection<TMessage, TCommand, TEvent, TRequest, TResponse> SubscribeToProjectionStream<T>(string streamName) where T : TEvent
        {
            _projectionStreams.Add(Tuple.Create(streamName, typeof(T)));
            return this;
        }
    }
}