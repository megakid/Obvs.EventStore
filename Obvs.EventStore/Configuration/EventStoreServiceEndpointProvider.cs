using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using EventStore.ClientAPI;
using Obvs.Configuration;
using Obvs.EventStore.Serialization;
using Obvs.Serialization;

namespace Obvs.EventStore.Configuration
{
    public class EventStoreServiceEndpointProvider<TServiceMessage, TMessage, TCommand, TEvent, TRequest, TResponse> : ServiceEndpointProviderBase<TMessage, TCommand, TEvent, TRequest, TResponse>
        where TMessage : class
        where TServiceMessage : class
        where TCommand : class, TMessage
        where TEvent : class, TMessage
        where TRequest : class, TMessage
        where TResponse : class, TMessage
    {
        private readonly EventStoreConfiguration _configuration;
        private readonly IMessageSerializer _serializer;
        private readonly IMessageDeserializerFactory _deserializerFactory;

        private readonly Func<Assembly, bool> _assemblyFilter;
        private readonly Func<Type, bool> _typeFilter;
        private readonly Func<Dictionary<string, string>, bool> _propertyFilter;
        private readonly Func<TMessage, Dictionary<string, string>> _propertyProvider;
        private readonly Lazy<IEventStoreConnection> _sharedConnection;
        private readonly List<Tuple<string, Type>> _projectionStreams;

        public EventStoreServiceEndpointProvider(string serviceName, EventStoreConfiguration configuration, 
            IMessageSerializer serializer, IMessageDeserializerFactory deserializerFactory, 
            Func<Assembly, bool> assemblyFilter = null, Func<Type, bool> typeFilter = null, 
            Func<Dictionary<string, string>, bool> propertyFilter = null, 
            Func<TMessage, Dictionary<string, string>> propertyProvider = null, 
            List<Tuple<string, Type>> projectionStreams = null, 
            Lazy<IEventStoreConnection> sharedConnection = null)
            : base(serviceName)
        {
            _configuration = configuration;
            _serializer = serializer;
            _deserializerFactory = deserializerFactory;
            _assemblyFilter = assemblyFilter;
            _typeFilter = typeFilter;
            _propertyFilter = propertyFilter;
            _propertyProvider = propertyProvider;
            _sharedConnection = sharedConnection;
            _projectionStreams = projectionStreams ?? new List<Tuple<string, Type>>();

            if (string.IsNullOrEmpty(_configuration?.ConnectionString))
            {
                throw new InvalidOperationException(string.Format("For service endpoint '{0}', please specify a eventstore connection string to connect to. To do this you can use ConnectToEventStore() per endpoint", serviceName));
            }
        }

        public override IServiceEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse> CreateEndpoint()
        {
            var lazyConnection = _sharedConnection ?? EventStoreConnection.Create(_configuration.ConnectionString).AsLazy();

            return new ServiceEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse>(
                    CreateSource<TRequest>(lazyConnection, RequestsDestination),
                    CreateSource<TCommand>(lazyConnection, CommandsDestination),
                    CreatePublisher<TEvent>(lazyConnection, EventsDestination),
                    CreatePublisher<TResponse>(lazyConnection, ResponsesDestination),
                    typeof(TServiceMessage));
        }

        private IMessageSource<T> CreateSource<T>(Lazy<IEventStoreConnection> lazyConnection, string topic) where T : class, TMessage
        {
            return SourceFactory.Create<T, TServiceMessage>(lazyConnection, topic, _deserializerFactory, _propertyFilter, _assemblyFilter, _typeFilter);
        }

        private IMessagePublisher<T> CreatePublisher<T>(Lazy<IEventStoreConnection> lazyConnection, string topic) where T : class, TMessage
        {
            return PublisherFactory.Create<T>(lazyConnection, topic, _serializer, _propertyProvider);
        }

        public override IServiceEndpointClient<TMessage, TCommand, TEvent, TRequest, TResponse> CreateEndpointClient()
        {
           var lazyConnection = _sharedConnection ?? EventStoreConnection.Create(_configuration.ConnectionString).AsLazy();

            return new ServiceEndpointClient<TMessage, TCommand, TEvent, TRequest, TResponse>(
                    CreateEventsSource(lazyConnection),
                    CreateSource<TResponse>(lazyConnection, ResponsesDestination),
                    CreatePublisher<TRequest>(lazyConnection, RequestsDestination),
                    CreatePublisher<TCommand>(lazyConnection, CommandsDestination),
                    typeof(TServiceMessage));
        }

        private IMessageSource<TEvent> CreateEventsSource(Lazy<IEventStoreConnection> lazyConnection)
        {
            var eventSources = new List<IMessageSource<TEvent>>
            {
                CreateSource<TEvent>(lazyConnection, EventsDestination)
            };
            eventSources.AddRange(
                _projectionStreams.Select(projectionStream =>
                    new MessageSource<TEvent>(lazyConnection,
                        projectionStream.Item1,
                        new[]
                        {
                            new JsonProjectionDeserializer<TEvent>(projectionStream.Item2)
                        },
                        _propertyFilter)));

            return eventSources.Count == 1
                ? eventSources.Single()
                : new MergedMessageSource<TEvent>(eventSources);
        }
    }
}