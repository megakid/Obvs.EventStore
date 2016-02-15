using System;
using System.Reflection;
using EventStore.ClientAPI;
using Obvs.Configuration;
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
        private bool _created;

        private readonly EventStoreConfiguration _eventStoreConfiguration;
        private readonly IMessageSerializer _serializer;
        private readonly IMessageDeserializerFactory _deserializerFactory;
        private readonly AsyncLazy<IEventStoreConnection> _lazyConnection;

        private readonly Func<Assembly, bool> _assemblyFilter;
        private readonly Func<Type, bool> _typeFilter;

        public EventStoreServiceEndpointProvider(string serviceName,
                                               EventStoreConfiguration eventStoreConfiguration,
                                               IMessageSerializer serializer,
                                               IMessageDeserializerFactory deserializerFactory,
                                               Func<Assembly, bool> assemblyFilter = null,
                                               Func<Type, bool> typeFilter = null)
            : base(serviceName)
        {
            _eventStoreConfiguration = eventStoreConfiguration;
            _serializer = serializer;
            _deserializerFactory = deserializerFactory;
            _assemblyFilter = assemblyFilter;
            _typeFilter = typeFilter;
            
            if (string.IsNullOrEmpty(_eventStoreConfiguration?.ConnectionString))
            {
                throw new InvalidOperationException(string.Format("For service endpoint '{0}', please specify a eventstore connection string to connect to. To do this you can use ConnectToEventStore() per endpoint", serviceName));
            }

            _lazyConnection = new AsyncLazy<IEventStoreConnection>(
                async () =>
                {
                    var esc = EventStoreConnection.Create(_eventStoreConfiguration.ConnectionString);
                    await esc.ConnectAsync();
                    return esc;
                });

        }

        public override IServiceEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse> CreateEndpoint()
        {
            if (_created)
            {
                throw new InvalidOperationException($"{nameof(EventStoreServiceEndpointProvider<TServiceMessage, TMessage, TCommand, TEvent, TRequest, TResponse>)} can only be used once");
            }

            _created = true;

            return new ServiceEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse>(
                    CreateSource<TRequest>(_lazyConnection, RequestsDestination),
                    CreateSource<TCommand>(_lazyConnection, CommandsDestination),
                    CreatePublisher<TEvent>(_lazyConnection, EventsDestination),
                    CreatePublisher<TResponse>(_lazyConnection, ResponsesDestination),
                    typeof(TServiceMessage));
        }

        private IMessageSource<T> CreateSource<T>(AsyncLazy<IEventStoreConnection> lazyConnection, string topic) where T : class, TMessage
        {
            return SourceFactory.Create<T, TServiceMessage>(lazyConnection, topic, _deserializerFactory, _assemblyFilter, _typeFilter);
        }

        private IMessagePublisher<T> CreatePublisher<T>(AsyncLazy<IEventStoreConnection> lazyConnection, string topic) where T : class, TMessage
        {
            return PublisherFactory.Create<T>(lazyConnection, topic, _serializer, null);
        }

        public override IServiceEndpointClient<TMessage, TCommand, TEvent, TRequest, TResponse> CreateEndpointClient()
        {
            if (_created)
            {
                throw new InvalidOperationException($"{nameof(EventStoreServiceEndpointProvider<TServiceMessage, TMessage, TCommand, TEvent, TRequest, TResponse>)} can only be used once");
            }

            _created = true;

            return new ServiceEndpointClient<TMessage, TCommand, TEvent, TRequest, TResponse>(
                    CreateSource<TEvent>(_lazyConnection, EventsDestination),
                    CreateSource<TResponse>(_lazyConnection, ResponsesDestination),
                    CreatePublisher<TRequest>(_lazyConnection, RequestsDestination),
                    CreatePublisher<TCommand>(_lazyConnection, CommandsDestination),
                    typeof(TServiceMessage));
        }

    }
}