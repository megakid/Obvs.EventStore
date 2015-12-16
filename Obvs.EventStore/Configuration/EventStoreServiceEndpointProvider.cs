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
        private readonly EventStoreSourceConfiguration _sourceConfiguration;
        private readonly EventStoreProducerConfiguration _producerConfiguration;
        private readonly IMessageSerializer _serializer;
        private readonly IMessageDeserializerFactory _deserializerFactory;
        private readonly AsyncLazy<IEventStoreConnection> _lazyConnection;

        private readonly Func<Assembly, bool> _assemblyFilter;
        private readonly Func<Type, bool> _typeFilter;

        public EventStoreServiceEndpointProvider(string serviceName,
                                               EventStoreConfiguration eventStoreConfiguration,
                                               EventStoreSourceConfiguration sourceConfiguration,
                                               EventStoreProducerConfiguration producerConfiguration,
                                               IMessageSerializer serializer,
                                               IMessageDeserializerFactory deserializerFactory,
                                               Func<Assembly, bool> assemblyFilter = null,
                                               Func<Type, bool> typeFilter = null)
            : base(serviceName)
        {
            _eventStoreConfiguration = eventStoreConfiguration;
            _sourceConfiguration = sourceConfiguration ?? new EventStoreSourceConfiguration();
            _producerConfiguration = producerConfiguration ?? new EventStoreProducerConfiguration();
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
                    CreateSource<TRequest>(_lazyConnection, _sourceConfiguration, RequestsDestination),
                    CreateSource<TCommand>(_lazyConnection, _sourceConfiguration, CommandsDestination),
                    CreatePublisher<TEvent>(_lazyConnection, _producerConfiguration, EventsDestination),
                    CreatePublisher<TResponse>(_lazyConnection, _producerConfiguration, ResponsesDestination),
                    typeof(TServiceMessage));
        }

        private IMessageSource<T> CreateSource<T>(AsyncLazy<IEventStoreConnection> lazyConnection, EventStoreSourceConfiguration sourceConfiguration, string topic) where T : class, TMessage
        {
            return DestinationFactory.CreateSource<T, TServiceMessage>(lazyConnection, sourceConfiguration, topic, _deserializerFactory, _assemblyFilter, _typeFilter);
        }

        private IMessagePublisher<T> CreatePublisher<T>(AsyncLazy<IEventStoreConnection> lazyConnection, EventStoreProducerConfiguration producerConfiguration, string topic) where T : class, TMessage
        {
            return DestinationFactory.CreatePublisher<T>(lazyConnection, producerConfiguration, topic, _serializer, null, null);
        }


        public override IServiceEndpointClient<TMessage, TCommand, TEvent, TRequest, TResponse> CreateEndpointClient()
        {
            if (_created)
            {
                throw new InvalidOperationException($"{nameof(EventStoreServiceEndpointProvider<TServiceMessage, TMessage, TCommand, TEvent, TRequest, TResponse>)} can only be used once");
            }

            _created = true;

            return new ServiceEndpointClient<TMessage, TCommand, TEvent, TRequest, TResponse>(
                    CreateSource<TEvent>(_lazyConnection, _sourceConfiguration, EventsDestination),
                    CreateSource<TResponse>(_lazyConnection, _sourceConfiguration, ResponsesDestination),
                    CreatePublisher<TRequest>(_lazyConnection, _producerConfiguration, RequestsDestination),
                    CreatePublisher<TCommand>(_lazyConnection, _producerConfiguration, CommandsDestination),
                    typeof(TServiceMessage));
        }

    }
}