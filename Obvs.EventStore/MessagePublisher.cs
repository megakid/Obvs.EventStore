using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reactive.Disposables;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Obvs.MessageProperties;
using Obvs.Serialization;

namespace Obvs.EventStore
{
    public class MessagePublisher<TMessage> : IMessagePublisher<TMessage>
        where TMessage : class
    {
        private readonly string _topic;
        private readonly AsyncLazy<IEventStoreConnection> _lazyConnection;
        private readonly EventStoreProducerConfiguration _producerConfiguration;
        private readonly IMessageSerializer _serializer;
        private readonly IMessagePropertyProvider<TMessage> _propertyProvider;

        private IDisposable _disposable;
        private bool _disposed;
        private long _connected;

        private readonly bool _isJsonSerializer;

        public MessagePublisher(AsyncLazy<IEventStoreConnection> lazyConnection, EventStoreProducerConfiguration producerConfiguration, string topic, IMessageSerializer serializer, IMessagePropertyProvider<TMessage> propertyProvider)
        {
            _lazyConnection = lazyConnection;
            _topic = topic;
            _serializer = serializer;
            _propertyProvider = propertyProvider;
            _producerConfiguration = producerConfiguration;

            _isJsonSerializer = _serializer.GetType().FullName.ToUpperInvariant().Contains("JSON");
        }

        public Task PublishAsync(TMessage message)
        {
            if (_disposed)
            {
                throw new InvalidOperationException("Publisher has been disposed already.");
            }

            return Publish(message);
        }

        private Task Publish(TMessage message)
        {
            List<KeyValuePair<string, object>> properties = _propertyProvider.GetProperties(message).ToList();

            return Publish(message, properties);
        }

        private async Task Publish(TMessage message, List<KeyValuePair<string, object>> properties)
        {
            if (_disposed)
            {
                return;
            }

            Init();

            var messageType = message.GetType().Name;

            byte[] payload;
            using (MemoryStream stream = new MemoryStream())
            {
                _serializer.Serialize(stream, message);
                payload = stream.ToArray();
            }

            var eventStoreConnection = await _lazyConnection;

            await eventStoreConnection.AppendToStreamAsync(
                _topic,
                ExpectedVersion.Any,
                new EventData(Guid.NewGuid(), messageType, _isJsonSerializer, payload, null));
        }

        private void Init()
        {
            if (Interlocked.CompareExchange(ref _connected, 1, 0) == 0)
            {
                _disposable = Disposable.Create(() =>
                {
                    _disposed = true;
                });
            }
        }

        public void Dispose()
        {
            _disposable?.Dispose();
        }
    }
}