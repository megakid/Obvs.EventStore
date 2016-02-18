using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Disposables;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Obvs.EventStore.Serialization;
using Obvs.MessageProperties;
using Obvs.Serialization;

namespace Obvs.EventStore
{
    public class MessagePublisher<TMessage> : IMessagePublisher<TMessage>
        where TMessage : class
    {
        private readonly string _streamName;
        private readonly Lazy<IEventStoreConnection> _lazyConnection;
        private readonly IMessageSerializer _serializer;
        private readonly IMessagePropertyProvider<TMessage> _propertyProvider;

        private IDisposable _disposable;
        private bool _disposed;
        private long _connected;

        private readonly bool _isJsonSerializer;
        private readonly JsonMessageSerializer _metaDataSerializer;

        public MessagePublisher(Lazy<IEventStoreConnection> lazyConnection, string streamName, IMessageSerializer serializer, IMessagePropertyProvider<TMessage> propertyProvider)
        {
            _metaDataSerializer = new JsonMessageSerializer();
            _lazyConnection = lazyConnection;
            _streamName = streamName;
            _serializer = serializer;
            _propertyProvider = propertyProvider;

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
            var properties = _propertyProvider.GetProperties(message).ToArray();

            return Publish(message, properties);
        }

        private async Task Publish(TMessage message, KeyValuePair<string, object>[] properties)
        {
            if (_disposed)
            {
                return;
            }

            Init();

            var eventData = Serialize(message, properties);

            await AppendToStreamAsync(eventData);
        }

        private EventData Serialize(TMessage message, KeyValuePair<string, object>[] properties)
        {
            var payload = GetPayload(message);
            var metaData = GetMetaData(properties);
            var eventData = new EventData(Guid.NewGuid(), message.GetType().Name, _isJsonSerializer, payload, metaData);
            return eventData;
        }

        private async Task<WriteResult> AppendToStreamAsync(EventData eventData)
        {
            return await _lazyConnection.Value.AppendToStreamAsync(_streamName, ExpectedVersion.Any, eventData);
        }

        private byte[] GetMetaData(KeyValuePair<string, object>[] properties)
        {
            if (!properties.Any())
            {
                return null;
            }

            using (var stream = new MemoryStream())
            {
                _metaDataSerializer.Serialize(stream, properties);
                return stream.ToArray();
            }
        }

        private byte[] GetPayload(TMessage message)
        {
            using (var stream = new MemoryStream())
            {
                _serializer.Serialize(stream, message);
                return stream.ToArray();
            }
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