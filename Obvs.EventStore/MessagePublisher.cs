using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Obvs.MessageProperties;
using Obvs.Serialization;

namespace Obvs.EventStore
{
    public class AsyncLazy<T> : Lazy<Task<T>>
    {
        public AsyncLazy(Func<T> valueFactory) :
            base(() => Task.Run(valueFactory), LazyThreadSafetyMode.ExecutionAndPublication)
        { }

        public AsyncLazy(Func<Task<T>> taskFactory) :
            base(() => taskFactory(), LazyThreadSafetyMode.ExecutionAndPublication)
        { }

        public TaskAwaiter<T> GetAwaiter() { return Value.GetAwaiter(); }
    }

    public class MessagePublisher<TMessage> : IMessagePublisher<TMessage>
        where TMessage : class
    {
        private readonly string _topic;
        private readonly EventStoreProducerConfig _producerConfig;
        private readonly IMessageSerializer _serializer;
        private readonly IMessagePropertyProvider<TMessage> _propertyProvider;

        private IDisposable _disposable;
        private bool _disposed;
        private long _connected;
        private AsyncLazy<IEventStoreConnection> _lazyConnection;

        private readonly bool _isJsonSerializer;

        public MessagePublisher(AsyncLazy<IEventStoreConnection> lazyConnection, EventStoreProducerConfig producerConfig, string topic, IMessageSerializer serializer, IMessagePropertyProvider<TMessage> propertyProvider)
        {
            _lazyConnection = lazyConnection;
            _topic = topic;
            _serializer = serializer;
            _propertyProvider = propertyProvider;
            _producerConfig = producerConfig;

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

            byte[] payload = null;
            using (MemoryStream stream = new MemoryStream())
            {
                _serializer.Serialize(stream, message);
                payload = stream.ToArray();
            }

            await (await _lazyConnection).AppendToStreamAsync(
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
            if (_disposable != null)
            {
                _disposable.Dispose();
            }
        }
    }
}