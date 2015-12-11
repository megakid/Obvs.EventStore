using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reactive.Disposables;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using kafka4net;
using Obvs.MessageProperties;
using Obvs.Serialization;

namespace Obvs.Kafka
{
    public class AsyncLazy<T> : Lazy<Task<T>>
    {
        public AsyncLazy(Func<T> valueFactory) :
            base(() => Task.Factory.StartNew(valueFactory), LazyThreadSafetyMode.ExecutionAndPublication)
        { }

        public AsyncLazy(Func<Task<T>> taskFactory) :
            base(() => taskFactory())
        { }

        public TaskAwaiter<T> GetAwaiter() { return Value.GetAwaiter(); }
    }

    public class MessagePublisher<TMessage> : IMessagePublisher<TMessage>
        where TMessage : class
    {
        private readonly KafkaConfiguration _kafkaConfiguration;
        private readonly string _topic;
        private readonly EventStoreProducerConfig _producerConfig;
        private readonly IMessageSerializer _serializer;
        private readonly IMessagePropertyProvider<TMessage> _propertyProvider;

        private IDisposable _disposable;
        private bool _disposed;
        private long _connected;
        private Producer _producer;
        private Lazy<IEventStoreConnection> _lazyConnection;

        private readonly bool _isJsonSerializer;

        public MessagePublisher(Lazy<IEventStoreConnection> lazyConnection, EventStoreProducerConfig producerConfig, string topic, IMessageSerializer serializer, IMessagePropertyProvider<TMessage> propertyProvider)
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

            await Connect();

            var messageType = message.GetType().Name;

            byte[] payload = null;
            using (MemoryStream stream = new MemoryStream())
            {
                _serializer.Serialize(stream, message);
                payload = stream.ToArray();
            }

            await _lazyConnection.Value.AppendToStreamAsync(
                _topic,
                ExpectedVersion.Any,
                new EventData(Guid.NewGuid(), messageType, _isJsonSerializer, payload, null));
            
        }

        private async Task Connect()
        {
            if (Interlocked.CompareExchange(ref _connected, 1, 0) == 0)
            {
                

                var producerConfiguration = new ProducerConfiguration(_topic,
                    batchFlushTime: TimeSpan.FromMilliseconds(50),
                    batchFlushSize: _producerConfig.BatchFlushSize,
                    requiredAcks: 1,
                    autoGrowSendBuffers: true,
                    sendBuffersInitialSize: 200,
                    maxMessageSetSizeInBytes: 1073741824,
                    producerRequestTimeout: null,
                    partitioner: null);

                _producer = new Producer(_kafkaConfiguration.SeedAddresses, producerConfiguration);

                await _producer.ConnectAsync();

                _disposable = Disposable.Create(() =>
                {
                    _disposed = true;
                    _producer.CloseAsync(TimeSpan.FromSeconds(2)).Wait();
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