using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using EventStore.ClientAPI;
using Obvs.EventStore.Serialization;
using Obvs.Serialization;

namespace Obvs.EventStore
{
    public class MessageSource<TMessage> : IMessageSource<TMessage> 
        where TMessage : class
    {
        private readonly IDictionary<string, IMessageDeserializer<TMessage>> _deserializers;
        private readonly Lazy<IEventStoreConnection> _lazyConnection;
        private readonly string _streamName;
        private readonly Func<Dictionary<string, string>, bool> _propertyFilter;
        private readonly JsonPropertySerializer _propertySerializer = new JsonPropertySerializer();
        private readonly bool _applyFilter;

        public MessageSource(Lazy<IEventStoreConnection> lazyConnection, 
            string streamName,
            IEnumerable<IMessageDeserializer<TMessage>> deserializers,
            Func<Dictionary<string, string>, bool> propertyFilter)
        {
            _deserializers = deserializers.ToDictionary(d => d.GetTypeName());
            _lazyConnection = lazyConnection;
            _streamName = streamName;
            _propertyFilter = propertyFilter;
            _applyFilter = _propertyFilter != null;
        }

        public IObservable<TMessage> Messages
        {
            get
            {
                return Observable.Create<TMessage>(observer =>
                {
                    return _lazyConnection.Value.SubscribeToStreamAsync(
                        _streamName, true,
                        (sub, msg) => TryDeserialize(observer, msg),
                        (sub, reason, ex) => observer.OnError(new Exception(reason.ToString(), ex))).Result;
                });
            }
        }

        private void TryDeserialize(IObserver<TMessage> observer, ResolvedEvent msg)
        {
            try
            {
                if (_applyFilter && _propertyFilter(GetProperties(msg.Event.Metadata)))
                {
                    observer.OnNext(Deserialize(msg));
                }
            }
            catch (Exception exception)
            {
                observer.OnError(exception);
            }
        }

        private Dictionary<string, string> GetProperties(byte[] metaData)
        {
            if (metaData == null || !metaData.Any())
            {
                return new Dictionary<string, string>();
            }
            using (var stream = new MemoryStream(metaData))
            {
                return _propertySerializer.Deserialize(stream);
            }
        }

        private TMessage Deserialize(ResolvedEvent message)
        {
            IMessageDeserializer<TMessage> deserializer;
            if (!_deserializers.TryGetValue(message.Event.EventType, out deserializer))
            {
                throw new Exception(string.Format("Missing deserializer for EventType '{0}'", message.Event.EventType));
            }
            return deserializer.Deserialize(new MemoryStream(message.Event.Data));
        }

        public void Dispose()
        {
        }
    }
}