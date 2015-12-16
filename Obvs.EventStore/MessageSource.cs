using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using EventStore.ClientAPI;
using Obvs.Serialization;
using ProtoBuf;

namespace Obvs.EventStore
{
    public class MessageSource<TMessage> : IMessageSource<TMessage> 
        where TMessage : class
    {
        private readonly IDictionary<string, IMessageDeserializer<TMessage>> _deserializers;
        private readonly AsyncLazy<IEventStoreConnection> _lazyConnection;
        private readonly string _topicName;

        private readonly EventStoreSourceConfig _sourceConfig;

        public MessageSource(AsyncLazy<IEventStoreConnection> lazyConnection,
            EventStoreSourceConfig sourceConfig, 
            string topicName,
            IEnumerable<IMessageDeserializer<TMessage>> deserializers)
        {
            _deserializers = deserializers.ToDictionary(d => d.GetTypeName());
            _lazyConnection = lazyConnection;
            _topicName = topicName;
            _sourceConfig = sourceConfig;
        }

        public IObservable<TMessage> Messages
        {
            get
            {
                return Observable.Create<TMessage>(async observer =>
                {
                    var subject = new Subject<ResolvedEvent>();

                    var subscription = await (await _lazyConnection).SubscribeToStreamAsync(_topicName, true,
                        (sub, msg) => subject.OnNext(msg), (sub, reason, ex) => subject.OnError(ex));

                    return subject
                        .Select(Deserialize)
                        .Finally(() => { try { subscription.Dispose(); } catch { } })
                        .Subscribe(observer);
                });
            }
        }

        private TMessage Deserialize(ResolvedEvent message)
        {
            IMessageDeserializer<TMessage> deserializer = _deserializers[message.Event.EventType];

            TMessage deserializedMessage = deserializer.Deserialize(new MemoryStream(message.Event.Data));

            return deserializedMessage;
        }

        public void Dispose()
        {
        }
    }
}