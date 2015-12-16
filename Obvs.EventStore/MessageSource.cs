using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
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

        private readonly EventStoreSourceConfiguration _sourceConfig;

        public MessageSource(AsyncLazy<IEventStoreConnection> lazyConnection,
            EventStoreSourceConfiguration sourceConfig, 
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
                return Observable.Create<TMessage>(observer =>
                {
                    var subject = new Subject<ResolvedEvent>();

                    var subscription = subject
                        .Select(Deserialize)
                        .Subscribe(observer);

                    var eventStoreConnection = _lazyConnection.Value.Result;

                    var esStream = eventStoreConnection.SubscribeToStreamAsync(_topicName, true,
                        (sub, msg) => subject.OnNext(msg), (sub, reason, ex) => subject.OnError(ex)).Result;
                    
                    return new CompositeDisposable(subscription, esStream);
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