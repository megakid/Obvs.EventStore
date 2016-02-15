using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using EventStore.ClientAPI;
using Obvs.Serialization;

namespace Obvs.EventStore
{
    public class MessageSource<TMessage> : IMessageSource<TMessage> 
        where TMessage : class
    {
        private readonly IDictionary<string, IMessageDeserializer<TMessage>> _deserializers;
        private readonly AsyncLazy<IEventStoreConnection> _lazyConnection;
        private readonly string _streamName;

        public MessageSource(AsyncLazy<IEventStoreConnection> lazyConnection, 
            string streamName,
            IEnumerable<IMessageDeserializer<TMessage>> deserializers)
        {
            _deserializers = deserializers.ToDictionary(d => d.GetTypeName());
            _lazyConnection = lazyConnection;
            _streamName = streamName;
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

                    var esStream = eventStoreConnection.SubscribeToStreamAsync(_streamName, true,
                        (sub, msg) => subject.OnNext(msg), (sub, reason, ex) => subject.OnError(ex)).Result;
                    
                    return new CompositeDisposable(subscription, esStream);
                });
            }
        }

        private TMessage Deserialize(ResolvedEvent message)
        {
            return _deserializers[message.Event.EventType].Deserialize(new MemoryStream(message.Event.Data));
        }

        public void Dispose()
        {
        }
    }
}