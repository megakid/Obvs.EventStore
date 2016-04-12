using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using NUnit.Framework;
using Obvs.ActiveMQ.Configuration;
using Obvs.Configuration;
using Obvs.EventStore.Configuration;
using Obvs.EventStore.Tests.Messages;
using Obvs.Serialization.Json.Configuration;

namespace Obvs.EventStore.Tests
{
    [TestFixture]
    public class TestServiceBus
    {
        [Test, Explicit]
        public async void TestServiceBusWithLocalEventStore()
        {
            var serviceBus = ServiceBus.Configure()
                .WithEventStoreSharedConnectionScope("ConnectTo=tcp://admin:changeit@127.0.0.1:1113", config => config
                    .WithEventStoreEndpoints<ITestMessage1>()
                        .Named("Obvs.EventStore.Test")
                        .AppendMessageProperties(message => null)
                        .FilterReceivedMessages(properties => true)
                        .ConnectToEventStore("ConnectTo=tcp://admin:changeit@127.0.0.1:1113")
                        .SerializedAsJson()
                        .AsClientAndServer())
                .UsingConsoleLogging()
                .Create();

            var fakeService = new AnonymousObserver<ITestMessage1>(msg =>
            {
                var command = msg as TestCommand;
                if (command != null)
                {
                    serviceBus.PublishAsync(new TestEvent {Id = command.Id});
                    return;
                }

                var request = msg as TestRequest;
                if (request != null)
                {
                    serviceBus.ReplyAsync(request, new TestResponse {Id = request.Id});
                }
            });

            var sub = serviceBus.Commands.OfType<ITestMessage1>()
                .Merge(serviceBus.Requests.OfType<ITestMessage1>())
                .ObserveOn(Scheduler.Default)
                .Subscribe(fakeService);

            var receivedEvent = await Observable.Create<TestEvent>(observer =>
            {
                var disposable = serviceBus.Events
                    .OfType<TestEvent>()
                    .ObserveOn(Scheduler.Default)
                    .Subscribe(observer);

                serviceBus.SendAsync(new TestCommand {Id = 123});

                return disposable;
            }).Take(1);

            var receivedResponse = await serviceBus.GetResponse<TestResponse>(new TestRequest {Id = 456});

            sub.Dispose();

            Assert.That(receivedEvent.Id, Is.EqualTo(123));
            Assert.That(receivedResponse.Id, Is.EqualTo(456));
        }
    }
}