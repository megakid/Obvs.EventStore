using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
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
        public async Task TestServiceBusWithLocalEventStore()
        {
            // run EventStore on Docker and then edit below for local address:port that maps to 1113
            var serviceBus = ServiceBus.Configure()
                .WithEventStoreSharedConnectionScope("ConnectTo=tcp://admin:changeit@192.168.99.100:32777", config => config
                    .WithEventStoreEndpoints<ITestMessage1>()
                        .Named("Obvs.EventStore.Test")
                        .AppendMessageProperties(message => null)
                        .FilterReceivedMessages(properties => true)
                        .UseSharedConnection()
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