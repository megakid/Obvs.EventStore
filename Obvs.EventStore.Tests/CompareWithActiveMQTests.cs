using System;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using NLog.Config;
using NLog.Targets;
using NUnit.Framework;
using Obvs.Configuration;
using Obvs.EventStore.Configuration;
using Obvs.Serialization.Json.Configuration;
using Obvs.Types;

namespace Obvs.EventStore.Tests
{
    [TestFixture]
    public class CompareWithActiveMqTests
    {
        private const string EventStoreConnectionString = "ConnectTo=tcp://admin:changeit@127.0.0.1:1113";
        private Stopwatch _sw;

        [SetUp]
        public void SetUp()
        {
            var config = new LoggingConfiguration();
            var consoleTarget = new ColoredConsoleTarget {Layout = @"${date:format=HH\:mm\:ss} ${logger} ${message}"};
            var rule1 = new LoggingRule("*", LogLevel.Trace, consoleTarget);
            config.LoggingRules.Add(rule1);
            LogManager.Configuration = config;
        }


        [Explicit]
        [Test]
        [TestCase(2000, 10)]
        public async Task TestServiceBusWithRemoteEventStore(int count, int watchers)
        {
            var tasks = Enumerable.Range(0, watchers)
                .Select(i => StartWatcher(i, count))
                .ToArray();

            await SendCommands(count);

            await Task.WhenAll(tasks);
        }

        private async Task SendCommands(int count)
        {
            IServiceBus serviceBus = ServiceBus.Configure()
                .WithEventStoreEndpoints<ITestMessage1>()
                    .Named("Obvs.TestService")
                    .ConnectToEventStore(EventStoreConnectionString)
                    .SerializedAsJson()
                    .AsClient()
                .UsingConsoleLogging()
                .Create();

            Stopwatch sw = Stopwatch.StartNew();

            var sendTasks = Enumerable.Range(0, count)
                .Select(i => serviceBus.SendAsync(new TestCommand { Id = i }));

            await Task.WhenAll(sendTasks);

            Console.WriteLine($"###$$$$### Sends: {sw.ElapsedMilliseconds}ms");
            _sw = Stopwatch.StartNew();

            ((IDisposable)serviceBus).Dispose();
        }

        private Task StartWatcher(int i, int count)
        {
            IServiceBus serviceBus = ServiceBus.Configure()
                .WithEventStoreEndpoints<ITestMessage1>()
                .Named("Obvs.TestService")
                .ConnectToEventStore(EventStoreConnectionString)
                .SerializedAsJson()
                .AsServer()
                .UsingConsoleLogging()
                .Create();

            double?[] times = new double?[count];
            long[] received = {0};

            var dis = serviceBus.Commands.OfType<TestCommand>().Subscribe(x =>
            {
                var increment = Interlocked.Increment(ref received[0]);
                if (increment%100 == 0)
                    Console.WriteLine($"Watcher {i}: {increment} msgs");
                var ms = (Stopwatch.GetTimestamp() - x.Ticks)/((double) Stopwatch.Frequency/1000);
                times[x.Id] = ms;
            });

            return Task.Run(() =>
            {
                SpinWait.SpinUntil(() => Interlocked.Read(ref received[0]) == count);

                Console.WriteLine(
                    $"******* Watcher {i}: Total {_sw.ElapsedMilliseconds}ms ({count} msgs), Min/Avg/Max (ms) = {times.Min(d => d.Value):0}/{times.Average(d => d.Value):0}/{times.Max(d => d.Value):0}");

                dis.Dispose();
                ((IDisposable) serviceBus).Dispose();
            });
        }


        public interface ITestMessage1 : IMessage
        {
        }

        public interface ITestMessage2 : IMessage
        {
        }

        public class TestEvent : ITestMessage1, IEvent
        {
            public int Id { get; set; }

            public override string ToString()
            {
                return string.Format("{0}[Id={1}]", GetType().Name, Id);
            }
        }

        public class TestCommand : ITestMessage1, ICommand
        {
            public int Id { get; set; }

            public long Ticks { get; set; } = Stopwatch.GetTimestamp();

            public override string ToString()
            {
                return string.Format("{0}[Id={1}]", GetType().Name, Id);
            }
        }

        public class Test2Event : ITestMessage1, IEvent
        {
            public int Id { get; set; }

            public override string ToString()
            {
                return string.Format("{0}[Id={1}]", GetType().Name, Id);
            }
        }

        public class Test2Command : ITestMessage1, ICommand
        {
            public int Id { get; set; }

            public override string ToString()
            {
                return string.Format("{0}[Id={1}]", GetType().Name, Id);
            }
        }

        public class TestCommand2 : ITestMessage1, ICommand
        {
            public int Id { get; set; }

            public override string ToString()
            {
                return string.Format("{0}[Id={1}]", GetType().Name, Id);
            }
        }

        public class TestCommand3 : ITestMessage1, ICommand
        {
            public int Id { get; set; }

            public override string ToString()
            {
                return string.Format("{0}[Id={1}]", GetType().Name, Id);
            }
        }

        public class TestRequest : ITestMessage1, IRequest
        {
            public int Id { get; set; }

            public override string ToString()
            {
                return string.Format("{0}[Id={1}]", GetType().Name, Id);
            }

            public string RequestId { get; set; }
            public string RequesterId { get; set; }
        }

        public class TestResponse : ITestMessage1, IResponse
        {
            public int Id { get; set; }

            public override string ToString()
            {
                return string.Format("{0}[Id={1}]", GetType().Name, Id);
            }

            public string RequestId { get; set; }
            public string RequesterId { get; set; }
        }
    }
}
