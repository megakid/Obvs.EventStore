using System;
using System.Threading;
using EventStore.ClientAPI;

namespace Obvs.EventStore.Configuration
{
    internal static class EventStoreConnectionExtensions
    {
        public static Lazy<IEventStoreConnection> AsLazy(this IEventStoreConnection connection)
        {
            return new Lazy<IEventStoreConnection>(() =>
            {
                connection.ConnectAsync().Wait(TimeSpan.FromSeconds(10));
                return connection;
            }, LazyThreadSafetyMode.ExecutionAndPublication);
        }
    }
}