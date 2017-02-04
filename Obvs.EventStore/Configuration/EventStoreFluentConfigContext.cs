using System;
using EventStore.ClientAPI;

namespace Obvs.EventStore.Configuration
{
    internal static class EventStoreFluentConfigContext
    {
        [ThreadStatic]
        internal static Lazy<IEventStoreConnection> SharedConnection;
    }
}