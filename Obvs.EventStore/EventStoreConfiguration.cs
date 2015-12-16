namespace Obvs.EventStore
{
    public class EventStoreConfiguration
    {
        public EventStoreConfiguration(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public string ConnectionString { get; }
    }
}