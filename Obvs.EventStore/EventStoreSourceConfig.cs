namespace Obvs.EventStore
{
    public class EventStoreSourceConfig
    {
        public int MinBytesPerFetch { get; set; } = 1;
        public int MaxBytesPerFetch { get; set; } = 262144;
    }
}