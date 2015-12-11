namespace Obvs.Kafka
{
    public class EventStoreProducerConfig
    {
        public int BatchFlushSize { get; set; } = 1000;
    }
}