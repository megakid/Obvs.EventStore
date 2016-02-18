using Obvs.Types;

namespace Obvs.EventStore.Tests.Messages
{
    public class TestEvent : ITestMessage1, IEvent
    {
        public int Id { get; set; }

        public override string ToString()
        {
            return string.Format("{0}[Id={1}]", GetType().Name, Id);
        }
    }
}