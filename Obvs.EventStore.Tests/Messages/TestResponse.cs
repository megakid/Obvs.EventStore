using Obvs.Types;

namespace Obvs.EventStore.Tests.Messages
{
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