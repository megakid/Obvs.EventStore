using Obvs.Types;

namespace Obvs.EventStore.Tests.Messages
{
    public class Test2Command : ITestMessage1, ICommand
    {
        public int Id { get; set; }

        public override string ToString()
        {
            return string.Format("{0}[Id={1}]", GetType().Name, Id);
        }
    }
}