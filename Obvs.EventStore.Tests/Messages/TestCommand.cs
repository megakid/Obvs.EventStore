using System.Diagnostics;
using Obvs.Types;

namespace Obvs.EventStore.Tests.Messages
{
    public class TestCommand : ITestMessage1, ICommand
    {
        public int Id { get; set; }

        public long Ticks { get; set; } = Stopwatch.GetTimestamp();

        public override string ToString()
        {
            return string.Format("{0}[Id={1}]", GetType().Name, Id);
        }
    }
}