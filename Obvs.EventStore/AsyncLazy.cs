using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Obvs.EventStore
{
    public class AsyncLazy<T> : Lazy<Task<T>>
    {
        public AsyncLazy(Func<T> valueFactory) :
            base(() => Task.Run(valueFactory), LazyThreadSafetyMode.ExecutionAndPublication)
        { }

        public AsyncLazy(Func<Task<T>> taskFactory) :
            base(() => taskFactory(), LazyThreadSafetyMode.ExecutionAndPublication)
        { }

        public TaskAwaiter<T> GetAwaiter() { return Value.GetAwaiter(); }
    }
}