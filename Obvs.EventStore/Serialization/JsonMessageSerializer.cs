using System.IO;
using System.Text;
using Newtonsoft.Json;

namespace Obvs.EventStore.Serialization
{
    internal class JsonMessageSerializer
    {
        private readonly JsonSerializer _serializer;

        public JsonMessageSerializer()
        {
            _serializer = new JsonSerializer();
        }

        public void Serialize(Stream stream, object message)
        {
            using (var sw = new StreamWriter(stream, Encoding.UTF8, 1024, true))
            {
                using (var jtx = new JsonTextWriter(sw))
                {
                    _serializer.Serialize(jtx, message);
                }
            }
        }
    }
}