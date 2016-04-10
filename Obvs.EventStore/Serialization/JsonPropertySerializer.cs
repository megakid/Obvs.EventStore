using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;

namespace Obvs.EventStore.Serialization
{
    internal class JsonPropertySerializer
    {
        private readonly JsonSerializer _serializer;

        public JsonPropertySerializer()
        {
            _serializer = new JsonSerializer();
        }

        public void Serialize(Stream stream, Dictionary<string, string> properties)
        {
            using (var sw = new StreamWriter(stream, Encoding.UTF8, 1024, true))
            {
                using (var jtx = new JsonTextWriter(sw))
                {
                    _serializer.Serialize(jtx, properties);
                }
            }
        }

        public Dictionary<string, string> Deserialize(Stream stream)
        {
            using (var streamReader = new StreamReader(stream, Encoding.UTF8, false, 1024, true))
            {
                using (var jtr = new JsonTextReader(streamReader))
                {
                    return _serializer.Deserialize<Dictionary<string, string>>(jtr);
                }
            }
        }
    }
}