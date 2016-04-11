using System;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using Obvs.Serialization;

namespace Obvs.EventStore.Serialization
{
    internal class JsonProjectionDeserializer<TEvent> : IMessageDeserializer<TEvent> where TEvent : class
    {
        private readonly Type _type;
        private readonly JsonSerializer _serializer = new JsonSerializer();
        private readonly string _typeName;

        public JsonProjectionDeserializer(Type type)
        {
            _type = type;
            _typeName = type.Name;
        }

        public TEvent Deserialize(Stream source)
        {
            using (var streamReader = new StreamReader(source, Encoding.UTF8, false, 1024, true))
            {
                using (var jtr = new JsonTextReader(streamReader))
                {
                    return _serializer.Deserialize(jtr, _type) as TEvent;
                }
            }
        }

        public string GetTypeName()
        {
            return _typeName;
        }
    }
}