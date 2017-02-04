using System;
using EventStore.ClientAPI;
using Obvs.Configuration;
using Obvs.Types;
using IMessage = Obvs.Types.IMessage;

namespace Obvs.EventStore.Configuration
{
    public static class EventStoreConfigExtensions
    {
        public static ICanSpecifyEventStoreServiceName<TMessage, TCommand, TEvent, TRequest, TResponse> WithEventStoreEndpoints<TServiceMessage, TMessage, TCommand, TEvent, TRequest, TResponse>(this ICanAddEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse> canAddEndpoint) 
            where TMessage : class
            where TServiceMessage : class 
            where TCommand : class, TMessage 
            where TEvent : class, TMessage 
            where TRequest : class, TMessage 
            where TResponse : class, TMessage
        {
            return new EventStoreFluentConfig<TServiceMessage, TMessage, TCommand, TEvent, TRequest, TResponse>(canAddEndpoint);
        }

        public static ICanSpecifyEventStoreServiceName<IMessage, ICommand, IEvent, IRequest, IResponse> WithEventStoreEndpoints<TServiceMessage>(this ICanAddEndpoint<IMessage, ICommand, IEvent, IRequest, IResponse> canAddEndpoint) where TServiceMessage : class 
        {
            return new EventStoreFluentConfig<TServiceMessage, IMessage, ICommand, IEvent, IRequest, IResponse>(canAddEndpoint);
        }

        public static ICanAddEndpointOrLoggingOrCorrelationOrCreate<TMessage, TCommand, TEvent, TRequest, TResponse> WithEventStoreSharedConnectionScope<TMessage, TCommand, TEvent, TRequest, TResponse>(this ICanAddEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse> canAddEndpoint, string connectionString,
            Func<ICanAddEndpoint<TMessage, TCommand, TEvent, TRequest, TResponse>, ICanAddEndpointOrLoggingOrCorrelationOrCreate<TMessage, TCommand, TEvent, TRequest, TResponse>> endPointFactory)
            where TMessage : class
            where TCommand : class, TMessage
            where TEvent : class, TMessage
            where TRequest : class, TMessage
            where TResponse : class, TMessage
        {
            EventStoreFluentConfigContext.SharedConnection = EventStoreConnection.Create(connectionString).AsLazy();

            var result = endPointFactory(canAddEndpoint);

            EventStoreFluentConfigContext.SharedConnection = null;
            return result;
        }

        public static ICanAddEndpointOrLoggingOrCorrelationOrCreate<IMessage, ICommand, IEvent, IRequest, IResponse> WithEventStoreSharedConnectionScope<TServiceMessage>(this ICanAddEndpoint<IMessage, ICommand, IEvent, IRequest, IResponse> canAddEndpoint, string connectionString,
            Func<ICanAddEndpoint<IMessage, ICommand, IEvent, IRequest, IResponse>, ICanAddEndpointOrLoggingOrCorrelationOrCreate<IMessage, ICommand, IEvent, IRequest, IResponse>> endPointFactory) 
            where TServiceMessage : class
        {
            return canAddEndpoint.WithEventStoreSharedConnectionScope(connectionString, endPointFactory);
        }
    }
}