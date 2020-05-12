using Microsoft.ServiceBus.Messaging;
using System;
using System.Threading;

namespace SessionQueueProcessor
{
    public class MessageSessionHandler : IMessageSessionHandler
    {
        private Guid messageHandlerId = Guid.NewGuid();

        public void OnCloseSession(MessageSession session)
        {
            Console.WriteLine($"Session closed - Handler Id {messageHandlerId}");
        }

        public void OnMessage(MessageSession session, BrokeredMessage message)
        {
            var msg = message.GetBody<string>();
            Console.WriteLine($"Received : {msg} from session : " +
                    $"{message.SessionId} handled by : {messageHandlerId} ");
            Thread.Sleep(1000);
            message.Complete();
        }

        public void OnSessionLost(Exception exception)
        {
            Console.WriteLine($"Session lost - Handler Id {messageHandlerId} due to " +
                $"exception {exception.Message}");
        }
    }
}
