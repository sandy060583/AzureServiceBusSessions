using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

namespace SimpleMessageMover
{
    static class Program
    {
        /* ***********************************************************************/
        /*          Azure ServiceBus Session Queue Message Mover Utility         */
        /* ***********************************************************************/
        /* Move messages from source session queue to destination session queue  */
        /* ***********************************************************************/
        private static int ReadCount = 0;
        private static int WriteCount = 0;
        private static IMessageSender senderClient;
        private static QueueClient readerClient;

        static void Main(string[] args)
        {
            try
            {
                // TODO - Update connection string before you run this console app 
                var sourceQueueConnectionString = "Endpoint=sb://sessionqueuetest.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=XXXXX";
                var sourceQueueEntityPath = "SourceQ";
                var destQueueConnectionString = "Endpoint=sb://sessionqueuetest.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=XXXXX";
                var destQueueEntityPath = "DestQ";

                // Create Queue client for source queue (From which we need to read the messages)
                readerClient = new QueueClient(sourceQueueConnectionString, sourceQueueEntityPath, ReceiveMode.PeekLock);

                // Create message sender client for destination queue (To which we need to write/move message)
                senderClient = new MessageSender(destQueueConnectionString, destQueueEntityPath);

                Console.WriteLine("Queue Client Creation Done.");

                // Start Listener for source queue and write messages into destination queue.
                ReadMessageFromSourceSessionQueue();
                Console.ReadKey();

            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception - {0}.", ex);
            }
        }

        static void ReadMessageFromSourceSessionQueue()
        {
            // Register Session Handler and send the message
            readerClient.RegisterSessionHandler(
                   async (session, message, cancellationToken) =>
                   {
                       ReadCount = ReadCount + 1;
                       lock (Console.Out)
                       {
                           Console.ForegroundColor = ConsoleColor.Cyan;
                           Console.WriteLine("Message read: Session {0}, MessageId = {1}, ReadCount = {2}",
                               message.SessionId, message.MessageId, ReadCount);
                           Console.ResetColor();
                       }

                       await ProcessMessageAsync(message);

                       // Delete message from source queue (reader queue)
                       await session.CompleteAsync(message.SystemProperties.LockToken);
                       // Close session from source queue
                       await session.CloseAsync();
                   },
                    new SessionHandlerOptions(e => LogMessageHandlerException(e))
                    {
                        MessageWaitTimeout = TimeSpan.FromSeconds(5),
                        MaxConcurrentSessions = 1,
                        AutoComplete = false
                    });
        }

        static async Task ProcessMessageAsync(Message message)
        {
            // Create new message from existing one
            Message messageCopy = new Message();
            messageCopy.Body = message.Body;
            messageCopy.SessionId = message.SessionId;
            messageCopy.MessageId = message.MessageId;

            await senderClient.SendAsync(messageCopy);

            lock (Console.Out)
            {
                WriteCount = WriteCount + 1;
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Message sent: Session {0}, MessageId = {1}, WriteCount = {2}",
                    messageCopy.SessionId, messageCopy.MessageId, WriteCount);
                Console.ResetColor();
            }
        }

        static Task LogMessageHandlerException(ExceptionReceivedEventArgs e)
        {
            Console.WriteLine("Exception: \"{0}\" {1}", e.Exception.Message, e.ExceptionReceivedContext.EntityPath);
            return Task.CompletedTask;
        }
    }
}
