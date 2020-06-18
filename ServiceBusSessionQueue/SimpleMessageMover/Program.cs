using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
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

        static void Main(string[] args)
        {
            try
            {   
                // TODO - Update connection string before you run this console app
                var sourceQueueConnectionString = "Endpoint=sb://entitypath.servicebus.windows.net/;SharedAccessKeyName=Listen;SharedAccessKey=XXXXXXX";
                var sourceQueueEntityPath = "entitypathSource";
                var destQueueConnectionString = "Endpoint=sb://entitypath.servicebus.windows.net/;SharedAccessKeyName=Send;SharedAccessKey=XXXXXXX";
                var destQueueEntityPath = "entitypathDestination";
               
                // Create Queue client for source queue (From which we need to read the messages)
                QueueClient readerClient = new QueueClient(sourceQueueConnectionString, sourceQueueEntityPath, ReceiveMode.PeekLock);

                // Create message sender client for destination queue (To which we need to write/move message)
                MessageSender senderClient = new MessageSender(destQueueConnectionString, destQueueEntityPath);

                Console.WriteLine("Queue Client Creation Done.");

                // Start Listener for source queue and write messages into destination queue.
                ReadMessageFromSourceSessionQueue(readerClient, senderClient);
                Console.ReadKey();

                // Close reader
                readerClient.CloseAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception - {0}.", ex);
            }
        }

        static void ReadMessageFromSourceSessionQueue(QueueClient readerClient, MessageSender senderClient)
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

                           ProcessMessageAsync(message, senderClient);
                       }

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

        static void ProcessMessageAsync(Message message, MessageSender senderClient)
        {
            senderClient.SendAsync(message);
            lock (Console.Out)
            {
                WriteCount = WriteCount + 1;
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Message sent: Session {0}, MessageId = {1}, WriteCount = {2}",
                    message.SessionId, message.MessageId, WriteCount);
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
