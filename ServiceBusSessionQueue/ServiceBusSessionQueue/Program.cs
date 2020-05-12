﻿using System;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace ServiceBusSessionQueue
{
    class Program
    {
        private static readonly string sbConnectionString = "Endpoint=sb://testsessionsbus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=nAcvTL2QX7oC1wzuwWeTjc2armGgazuez9QmJmNl0IY=";
        private static readonly string queueName = "fifotest";

        static void Main(string[] args)
        {
            Console.WriteLine("Azure ServiceBus Session Example...");

            // Send Messages to Service Bus Queue 
            SendMessagesWithSessionId(5);

            // Process messages from Service Bus Queue 
            ReadMessageWithSessionHandler();

        }

        static void SendMessagesWithSessionId(int numberOfMessages)
        {
            var queueClient = CreateSessionQueue(sbConnectionString, queueName);
            for (int i = 0; i < numberOfMessages; i++)
            {
                var message = new BrokeredMessage("message : " + i);
                if (i % 2 == 0)
                    message.SessionId = "Session-Even";
                else
                    message.SessionId = "Session-Odd";
                queueClient.Send(message);
                Console.WriteLine("sent : " + i);
            }
            queueClient.Close();
        }

        static QueueClient CreateSessionQueue(string sbConnectionString, string queueName)
        {
            NamespaceManager nsManager = NamespaceManager.CreateFromConnectionString(sbConnectionString);
            QueueDescription qDescription = new QueueDescription(queueName)
            {
                AutoDeleteOnIdle = TimeSpan.FromMinutes(5),
                DuplicateDetectionHistoryTimeWindow = TimeSpan.FromSeconds(60),
                RequiresDuplicateDetection = true,
                RequiresSession = true
            };
            if (nsManager.QueueExists(queueName))
            {
                nsManager.DeleteQueue(queueName);
            }
            nsManager.CreateQueue(qDescription);
            return QueueClient.CreateFromConnectionString(sbConnectionString, queueName);
        }

        static void ReadMessageWithSessionHandler()
        {
            var queueClient = QueueClient.CreateFromConnectionString(sbConnectionString, queueName);

            var sessionOptions = new SessionHandlerOptions()
            {
                AutoComplete = false,
                AutoRenewTimeout = TimeSpan.FromSeconds(30),
                MaxConcurrentSessions = 1,
                MessageWaitTimeout = TimeSpan.FromSeconds(10)
            };

            queueClient.RegisterSessionHandler(typeof(MessageSessionHandler), sessionOptions);
        }
    }
}