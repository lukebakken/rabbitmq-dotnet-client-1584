using System.Diagnostics;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

const string exchangeName = "status_exchange";
const string queueName = "status_updates";

AutoResetEvent latch = new AutoResetEvent(false);

void CancelHandler(object? sender, ConsoleCancelEventArgs e)
{
    Console.WriteLine("CTRL-C pressed, exiting!");
    e.Cancel = true;
    latch.Set();
}

Console.CancelKeyPress += new ConsoleCancelEventHandler(CancelHandler);

string hostName = "rmq0.local";
ushort port = 5672;

Console.WriteLine($"CONSUMER: waiting 10 seconds to try initial connection to {hostName}:{port}");
if (latch.WaitOne(TimeSpan.FromSeconds(10)))
{
    Console.WriteLine("CONSUMER EXITING");
    Environment.Exit(0);
}

var factory = new ConnectionFactory()
{
    HostName = hostName,
    Port = port,
    AutomaticRecoveryEnabled = false,
    TopologyRecoveryEnabled = false
};

bool connected = false;

IConnection? connection = null;

while(!connected)
{
    try
    {
        connection = factory.CreateConnection();
        connected = true;
    }
    catch (BrokerUnreachableException)
    {
        connected = false;
        Console.WriteLine("CONSUMER: waiting 5 seconds to re-try connection!");
        Thread.Sleep(TimeSpan.FromSeconds(5));
    }
}

int message_count = 0;

using (connection)
{
    if (connection == null)
    {
        Console.Error.WriteLine("CONSUMER: unexpected null connection");
    }
    else
    {
        connection.CallbackException += (s, ea) =>
        {
            var cea = (CallbackExceptionEventArgs)ea;
            Console.Error.WriteLine($"CONSUMER: connection.CallbackException: {cea}");
        };

        connection.ConnectionBlocked += (s, ea) =>
        {
            var cbea = (ConnectionBlockedEventArgs)ea;
            Console.Error.WriteLine($"CONSUMER: connection.ConnectionBlocked: {cbea}");
        };

        connection.ConnectionUnblocked += (s, ea) =>
        {
            Console.Error.WriteLine($"CONSUMER: connection.ConnectionUnblocked: {ea}");
        };

        connection.ConnectionShutdown += (s, ea) =>
        {
            var sdea = (ShutdownEventArgs)ea;
            Console.Error.WriteLine($"CONSUMER: connection.ConnectionShutdown: {sdea}");
        };

        using (var channel = connection.CreateModel())
        {
            channel.CallbackException += (s, ea) =>
            {
                var cea = (CallbackExceptionEventArgs)ea;
                Console.Error.WriteLine($"CONSUMER: channel.CallbackException: {cea}");
            };

            channel.ModelShutdown += (s, ea) =>
            {
                var sdea = (ShutdownEventArgs)ea;
                Console.Error.WriteLine($"CONSUMER: channel.ModelShutdown: {sdea}");
            };

            channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Topic, durable: true);

            var queueDeclareResult = channel.QueueDeclare(queue: queueName, durable: true,
                    exclusive: false, autoDelete: false, arguments: null);
            Debug.Assert(queueName == queueDeclareResult.QueueName);

            channel.QueueBind(queue: queueName, exchange: exchangeName, routingKey: "update.*");

            channel.BasicQos(0, 1, false);

            Console.WriteLine("CONSUMER: waiting for messages...");

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                DateTime received = DateTime.Now;
                string receivedText = received.ToString("MM/dd/yyyy HH:mm:ss.ffffff");
                Console.WriteLine($"CONSUMER received at {receivedText}, size {ea.Body.Length}, message_count: {message_count++}");
                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            };

            channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

            latch.WaitOne();

            Console.WriteLine("CONSUMER EXITING");
        }
    }
}
