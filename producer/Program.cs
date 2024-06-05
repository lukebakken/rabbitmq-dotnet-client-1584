using System.Diagnostics;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

const string exchangeName = "status_exchange";
const string queueName = "status_updates";
const string hostName = "rmq1.local";
const ushort port = 5672;

AutoResetEvent latch = new AutoResetEvent(false);

void CancelHandler(object? sender, ConsoleCancelEventArgs e)
{
    Console.WriteLine("CTRL-C pressed, exiting!");
    e.Cancel = true;
    latch.Set();
}

Console.CancelKeyPress += new ConsoleCancelEventHandler(CancelHandler);

Console.WriteLine($"PRODUCER: waiting 10 seconds to try initial connection to {hostName}:{port}");
Thread.Sleep(TimeSpan.FromSeconds(10));

var factory = new ConnectionFactory()
{
    HostName = hostName,
    Port = port
};

TimeSpan latchWaitSpan = TimeSpan.FromSeconds(1);
bool connected = false;

IConnection? connection = null;

while (!connected)
{
    try
    {
        connection = factory.CreateConnection();
        connected = true;
    }
    catch (BrokerUnreachableException)
    {
        connected = false;
        Console.WriteLine($"PRODUCER: waiting 5 seconds to re-try connection!");
        Thread.Sleep(TimeSpan.FromSeconds(5));
    }
}

byte[] buffer = new byte[1024];
Random rnd = new Random();

using (connection)
{
    if (connection == null)
    {
        Console.Error.WriteLine("PRODUCER: unexpected null connection");
    }
    else
    {
        connection.CallbackException += (s, ea) =>
        {
            var cea = (CallbackExceptionEventArgs)ea;
            Console.Error.WriteLine($"PRODUCER: connection.CallbackException: {cea}");
        };

        connection.ConnectionBlocked += (s, ea) =>
        {
            var cbea = (ConnectionBlockedEventArgs)ea;
            Console.Error.WriteLine($"PRODUCER: connection.ConnectionBlocked: {cbea}");
        };

        connection.ConnectionUnblocked += (s, ea) =>
        {
            Console.Error.WriteLine($"PRODUCER: connection.ConnectionUnblocked: {ea}");
        };

        connection.ConnectionShutdown += (s, ea) =>
        {
            var sdea = (ShutdownEventArgs)ea;
            Console.Error.WriteLine($"PRODUCER: connection.ConnectionShutdown: {sdea}");
        };

        using (var channel = connection.CreateModel())
        {
            channel.CallbackException += (s, ea) =>
            {
                var cea = (CallbackExceptionEventArgs)ea;
                Console.Error.WriteLine($"PRODUCER: channel.CallbackException: {cea}");
            };

            channel.ModelShutdown += (s, ea) =>
            {
                var sdea = (ShutdownEventArgs)ea;
                Console.Error.WriteLine($"PRODUCER: channel.ModelShutdown: {sdea}");
            };

            channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Topic, durable: true);

            var queueDeclareResult = channel.QueueDeclare(queue: queueName, durable: true,
                    exclusive: false, autoDelete: false, arguments: null);
            Debug.Assert(queueName == queueDeclareResult.QueueName);

            channel.QueueBind(queue: queueName, exchange: exchangeName, routingKey: "update.*");

            channel.ConfirmSelect();

            var latchSpan = TimeSpan.FromSeconds(1);
            while (false == latch.WaitOne(latchSpan))
            {
                rnd.NextBytes(buffer);
                string now = DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.ffffff");
                string routingKey = string.Format("update.{0}", Guid.NewGuid().ToString());
                channel.BasicPublish(exchange: exchangeName, routingKey: routingKey, basicProperties: null, body: buffer);
                channel.WaitForConfirmsOrDie();
                Console.WriteLine($"PRODUCER sent message at {now}");
            }
        }
    }
}
