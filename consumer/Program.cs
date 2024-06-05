using System.Diagnostics;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

const string exchangeName = "rmq-dotnet-client-1584-exchange";
const string queueName = "rmq-dotnet-client-1584";

const string hostName = "haproxy.local";
ushort port = 5672;

AutoResetEvent latch = new AutoResetEvent(false);
bool running = true;

void CancelHandler(object? sender, ConsoleCancelEventArgs e)
{
    Console.WriteLine("[INFO] CTRL-C pressed, exiting!");
    e.Cancel = true;
    running = false;
    latch.Set();
}

Console.CancelKeyPress += new ConsoleCancelEventHandler(CancelHandler);

Console.WriteLine("[INFO] CONSUMER: waiting 10 seconds to try initial connection to RabbitMQ");
if (latch.WaitOne(TimeSpan.FromSeconds(10)))
{
    Console.WriteLine("[INFO] CONSUMER EXITING");
    Environment.Exit(0);
}

var factory = new ConnectionFactory()
{
    ClientProvidedName = "CONSUMER",
    HostName = hostName,
    Port = port,
    AutomaticRecoveryEnabled = true,
    TopologyRecoveryEnabled = true,
    DispatchConsumersAsync = true
};

bool connected = false;
IConnection? connection = null;

async Task Connect()
{
    while (!connected)
    {
        try
        {
            connection = await factory.CreateConnectionAsync();
            connected = true;
        }
        catch (BrokerUnreachableException)
        {
            connected = false;
            Console.WriteLine("[INFO] CONSUMER: waiting 5 seconds to re-try connection!");
            await Task.Delay(TimeSpan.FromSeconds(5));
        }
    }
}

int message_count = 0;

async Task Consume()
{
    var latchSpan = TimeSpan.FromSeconds(5);
    bool maybeExit = false;

    try
    {
        using (connection)
        {
            if (connection == null)
            {
                Console.Error.WriteLine("[ERROR] CONSUMER: unexpected null connection");
            }
            else
            {
                connection.RecoverySucceeded += (s, ea) =>
                {
                    Console.WriteLine("[INFO] CONSUMER: connection recovery succeeded!");
                    maybeExit = false;
                };

                connection.CallbackException += (s, ea) =>
                {
                    var cea = (CallbackExceptionEventArgs)ea;
                    Console.Error.WriteLine($"[WARNING] CONSUMER: connection.CallbackException: {cea}");
                };

                connection.ConnectionBlocked += (s, ea) =>
                {
                    var cbea = (ConnectionBlockedEventArgs)ea;
                    Console.Error.WriteLine($"[WARNING] CONSUMER: connection.ConnectionBlocked: {cbea}");
                };

                connection.ConnectionUnblocked += (s, ea) =>
                {
                    Console.WriteLine($"[INFO] CONSUMER: connection.ConnectionUnblocked: {ea}");
                };

                connection.ConnectionShutdown += (s, ea) =>
                {
                    var sdea = (ShutdownEventArgs)ea;
                    Console.Error.WriteLine($"[WARNING] CONSUMER: connection.ConnectionShutdown: {sdea}");
                    maybeExit = true;
                };

                using (var channel = await connection.CreateChannelAsync())
                {
                    channel.CallbackException += (s, ea) =>
                    {
                        var cea = (CallbackExceptionEventArgs)ea;
                        Console.Error.WriteLine($"[ERROR] CONSUMER: channel.CallbackException: {cea}");
                    };

                    channel.ChannelShutdown += (s, ea) =>
                    {
                        var sdea = (ShutdownEventArgs)ea;
                        Console.Error.WriteLine($"[WARNING] CONSUMER: channel.ChannelShutdown: {sdea}");
                        maybeExit = true;
                    };

                    await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Topic, durable: true);

                    var queueArgs = new Dictionary<string, object>
                    {
                        { "x-queue-type", "quorum" }
                    };
                    var queueDeclareResult = await channel.QueueDeclareAsync(queue: queueName, durable: true,
                            exclusive: false, autoDelete: false, arguments: queueArgs);
                    Debug.Assert(queueName == queueDeclareResult.QueueName);

                    await channel.QueueBindAsync(queue: queueName, exchange: exchangeName, routingKey: "update.*");

                    await channel.BasicQosAsync(0, 1, false);

                    Console.WriteLine("[INFO] CONSUMER: waiting for messages...");

                    var consumer = new AsyncEventingBasicConsumer(channel);
                    consumer.Received += async (model, ea) =>
                    {
                        DateTime received = DateTime.Now;
                        string receivedText = received.ToString("MM/dd/yyyy HH:mm:ss.ffffff");
                        Console.WriteLine($"[INFO] CONSUMER received at {receivedText}, size {ea.Body.Length}, message_count: {message_count++}");
                        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
                    };

                    await channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer);

                    int maybeExitTries = 0;
                    while (false == latch.WaitOne(latchSpan))
                    {
                        if (false == maybeExit && true == connection.IsOpen)
                        {
                            maybeExitTries = 0;
                        }
                        else
                        {
                            Console.WriteLine($"[INFO] CONSUMER waiting for connection to open: {maybeExitTries}");
                            if (maybeExit)
                            {
                                maybeExitTries++;
                            }

                            // Wait for 60 seconds for connection recovery (5 secs * 12 tries)
                            if (maybeExitTries > 12)
                            {
                                Console.Error.WriteLine($"[ERROR] CONSUMER could not auto-recover connection within 60 seconds, re-connecting!");
                                break;
                            }
                        }
                    }

                    Console.WriteLine("CONSUMER EXITING");
                }
            }
            Console.WriteLine("[INFO] CONSUMER: about to Dispose() connection...");
        }
        Console.WriteLine("[INFO] PRODUCER: Dispose() connection complete.");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"CONSUMER exception: {ex}");
    }
}

do
{
    await Connect();
    await Consume();
    connected = false;
    connection = null;
} while (running);
