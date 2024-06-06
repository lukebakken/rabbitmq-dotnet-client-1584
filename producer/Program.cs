﻿using System.Diagnostics;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

const string exchangeName = "rmq-dotnet-client-1584-exchange";
const string queueName = "rmq-dotnet-client-1584";

const string hostName0 = "rmq0.local";
const string hostName1 = "rmq1.local";
const string hostName2 = "rmq2.local";

const ushort port = 5672;

var tcs = new TaskCompletionSource();
var cts = new CancellationTokenSource();
cts.Token.Register(tcs.SetCanceled);
bool sigintReceived = false;

void CancelHandler(object? sender, ConsoleCancelEventArgs e)
{
    Console.WriteLine("[INFO] CTRL-C pressed, exiting!");
    e.Cancel = true;
    sigintReceived = true;
    cts.Cancel();
}

Console.CancelKeyPress += new ConsoleCancelEventHandler(CancelHandler);

AppDomain.CurrentDomain.ProcessExit += (_, _) =>
{
    if (false == sigintReceived)
    {
        Console.WriteLine("[INFO] Received SIGTERM");
        cts.Cancel();
    }
    else
    {
        Console.WriteLine("[INFO] Received SIGTERM, ignoring it because already processed SIGINT");
    }
};

Console.WriteLine("[INFO] PRODUCER: waiting 10 seconds to try initial connection to RabbitMQ");
try
{
    await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
}
catch (TimeoutException)
{
}
catch (OperationCanceledException)
{
    Console.WriteLine("[INFO] PRODUCER EXITING");
    Environment.Exit(0);
}

var factory = new ConnectionFactory()
{
    ClientProvidedName = "PRODUCER",
    Port = port,
    AutomaticRecoveryEnabled = true,
    TopologyRecoveryEnabled = true
};

var endpoints = new List<AmqpTcpEndpoint>
{
    new AmqpTcpEndpoint(hostName0, port),
    new AmqpTcpEndpoint(hostName1, port),
    new AmqpTcpEndpoint(hostName2, port),
};

bool connected = false;
IConnection? connection = null;

async Task Connect()
{
    while (!connected)
    {
        try
        {
            connection = await factory.CreateConnectionAsync(endpoints);
            connected = true;
        }
        catch (BrokerUnreachableException)
        {
            connected = false;
            Console.WriteLine("[INFO] PRODUCER: waiting 5 seconds to re-try connection!");
            await Task.Delay(TimeSpan.FromSeconds(5));
        }
    }
}

async Task<bool> Publish()
{
    var latchSpan = TimeSpan.FromSeconds(1);
    bool maybeExit = false;

    try
    {
        byte[] buffer = new byte[1024];
        Random rnd = new Random();

        using (connection)
        {
            if (connection == null)
            {
                Console.Error.WriteLine("[ERROR] PRODUCER: unexpected null connection");
            }
            else
            {
                connection.RecoverySucceeded += (s, ea) =>
                {
                    Console.WriteLine("[INFO] PRODUCER: connection recovery succeeded!");
                    maybeExit = false;
                };

                connection.CallbackException += (s, ea) =>
                {
                    var cea = (CallbackExceptionEventArgs)ea;
                    Console.Error.WriteLine($"[ERROR] PRODUCER: connection.CallbackException: {cea}");
                };

                connection.ConnectionBlocked += (s, ea) =>
                {
                    var cbea = (ConnectionBlockedEventArgs)ea;
                    Console.Error.WriteLine($"[WARNING] PRODUCER: connection.ConnectionBlocked: {cbea}");
                };

                connection.ConnectionUnblocked += (s, ea) =>
                {
                    Console.WriteLine($"[INFO] PRODUCER: connection.ConnectionUnblocked: {ea}");
                };

                connection.ConnectionShutdown += (s, ea) =>
                {
                    var sdea = (ShutdownEventArgs)ea;
                    Console.Error.WriteLine($"[WARNING] PRODUCER: connection.ConnectionShutdown: {sdea}");
                    maybeExit = true;
                };


                using (var channel = await connection.CreateChannelAsync())
                {
                    channel.CallbackException += (s, ea) =>
                    {
                        var cea = (CallbackExceptionEventArgs)ea;
                        Console.Error.WriteLine($"[ERROR] PRODUCER: channel.CallbackException: {cea}");
                    };

                    channel.ChannelShutdown += (s, ea) =>
                    {
                        var sdea = (ShutdownEventArgs)ea;
                        Console.Error.WriteLine($"[WARNING] PRODUCER: channel.ChannelShutdown: {sdea}");
                        maybeExit = true;
                    };

                    await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Topic, durable: true);

                    var args = new Dictionary<string, object>
                    {
                        { "x-queue-type", "quorum" }
                    };
                    var queueDeclareResult = await channel.QueueDeclareAsync(queue: queueName, durable: true,
                            exclusive: false, autoDelete: false, arguments: args);
                    Debug.Assert(queueName == queueDeclareResult.QueueName);

                    await channel.QueueBindAsync(queue: queueName, exchange: exchangeName, routingKey: "update.*");

                    await channel.ConfirmSelectAsync();

                    int maybeExitTries = 0;
                    var props = new BasicProperties();
                    while (false == cts.IsCancellationRequested)
                    {
                        try
                        {
                            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
                        }
                        catch (TimeoutException)
                        {
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }

                        if (false == maybeExit && true == connection.IsOpen)
                        {
                            maybeExitTries = 0;
                            rnd.NextBytes(buffer);
                            string now = DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.ffffff");
                            string routingKey = string.Format("update.{0}", Guid.NewGuid().ToString());
                            try
                            {
                                await channel.BasicPublishAsync(exchange: exchangeName, routingKey: routingKey,
                                        basicProperties: props, body: buffer, mandatory: true,
                                        cancellationToken: cts.Token);
                                await channel.WaitForConfirmsOrDieAsync(cancellationToken: cts.Token);
                                Console.WriteLine($"[INFO] PRODUCER sent message at {now}");
                            }
                            catch (AlreadyClosedException ex)
                            {
                                Console.Error.WriteLine($"[WARNING] PRODUCER caught already closed exception: {ex}");
                            }
                        }
                        else
                        {
                            Console.WriteLine($"[INFO] PRODUCER waiting for connection to open: {maybeExitTries}");
                            if (maybeExit)
                            {
                                maybeExitTries++;
                            }

                            // Wait for 60 seconds for connection recovery
                            if (maybeExitTries > 60)
                            {
                                Console.Error.WriteLine($"[ERROR] PRODUCER could not auto-recover connection within 60 seconds, re-connecting!");
                                return true;
                            }
                        }
                    }
                }
            }
            Console.WriteLine("[INFO] PRODUCER: about to Dispose() connection...");
        }
        Console.WriteLine("[INFO] PRODUCER: Dispose() connection complete.");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[ERROR] PRODUCER exception: {ex}");
        return false;
    }

    return true;
}

bool shouldReconnect = true;
do
{
    await Connect();
    shouldReconnect = await Publish();
    connected = false;
    connection = null;
} while (shouldReconnect && false == cts.IsCancellationRequested);
