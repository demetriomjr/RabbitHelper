using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;
using CToken = System.Threading.CancellationToken;

namespace RabbitHelper
{
    public static class RabbitHelperService
    {
        public record MetaData(string queue, byte[] body, BasicProperties properties);

        private static IConnection? _connection = null;
        private static IChannel? _channel = null;

        public static async Task Init(string host = "localhost")
        {
            if (_connection is not null && _connection!.IsOpen)
                return;

            var factory = new ConnectionFactory() { HostName = host };
            _connection = await factory.CreateConnectionAsync();
            _channel = await _connection.CreateChannelAsync();
        }

        private static MetaData GetMetaData<T>(object data, BasicProperties? properties)
        {
            var queue = nameof(T);
            var body = JsonSerializer.SerializeToUtf8Bytes(data);
            properties ??= new BasicProperties()
            {
                CorrelationId = Guid.NewGuid().ToString(),
            };
            return new(queue, body, properties);
        }

        private static async Task Publish(MetaData metaData, CToken ct)
        {
            if (_channel is null || _connection is null)
                await Init();

            if (_channel is null || _connection is null)
                return;

            await _channel.BasicPublishAsync(
                exchange: "",
                body: metaData.body,
                routingKey: metaData.queue,
                basicProperties: metaData.properties,
                mandatory: true,
                cancellationToken: ct
            );
        }

        public static async Task DeclareQueues<T>(this T model) where T : class
        {
            if (_channel is null || _connection is null)
                return;

            var queue = nameof(model);
            await Task.Run(() =>
            {
                _channel.QueueDeclareAsync(
                    queue: queue,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                    );
            });
        }
        
        public static async Task<MetaData> PublishOnQueueAsync<T>(object data, CToken ct)
        {
            if (_channel is null || _connection is null)
                return null!;

            var metaData = GetMetaData<T>(data, null);
            await Publish(metaData, ct);

            return metaData;
        }

        public static async Task PublishOnQueueAsync<T>(object data, CToken ct, Func<T, CToken, Task> handleResult)
        {

            if (_channel is null || _connection is null)
                await Init();

            var metaData = await PublishOnQueueAsync<T>(data, ct);
            var correlationId = metaData.properties.CorrelationId;

            if (_channel is null || _connection is null)
                return;

            _channel.BasicReturnAsync += async (sender, e) =>
            {
                if (e.RoutingKey.Equals(metaData.queue))
                {
                    if (e.BasicProperties.CorrelationId!.Equals(correlationId))
                    {
                        var result = JsonSerializer.Deserialize<T>(e.Body.ToArray());

                        if (result is null) return;
                        await handleResult(result, ct);
                    }
                }
            };

            await Task.CompletedTask;
        }

        public static async Task ObserveQueueAsync<T>(CToken ct, Func<ReadOnlyMemory<byte>, CToken, Task> handleResult)
        {
            if (_channel is null || _connection is null)
                await Init();
            if (_channel is null || _connection is null)
                return;

            var consumer = new AsyncEventingBasicConsumer(_channel);
            string queue = typeof(T).Name;
            await _channel.BasicConsumeAsync(
                queue: queue, 
                autoAck: true,
                noLocal: false,
                consumerTag: "",
                exclusive: false,
                arguments: null,
                consumer: consumer
            );

            consumer.ReceivedAsync += async (sender, e) =>
            {
                await handleResult(e.Body, ct);
            };
        }
    }
}
