global using RabbitMQ.Client;
global using System.Text.Json;
using CToken = System.Threading.CancellationToken;

namespace RabbitHelper
{
    public static class RabbitHelperService
    {
        private record MetaData(string queue, byte[] body, BasicProperties properties, Guid requestId);

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

        private static MetaData GetMetaData<TModel>(object data, BasicProperties? properties) where TModel : class
        {
            var queue = nameof(TModel);
            var body = JsonSerializer.SerializeToUtf8Bytes(data);
            properties ??= new BasicProperties();
            return new(queue, body, properties, Guid.NewGuid());
        }

        private static async Task Publish(MetaData metaData)
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
                mandatory: true
            );
        }

        public static async Task DeclareQueues<TModel>(this TModel model) where TModel : class
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

        public static async Task<MetaData> PublishOnQueueAsync<TModel>(object data) where TModel : class
        {
            if (_channel is null || _connection is null)
                return null!;

            var metaData = GetMetaData<TModel>(data, null);
            await Publish(metaData);

            await Task.CompletedTask;
            return metaData;
        }

        public static async Task PublishOnQueueAsync<TModel>(object data, CToken ct, Func<TModel?, CToken, Task> handleResult) where TModel : class
        {
            var metaData = GetMetaData<TModel>(data, null);
            await PublishOnQueueAsync<TModel>(data, new BasicProperties() { CorrelationId = metaData.requestId.ToString() });

            _channel.BasicReturnAsync += async (sender, e) =>
            {
                if (e.RoutingKey.Equals(metaData.queue))
                {
                    var result = JsonSerializer.Deserialize<TModel>(e.Body.ToArray());
                    if (e.RoutingKey.Equals(metaData.requestId))
                        await handleResult(result, ct);
                }
            };

            await Task.CompletedTask;
        }

        
    }
}
