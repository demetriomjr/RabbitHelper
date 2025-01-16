global using RabbitMQ.Client;
global using System.Text.Json;
using System.Dynamic;
using System.Runtime.CompilerServices;

namespace RabbitHelper
{
    public static class RabbitHelperService
    {
        private static IConnection? _connection = null;
        private static IChannel? _channel = null;

        private (string queue, byte[] body) GetMetaData<T>(object data)
        {
            var queue = nameof(T);
            var body = JsonSerializer.SerializeToUtf8Bytes(data);
            return (queue, body);
        }

        public static async Task Init(string host = "localhost")
        {
            if (_connection is not null && _connection!.IsOpen)
                return;

            var factory = new ConnectionFactory() { HostName = host };
            _connection = await factory.CreateConnectionAsync();
            _channel = await _connection.CreateChannelAsync();
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
        
        public static async Task PublishOnQueueAsync<TModel>(this TModel model, object data, BasicProperties? properties = null) where TModel : class
        {
            if (_channel is null || _connection is null)
                return;

            var metaData = GetMetaData<TModel>(data);

            await _channel.BasicPublishAsync(
                exchange: "",
                body: body,
                routingKey: queue,
                basicProperties: properties ?? new BasicProperties(),
                mandatory: true
                );

            await Task.CompletedTask;
        }
        public static async Task PublishOnQueueAsync<TModel>(object data, BasicProperties? properties = null) where TModel : class
        {
            if (_channel is null || _connection is null)
                return;

            var queue = nameof(TModel);
            var body = JsonSerializer.SerializeToUtf8Bytes(data);

            await _channel.BasicPublishAsync(
                exchange: "",
                body: body,
                routingKey: queue,
                basicProperties: properties ?? new BasicProperties(),
                mandatory: true
                );

            await Task.CompletedTask;
        }

        public static async Task PublishOnQueueAsync<TModel>(object data, 
            CancellationToken ct, 
            Func<TModel?, CancellationToken, Task> handleResult) where TModel : class
        {
            if (_channel is null || _connection is null)
                return;

            var queue = nameof(TModel);
            var requestId = Guid.NewGuid().ToString();
            await PublishOnQueueAsync<TModel>(data, new BasicProperties() { CorrelationId = requestId });

            _channel.BasicReturnAsync += async (sender, e) =>
            {
                if (e.RoutingKey.Equals(queue))
                {
                    var result = JsonSerializer.Deserialize<TModel>(e.Body.ToArray());
                    if (e.RoutingKey.Equals(requestId))
                        await handleResult(result, ct);
                }
            };

            await Task.CompletedTask;
        }

        public static async Task PublishOnQueueAsync<TModel>(this TModel model, object data, Func<TModel, Task> handleResult) where TModel : class
        {
            if (_channel is null || _connection is null)
                return;

            var queue = nameof(TModel);
            var requestId = Guid.NewGuid().ToString();
            await PublishOnQueueAsync(model, data, new BasicProperties() { CorrelationId = requestId });

            _channel.BasicReturnAsync += async (sender, e) =>
            {
                if (e.RoutingKey.Equals(queue))
                {
                    var result = JsonSerializer.Deserialize<TModel>(e.Body.ToArray());
                    if (e.RoutingKey.Equals(requestId))
                        await handleResult(result!);
                }
            };

            await Task.CompletedTask;
        }
    }
}
