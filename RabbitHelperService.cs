global using RabbitMQ.Client;
global using System.Text.Json;

namespace RabbitHelper
{
    public static class RabbitHelperService
    {
        private static IConnection? _connection = null;
        private static IChannel? _channel = null;

        public static async Task Initialize(string host = "localhost")
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
        
        public static async Task PublishOnQueue<TModel>(this TModel model, object data, BasicProperties? properties = null) where TModel : class
        {
            if (_channel is null || _connection is null)
                return;

            var queue = nameof(model);
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

        public static async Task PublishOnQueue<TModel>(this TModel model, object data, Func<Response<TModel>, Task> handleResult) where TModel : class
        {
            if (_channel is null || _connection is null)
                return;
            
            var queue = nameof(model);
            var requestId = Guid.NewGuid().ToString();
            await PublishOnQueue<TModel>(model, data, new BasicProperties() { CorrelationId =  requestId});

            _channel.BasicReturnAsync += async (sender, e) =>
            {
                if(e.RoutingKey.Equals(queue))
                {
                    var result = JsonSerializer.Deserialize<Response<TModel>>(e.Body.ToArray());
                    if (e.RoutingKey.Equals(requestId))
                        await handleResult(result);
                }
            };

            await Task.CompletedTask;
        }

        public static async Task PublishOnQueue<TModel>(this TModel model, object data, Func<TModel, Task> handleResult) where TModel : class
        {
            if (_channel is null || _connection is null)
                return;

            var queue = nameof(model);
            var requestId = Guid.NewGuid().ToString();
            await PublishOnQueue<TModel>(model, data, new BasicProperties() { CorrelationId = requestId });

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
