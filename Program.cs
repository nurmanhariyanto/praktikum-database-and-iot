using praktikum_database_and_iot;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<ConsumeRabbitMQHostedService>();

var host = builder.Build();
host.Run();
