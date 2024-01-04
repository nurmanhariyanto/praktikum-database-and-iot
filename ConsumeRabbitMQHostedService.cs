/*************************************************************************************************************************
 *                               DEVELOPMENT BY      : NURMAN HARIYANTO - PT.LSKK & PPTIK                                *
 *                                                VERSION             : 2                                                *
 *                                             TYPE APPLICATION    : WORKER                                              *
 * DESCRIPTION         : GET DATA FROM MQTT (OUTPUT DEVICE) CHECK TO DB RULES AND SEND BACK (INPUT DEVICE) IF DATA EXIST *
 *************************************************************************************************************************/

namespace praktikum_database_and_iot
{
   using System.Configuration;
   using System.Threading;
   using System.Threading.Tasks;
   using System.Text;
   using Microsoft.Extensions.Hosting;
   using Microsoft.Extensions.Logging;
   using RabbitMQ.Client;
   using RabbitMQ.Client.Events;
   using Microsoft.Data.Sqlite;
   using System;

   public class ConsumeRabbitMQHostedService : BackgroundService
   {
      private readonly ILogger _logger;
      private IConnection _connection;
      private IModel _channel;

      private static string RMQHost = ConfigurationManager.AppSettings["RMQHost"];
      private static string RMQVHost = ConfigurationManager.AppSettings["RMQVHost"];
      private static string RMQUsername = ConfigurationManager.AppSettings["RMQUsername"];
      private static string RMQPassword = ConfigurationManager.AppSettings["RMQPassword"];
      private static string RMQQueue = ConfigurationManager.AppSettings["RMQQueue"];
      private static string RMQExc = ConfigurationManager.AppSettings["RMQExc"];
      private static string RMQPubRoutingKey = ConfigurationManager.AppSettings["RMQPubRoutingKey"];
      private static string DBPath = ConfigurationManager.AppSettings["DBPath"];
      private static string InputTypeReader = "";
      private static string ValueInputReader = "";
      private static string ClassIdRegistration = "";
      private static string ClassIdSchedule = "";

      /*
Type Registration = R000
Type Attendance = A000
*/
      private static string ValueInputReaderRegistration = "R000";
      private static string ValueInputReaderAttendance = "A000";
      public ConsumeRabbitMQHostedService(ILoggerFactory loggerFactory)
      {

         this._logger = loggerFactory.CreateLogger<ConsumeRabbitMQHostedService>();
         InitRabbitMQ();
      }

      private void InitRabbitMQ()
      {

         var factory = new ConnectionFactory
         {
            HostName = RMQHost,
            VirtualHost = RMQVHost,
            UserName = RMQUsername,
            Password = RMQPassword
         };

         // create connection
         _connection = factory.CreateConnection();

         // create channel
         _channel = _connection.CreateModel();

         _connection.ConnectionShutdown += RabbitMQ_ConnectionShutdown;
      }
      protected override Task ExecuteAsync(CancellationToken stoppingToken)
      {

         stoppingToken.ThrowIfCancellationRequested();

         var consumer = new EventingBasicConsumer(_channel);
         consumer.Received += (ch, ea) =>
         {
            // received message
            var body = ea.Body.ToArray();
            var content = System.Text.Encoding.UTF8.GetString(body);
            // handle the received message
            HandleMessage(content);
            _channel.BasicAck(ea.DeliveryTag, true);
         };

         consumer.Shutdown += OnConsumerShutdown;

         _channel.BasicConsume(RMQQueue, false, consumer);
         return Task.CompletedTask;
      }

      private void HandleMessage(string content)
      {
         //just print this message 

         // _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
         _logger.LogInformation($"consumer received {content}");
          //_logger.LogInformation($"succed");
         //And splite message to Query Parameters
         DateTime now = DateTime.Now;
         String TimeStamp = now.ToString();
         string[] dataParsing = content.Split('#');
//tambah deskripsi
         foreach (var datas in dataParsing)
         {
            //System.Console.WriteLine($"{datas}>");
            InputTypeReader = dataParsing[0];
            ValueInputReader = dataParsing[1];
         }
        _logger.LogInformation($"Data ke 1 {InputTypeReader}");
        _logger.LogInformation($"Data ke 2 {ValueInputReader}");

         /*
         if data is attendance code
         */
/*
         if (InputTypeReader == ValueInputReaderAttendance)
         {
            var connectionStringBuilder = new SqliteConnectionStringBuilder();
            connectionStringBuilder.DataSource = DBPath;
            using (var connection = new SqliteConnection(connectionStringBuilder.ConnectionString))
            {

               connection.Open();
               var selectCmd = connection.CreateCommand();
               selectCmd.CommandText = "SELECT * FROM Registration WHERE card_code=@CardCode";
               selectCmd.Parameters.AddWithValue("@CardCode", ValueInputReader);
               using (var reader = selectCmd.ExecuteReader())
               {
                  while (reader.Read())
                  {
                     ClassIdRegistration = reader.GetString(4);
                    // _logger.LogInformation($"value class {ClassIdRegistration}");

                  }
               }
               var selectCmd2 = connection.CreateCommand();
               selectCmd2.CommandText = "SELECT * FROM Schedule WHERE class_code=@ClassCode";
               selectCmd2.Parameters.AddWithValue("@ClassCode", ClassIdRegistration);
               using (var reader2 = selectCmd2.ExecuteReader())
               {
                  while (reader2.Read())
                  {
                     ClassIdSchedule = reader2.GetString(1);
                     _logger.LogInformation($"value class2 {ClassIdSchedule}");
                     if (ClassIdSchedule == ClassIdRegistration)
                     {
                        using (var transaction = connection.BeginTransaction())
                        {
                           var insertCmd = connection.CreateCommand();
                           insertCmd.CommandText = "INSERT INTO Attendance (card_code,card_time_attendance,card_class_id)VALUES(@CardCode,@TimeAttendance,@ClassCode)";
                           insertCmd.Parameters.AddWithValue("@CardCode", ValueInputReader);
                           insertCmd.Parameters.AddWithValue("@TimeAttendance", TimeStamp);
                           insertCmd.Parameters.AddWithValue("@ClassCode", ClassIdSchedule);
                           insertCmd.ExecuteNonQuery();
                           _logger.LogInformation($"success");
                           transaction.Commit();


                        }
                     }

                  }
               }
               connection.Close();

            }
         }


         /*
         if data is register code
         

         if (InputTypeReader == ValueInputReaderRegistration)
         {
            var connectionStringBuilder = new SqliteConnectionStringBuilder();
            connectionStringBuilder.DataSource = DBPath;
            using (var connection = new SqliteConnection(connectionStringBuilder.ConnectionString))
            {

               connection.Open();
               using (var transaction = connection.BeginTransaction())
               {
                  var insertCmd = connection.CreateCommand();
                  insertCmd.CommandText = "INSERT INTO Registration (card_code,card_date_registration)VALUES(@CardCode,@TimeRegistration)";
                  insertCmd.Parameters.AddWithValue("@CardCode", ValueInputReader);
                  insertCmd.Parameters.AddWithValue("@TimeRegistration", TimeStamp);
                  insertCmd.ExecuteNonQuery();
                  //_logger.LogInformation($"success insert");
                  transaction.Commit();
               }
               connection.Close();
            }
         }
         */
         
      }

      private void RabbitMQ_ConnectionShutdown(object sender, ShutdownEventArgs e)
      {
         _logger.LogInformation($"connection shut down {e.ReplyText}");
      }

      private void OnConsumerShutdown(object sender, ShutdownEventArgs e)
      {
         _logger.LogInformation($"consumer shutdown {e.ReplyText}");
      }

      public override void Dispose()
      {
         _channel.Close();
         _connection.Close();
         base.Dispose();
      }
   }
}
