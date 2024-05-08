using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Moonman.DBRecordTypes;
using Microsoft.Extensions.Logging;
using Azure.Messaging.ServiceBus;
using Newtonsoft.Json;
using System.Collections.Generic; 
using System.Threading.Tasks;
using System.Net.Http;
using Microsoft.Azure.Functions.Worker.Extensions.Sql;
using Microsoft.Data.SqlClient;
using Microsoft.IdentityModel.Protocols;
using System.Configuration;
using Moonman.Errors;
using Microsoft.VisualBasic;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Schema;




namespace Moonman.Function
{
    public class Sync_Queue_DB
    {
        readonly string queueConnectionString = System.Environment.GetEnvironmentVariable("QueueConnectionString", EnvironmentVariableTarget.Process);
        readonly string errorQueueConnectionString = System.Environment.GetEnvironmentVariable("ErrorQueueConnectionString");
        readonly string queueName = "genesis-queue-0";
        readonly string errorQueueName = "genesis-error-00";
        readonly string sqlConnectionString = System.Environment.GetEnvironmentVariable("SqlConnectionString", EnvironmentVariableTarget.Process);
        readonly JSchema schema = JSchema.Parse(
            @"{
                'title': 'Order',
                'type': 'object',
                'properties': {
                    'OrderId': {'type': 'string', required: true},
                    'Quantity': {'type': 'integer', required: true},
                    'UnitPrice': {type: 'number', required: true}
                },
                'additionalProperties': false
            }"
        );


        [FunctionName("Sync_Queue_DB")]
        public async Task Run([TimerTrigger("0 */40 * * * *")]TimerInfo myTimer, ILogger log)
        {
            var currentMessage = new Object();
            if (myTimer.IsPastDue) {
                log.LogInformation("Timer is running late!");
            }
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            ServiceBusClient serviceBusErrorClient = new(errorQueueConnectionString);
            ServiceBusSender serviceBusSender = serviceBusErrorClient.CreateSender(errorQueueName);
            try
            {
                ServiceBusClient serviceBusClient = new (queueConnectionString);
                ServiceBusReceiver serviceBusReceiver = serviceBusClient.CreateReceiver(queueName, new ServiceBusReceiverOptions() {ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete});

                IAsyncEnumerable<ServiceBusReceivedMessage> messages = serviceBusReceiver.ReceiveMessagesAsync();
                int count = 0;



                SqlConnection connection = new(sqlConnectionString);
                connection.Open();
                await foreach (var message in messages) {
                    currentMessage = message;
                    count++;
                    JObject jsonObj = JObject.Parse(message.Body.ToString());
                    log.LogInformation(jsonObj.IsValid(schema).ToString());
                    
                    if (jsonObj.IsValid(schema)) {
                        Order order = JsonConvert.DeserializeObject<Order>(message.Body.ToString());
                        OrderOutput orderOutput = new() {
                            Order = order
                        };

                        string insertQuery = "INSERT INTO Orders (OrderId, Quantity, UnitPrice) VALUES (@Value1, @Value2, @Value3)";

                            
                            SqlCommand sqlCommand = new (insertQuery, connection);
                            sqlCommand.Parameters.AddWithValue("@Value1", order.OrderId);
                            sqlCommand.Parameters.AddWithValue("@Value2", order.Quantity);
                            sqlCommand.Parameters.AddWithValue("@Value3", order.UnitPrice);
                            using SqlDataReader reader = sqlCommand.ExecuteReader();
                    }
                    else {
                        ServiceBusMessage serviceBusMessage = new ServiceBusMessage(JsonConvert.SerializeObject(message));
                        await serviceBusSender.SendMessageAsync(serviceBusMessage);
                    }
                        
                    
                    
                }
                connection.Close();

                log.LogInformation($"Successfully picked {count} messages from queue and pushed to DB!");
            }
            catch (System.Exception e)
            {
                log.LogError(e.ToString());
                // Error err = new() {
                //     ErrorDescription = e.ToString(),
                //     QueueMessage = currentMessage
                // };
                // ServiceBusMessage serviceBusMessage = new ServiceBusMessage(JsonConvert.SerializeObject(err));
                // await serviceBusSender.SendMessageAsync(serviceBusMessage);
            }
        }
    }
}
