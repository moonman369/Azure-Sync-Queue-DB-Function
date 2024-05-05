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




namespace Moonman.Function
{
    public class Sync_Queue_DB
    {
        readonly string queueConnectionString = System.Environment.GetEnvironmentVariable("QueueConnectionString", EnvironmentVariableTarget.Process);
        readonly string queueName = "genesis-queue-0";
        


        [FunctionName("Sync_Queue_DB")]
        public async Task Run([TimerTrigger("0 */40 * * * *")]TimerInfo myTimer, ILogger log)
        {
            if (myTimer.IsPastDue) {
                log.LogInformation("Timer is running late!");
            }
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            try
            {
                ServiceBusClient serviceBusClient = new (queueConnectionString);
                ServiceBusReceiver serviceBusReceiver = serviceBusClient.CreateReceiver(queueName, new ServiceBusReceiverOptions() {ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete});

                IAsyncEnumerable<ServiceBusReceivedMessage> messages = serviceBusReceiver.ReceiveMessagesAsync();
                int count = 0;



                SqlConnection connection = new(System.Environment.GetEnvironmentVariable("SqlConnectionString", EnvironmentVariableTarget.Process));
                connection.Open();
                await foreach (var message in messages) {
                    count++;
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
                connection.Close();

                log.LogInformation($"Successfully picked {count} messages from queue and pushed to DB!");
            }
            catch (System.Exception e)
            {
                log.LogError(e.ToString());
            }
        }
    }
}
