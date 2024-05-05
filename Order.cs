using System;
using Microsoft.Azure.Functions.Worker.Extensions.Sql;
using System.Net.Http;
using Microsoft.IdentityModel.Protocols;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;

// string dbConnectionString = "Server=tcp:functions-sql-server-0.database.windows.net,1433;Initial Catalog=functions-db-0;Persist Security Info=False;User ID=moonman369;Password={your_password};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";

namespace Moonman.DBRecordTypes
{
    public class Order {
        
        public Guid OrderId {get; set;}
        public int Quantity {get; set;}
        public float UnitPrice {get; set;}
    }

    public class OrderOutput {
        [SqlOutput("dbo.Orders", connectionStringSetting: "SqlConnection")]
        public Order Order {get; set;}
    }
}