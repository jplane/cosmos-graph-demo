
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Graphs;
using Newtonsoft.Json;

namespace QueryTool
{
    class Program
    {
        static void Main(string[] args)
        {
            var endpoint = ConfigurationManager.AppSettings["endpoint"];
            var key = ConfigurationManager.AppSettings["key"];
            var dbName = ConfigurationManager.AppSettings["database"];
            var graphName = ConfigurationManager.AppSettings["graph"];

            Console.WriteLine("Welcome to the Cosmos DB Graphalyzer 5000");
            Console.WriteLine();

            while (true)
            {
                Console.Write("> ");

                var input = Console.ReadLine();

                if (input == "exit")
                {
                    break;
                }

                Console.WriteLine();

                var client = new DocumentClient(new Uri(endpoint), key);
                var graph =
                    client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(dbName))
                        .Where(c => c.Id == graphName)
                        .ToArray()
                        .Single();

                var query = client.CreateGremlinQuery(graph, input);

                while (query.HasMoreResults)
                {
                    foreach (var result in query.ExecuteNextAsync().Result)
                    {
                        Console.WriteLine($"\t {JsonConvert.SerializeObject(result)}");
                    }
                }

                Console.WriteLine();
            }
        }
    }
}
