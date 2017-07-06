
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Graphs;
using Newtonsoft.Json;

namespace GitHubArchiveParser
{
    class Program
    {
        private static ConcurrentDictionary<string, HashSet<string>> Users =
            new ConcurrentDictionary<string, HashSet<string>>();

        private static ConcurrentDictionary<string, HashSet<Tuple<InteractionType, string>>> InteractionMap =
            new ConcurrentDictionary<string, HashSet<Tuple<InteractionType, string>>>();

        private static ConcurrentBag<Interaction> Interactions = new ConcurrentBag<Interaction>();

        private static int vcount = 0;
        private static int ecount = 0;
        private static double consumption = 0;

        static void Main(string[] args)
        {
            var endpoint = ConfigurationManager.AppSettings["endpoint"];
            var key = ConfigurationManager.AppSettings["key"];
            var dbName = ConfigurationManager.AppSettings["database"];
            var graphName = ConfigurationManager.AppSettings["graph"];

            ParseJson();
            ProcessUsers(endpoint, key, dbName, graphName);
        }

        private static void ProcessUsers(string endpoint, string key, string dbName, string graphName)
        {
            Console.WriteLine("Creating users and repos...");
            Console.WriteLine();

            var client = new DocumentClient(new Uri(endpoint), key, new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp
            });

            var graph =
                client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(dbName))
                    .Where(c => c.Id == graphName)
                    .ToArray()
                    .Single();

            var cleanup = client.CreateGremlinQuery(graph, "g.V().drop()");

            cleanup.ExecuteNextAsync().Wait();

            Func<Interaction, Task> upload = async interaction =>
            {
                var addUser = false;

                Users.GetOrAdd(interaction.User, _ =>
                {
                    addUser = true;
                    return new HashSet<string>();
                });

                var addRepoOwner = false;

                var reposForOwner = Users.GetOrAdd(interaction.RepoOwner, _ =>
                {
                    addRepoOwner = true;
                    return new HashSet<string>();
                });

                var addRepo = false;

                lock (reposForOwner)
                {
                    addRepo = reposForOwner.Add(interaction.Repo);
                }

                var map = InteractionMap.GetOrAdd(interaction.User, _ => new HashSet<Tuple<InteractionType, string>>());

                var addInteraction = false;

                lock (map)
                {
                    addInteraction = map.Add(Tuple.Create(interaction.Type, interaction.Repo));
                }

                if (addUser)
                {
                    var query = client.CreateGremlinQuery(graph, $"g.addV('user').property('id', '{interaction.User}')");
                    var result = await query.ExecuteNextAsync();
                    Interlocked.Exchange(ref consumption, result.RequestCharge + consumption);
                    Interlocked.Increment(ref vcount);
                }

                if (addRepoOwner)
                {
                    var query = client.CreateGremlinQuery(graph, $"g.addV('user').property('id', '{interaction.RepoOwner}')");
                    var result = await query.ExecuteNextAsync();
                    Interlocked.Exchange(ref consumption, result.RequestCharge + consumption);
                    Interlocked.Increment(ref vcount);
                }

                if (addRepo)
                {
                    var query = client.CreateGremlinQuery(graph, $"g.addV('repo').property('id', '{interaction.Repo}')");
                    var result = await query.ExecuteNextAsync();
                    Interlocked.Exchange(ref consumption, result.RequestCharge + consumption);
                    Interlocked.Increment(ref vcount);

                    query = client.CreateGremlinQuery(graph, $"g.V('{interaction.RepoOwner}').addE('owns').to(g.V('{interaction.Repo}'))");
                    result = await query.ExecuteNextAsync();
                    Interlocked.Exchange(ref consumption, result.RequestCharge + consumption);
                    Interlocked.Increment(ref ecount);

                    query = client.CreateGremlinQuery(graph, $"g.V('{interaction.Repo}').addE('owned by').to(g.V('{interaction.RepoOwner}'))");
                    result = await query.ExecuteNextAsync();
                    Interlocked.Exchange(ref consumption, result.RequestCharge + consumption);
                    Interlocked.Increment(ref ecount);
                }

                if (addInteraction)
                {
                    var interest = string.Empty;
                    var inverseInterest = string.Empty;

                    switch (interaction.Type)
                    {
                        case InteractionType.CommentIssue:
                            interest = "commented on an issue for";
                            inverseInterest = "issue was commented on by";
                            break;

                        case InteractionType.ForkRepo:
                            interest = "forked";
                            inverseInterest = "was forked by";
                            break;

                        case InteractionType.OpenIssue:
                            interest = "opened an issue for";
                            inverseInterest = "issue was opened by";
                            break;

                        case InteractionType.PullRequest:
                            interest = "issued a pull request for";
                            inverseInterest = "pull request was issued by";
                            break;

                        case InteractionType.WatchRepo:
                            interest = "starred";
                            inverseInterest = "was starred by";
                            break;
                    }

                    var query1 = client.CreateGremlinQuery(graph,
                        $"g.V('{interaction.User}').addE('{interest}').to(g.V('{interaction.Repo}'))");
                    var result1 = await query1.ExecuteNextAsync();
                    Interlocked.Exchange(ref consumption, result1.RequestCharge + consumption);
                    Interlocked.Increment(ref ecount);

                    query1 = client.CreateGremlinQuery(graph,
                        $"g.V('{interaction.Repo}').addE('{inverseInterest}').to(g.V('{interaction.User}'))");
                    result1 = await query1.ExecuteNextAsync();
                    Interlocked.Exchange(ref consumption, result1.RequestCharge + consumption);
                    Interlocked.Increment(ref ecount);
                }
            };

            var stopwatch = new Stopwatch();
            var report = true;

            Func<Task> reportFunc = async () =>
            {
                while (report)
                {
                    Console.Write($"\rVertices: {vcount} Edges: {ecount} RU/s: {consumption / stopwatch.Elapsed.TotalSeconds}                      ");
                    await Task.Delay(1000);
                }
            };

            var reportTask = reportFunc();

            stopwatch.Start();
            
            Interactions.AsyncParallelForEach(upload, 32).Wait();

            report = false;

            stopwatch.Stop();
        }

        private static void ParseJson()
        {
            Parallel.For(1, 6, day =>
            {
                Parallel.For(0, 24, hour =>
                {
                    using (var stream = GetStreamForFile(day, hour))
                    using (var gzip = new GZipStream(stream, CompressionMode.Decompress))
                    using (var reader = new StreamReader(gzip))
                    using (var jsonReader = new JsonTextReader(reader))
                    {
                        jsonReader.SupportMultipleContent = true;

                        var serializer = new JsonSerializer();

                        while (true)
                        {
                            if (!jsonReader.Read())
                                break;

                            dynamic evt = serializer.Deserialize(jsonReader);

                            switch ((string) evt.type)
                            {
                                case "WatchEvent":
                                    HandleWatchEvent(evt);
                                    break;

                                case "ForkEvent":
                                    HandleForkEvent(evt);
                                    break;

                                case "IssueCommentEvent":
                                    HandleIssueCommentEvent(evt);
                                    break;

                                case "IssuesEvent":
                                    HandleIssuesEvent(evt);
                                    break;

                                case "PullRequestEvent":
                                    HandlePullRequestEvent(evt);
                                    break;

                                default:
                                    break;
                            }
                        }
                    }

                    Console.WriteLine($"Day {day} hour {hour} complete");
                });

                Console.WriteLine($"Day {day} complete");
            });
        }

        private static Stream GetStreamForFile(int day, int hour)
        {
            var file = $"2017-06-0{day}-{hour}.json.gz";

            var filepath = "..\\..\\data\\" + file;

            if (!File.Exists(filepath))
            {
                var url = "https://opensourcecontributo.rs/archive/events/" + file;

                var http = new HttpClient();

                using (var stream = http.GetStreamAsync(url).Result)
                using (var fs = File.OpenWrite(filepath))
                {
                    stream.CopyTo(fs);
                }
            }

            return File.OpenRead(filepath);
        }

        private static string GetOwnerForRepo(dynamic repo)
        {
            return ((string) repo.name).Split('/')[0].Trim();
        }

        private static void HandlePullRequestEvent(dynamic evt)
        {
            if (evt.payload.action == "opened")
            {
                string user = evt.actor.login;
                string repoOwner = GetOwnerForRepo(evt.repo);
                string repo = ((string)evt.repo.name).Replace("/", "-");

                Interactions.Add(new Interaction
                {
                    User = user,
                    RepoOwner = repoOwner,
                    Repo = repo,
                    Type = InteractionType.PullRequest
                });
            }
        }

        private static void HandleIssuesEvent(dynamic evt)
        {
            if (evt.payload.action == "opened")
            {
                string user = evt.actor.login;
                string repoOwner = GetOwnerForRepo(evt.repo);
                string repo = ((string)evt.repo.name).Replace("/", "-");

                Interactions.Add(new Interaction
                {
                    User = user,
                    RepoOwner = repoOwner,
                    Repo = repo,
                    Type = InteractionType.OpenIssue
                });
            }
        }

        private static void HandleIssueCommentEvent(dynamic evt)
        {
            if (evt.payload.action == "created")
            {
                string user = evt.actor.login;
                string repoOwner = GetOwnerForRepo(evt.repo);
                string repo = ((string)evt.repo.name).Replace("/", "-");

                Interactions.Add(new Interaction
                {
                    User = user,
                    RepoOwner = repoOwner,
                    Repo = repo,
                    Type = InteractionType.CommentIssue
                });
            }
        }

        private static void HandleForkEvent(dynamic evt)
        {
            string user = evt.actor.login;
            string repoOwner = GetOwnerForRepo(evt.repo);
            string repo = ((string)evt.repo.name).Replace("/", "-");

            Interactions.Add(new Interaction
            {
                User = user,
                RepoOwner = repoOwner,
                Repo = repo,
                Type = InteractionType.ForkRepo
            });
        }

        private static void HandleWatchEvent(dynamic evt)
        {
            string user = evt.actor.login;
            string repoOwner = GetOwnerForRepo(evt.repo);
            string repo = ((string)evt.repo.name).Replace("/", "-");

            Interactions.Add(new Interaction
            {
                User = user,
                RepoOwner = repoOwner,
                Repo = repo,
                Type = InteractionType.WatchRepo
            });
        }
    }

    class Interaction
    {
        public string User { get; set; }
        public string RepoOwner { get; set; }
        public string Repo { get; set; }
        public InteractionType Type { get; set; }
    }

    enum InteractionType
    {
        PullRequest = 1,
        OpenIssue,
        CommentIssue,
        ForkRepo,
        WatchRepo
    }
}
