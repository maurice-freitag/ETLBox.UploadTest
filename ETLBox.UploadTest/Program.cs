using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ETLBox.UploadTest
{
    internal static class Program
    {
        private static readonly bool resetData = false;
        private static readonly int dataEntries = 5_000;
        private static readonly Random r = new();

        private static readonly string sqlServerConnectionString = "";
        private static readonly string storageAccountConnectionString = "";
        private static readonly string containerName = "";

        public static async Task Main()
        {
            Logging.Logging.LogInstance = GetLogger();
            await PrepareDataAsync().ConfigureAwait(false);
            await ExportDataAsync().ConfigureAwait(false);
        }

        private static ILogger GetLogger()
        {
            var services = new ServiceCollection();
            services.AddLogging(x => x.AddConsole());
            return services.BuildServiceProvider().GetRequiredService<ILogger<Network>>();
        }

        private static async Task ExportDataAsync()
        {
            var network = CreateNetwork();
            await network.ExecuteAsync().ConfigureAwait(false);
        }

        private static Network CreateNetwork()
        {
            using var connectionManager = new SqlConnectionManager(sqlServerConnectionString);
            var source = new DbSource<MyModel>(connectionManager) { Sql = MyModel.Sql };

            var destination = new ParquetDestinationEx<MyModel>(GetFileName(0), ResourceType.AzureBlob);
            destination.AzureBlobStorage.ConnectionString = storageAccountConnectionString;
            destination.AzureBlobStorage.ContainerName = containerName;
            destination.AzureBlobStorage.BlockBlobOpenWriteOptions = new()
            {
                Tags = new Dictionary<string, string>
                {
                    { "state", "unprocessed" },
                    { "tenantid", $"{Guid.NewGuid()}" },
                }
            };
            source.LinkTo(destination);
            return new Network(source);
        }

        private static string GetFileName(int progress)
        {
            var fileNameTemplate = "MyModel_d{Date}_{Progress}.parquet";
            var fileName = fileNameTemplate.Replace("{Date}", DateTime.UtcNow.ToString("yyyyMMddHHmmss"));
            fileName = fileName.Replace("{Progress}", progress.ToString());
            return fileName;
        }

        private static async Task PrepareDataAsync()
        {
            Console.WriteLine("Preparing data.");
            using var connectionManager = new SqlConnectionManager(sqlServerConnectionString);
            var tables = GetTableListTask.ListAll(connectionManager);
            var tableExists = tables.Exists(x => x.UnquotatedObjectName == MyModel.TableName);

            if (!tableExists)
            {
                Console.WriteLine($"Ensuring table {MyModel.TableName} exists.");
                CreateTableTask.CreateIfNotExists(connectionManager, MyModel.TableName,
                    typeof(MyModel).GetProperties().Select(x => new ControlFlow.TableColumn(
                        name: x.Name,
                        dataType: GetDataType(x.PropertyType),
                        allowNulls: false,
                        isPrimaryKey: x.Name == nameof(MyModel.Id)
                        )).ToList());
            }
            if (!tableExists || resetData)
            {
                Console.WriteLine($"Deleting everything from {MyModel.TableName}.");
                SqlTask.ExecuteNonQuery(connectionManager,
                    $"DELETE FROM {MyModel.TableName}");

                var remainingEntries = dataEntries;
                var batchSize = 100_000;
                while (remainingEntries > 0)
                {
                    Console.WriteLine($"{remainingEntries} entries remaining.");
                    await PrepareDataBatchAsync(Math.Min(remainingEntries, batchSize), connectionManager).ConfigureAwait(false);
                    remainingEntries -= batchSize;
                }
            }
        }

        private static string GetDataType(Type propertyType)
        {
            if (propertyType == typeof(string))
                return "NVARCHAR(128)";
            if (propertyType == typeof(Guid))
                return "UNIQUEIDENTIFIER";
            if (propertyType == typeof(int))
                return "INT";
            if (propertyType == typeof(double))
                return "DECIMAL(28,8)";
            if (propertyType == typeof(DateTime))
                return "DATETIME";
            throw new NotSupportedException();
        }

        private static async Task PrepareDataBatchAsync(int batchSize, SqlConnectionManager connectionManager)
        {
            var source = new MemorySource<MyModel>
            {
                Data = Enumerable.Range(0, batchSize).Select(_ => GetModel())
            };
            var destination = new DbDestination<MyModel>(connectionManager, MyModel.TableName);
            source.LinkTo(destination);
            var network = new Network(source);
            await network.ExecuteAsync().ConfigureAwait(false);
        }

        private static MyModel GetModel() => new()
        {
            Id = Guid.NewGuid(),
            SomeInt1 = r.Next(),
            SomeInt2 = r.Next(),
            SomeId1 = Guid.NewGuid(),
            SomeId2 = Guid.NewGuid(),
            SomeId3 = Guid.NewGuid(),
            Starttime = DateTime.Now.AddSeconds(r.NextDouble()),
            Endtime = DateTime.Now.AddSeconds(r.NextDouble()),
            SomeDouble1 = r.NextDouble(),
            SomeDouble2 = r.NextDouble(),
            SomeDouble3 = r.NextDouble(),
            SomeDouble4 = r.NextDouble(),
            SomeDouble5 = r.NextDouble(),
            SomeDouble6 = r.NextDouble(),
            SomeDouble7 = r.NextDouble(),
            SomeDouble8 = r.NextDouble(),
            SomeString = $"{Guid.NewGuid()}{Guid.NewGuid()}"
        };
    }
}