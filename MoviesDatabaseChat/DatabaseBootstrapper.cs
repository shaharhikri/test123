using System.Globalization;
using System.Text.RegularExpressions;
using MoviesDatabaseChat.Entities;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Remote;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;


namespace MoviesDatabaseChat
{
    internal static class DatabaseBootstrapper
    {

        public static async Task<bool> CreateDatabaseAsync(IDocumentStore store, Action<string> log, bool smallDb)
        {
            if (await DatabaseExistsAsync(store, store.Database)) // db exists -> don't create it again
                return false;
            
            log($"Creating database '{store.Database}'");
            
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(store.Database)));
            log("Initialize Movies Database:");
            
            await GetS3Settings(store, "movies_images");
            await AddMoviesAsync(store, log, 40_000);
            await AddMoviesAttachmentsAsync(store, log, 2000);
            return true;
        }

        private static async Task<bool> DatabaseExistsAsync(IDocumentStore store, string database)
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
            return record != null;
        }

        public static Task GetS3Settings(IDocumentStore store, string remoteFolderName)
        {
            var s3SettingsString = Environment.GetEnvironmentVariable("S3_CREDENTIAL");
            if (s3SettingsString == null)
            {
                throw new InvalidOperationException(@"""S3_CREDENTIAL"" is missing");
            }

            var s3Settings = JsonConvert.DeserializeObject<S3Settings>(s3SettingsString);
            if (s3Settings == null)
                throw new InvalidOperationException(@"""S3_CREDENTIAL"" is empty");

            var settings_s3 = new S3Settings
            {
                BucketName = s3Settings.BucketName,
                RemoteFolderName = remoteFolderName,
                AwsAccessKey = s3Settings.AwsAccessKey,
                AwsSecretKey = s3Settings.AwsSecretKey,
                AwsRegionName = s3Settings.AwsRegionName
            };

            var s = new RemoteAttachmentsS3Settings
            {
                AwsAccessKey = settings_s3.AwsAccessKey,
                AwsSecretKey = settings_s3.AwsSecretKey,
                AwsSessionToken = settings_s3.AwsSessionToken,
                AwsRegionName = settings_s3.AwsRegionName,
                RemoteFolderName = settings_s3.RemoteFolderName,
                BucketName = settings_s3.BucketName,
                CustomServerUrl = settings_s3.CustomServerUrl,
                ForcePathStyle = settings_s3.ForcePathStyle,
                StorageClass = settings_s3.StorageClass
            };

            var conf = new RemoteAttachmentsConfiguration
            {
                Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                {
                    {
                        "conf-identifier", new RemoteAttachmentsDestinationConfiguration()
                        {
                            Disabled = false,
                            S3Settings = s,
                        }
                    }
                },
                CheckFrequencyInSec = 1,
            };
            return store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRemoteAttachmentsOperation(conf));
        }


        private static async Task AddMoviesAsync(IDocumentStore store, Action<string> log, int maxCount)
        {
            log("Movies");

            using var reader = new StreamReader(Path.Combine("Csvs", "movies.csv"));
            using var csv = new CsvHelper.CsvReader(reader, CultureInfo.InvariantCulture);

            int count = 0;

            using (var bulkInsert = store.BulkInsert())
            {
                foreach (var row in csv.GetRecords<MovieCsvRow>())
                {
                    if (count >= maxCount)
                        break;

                    var movie = row.ToMovie();
                    if (movie == null)
                        continue;

                    await bulkInsert.StoreAsync(movie);

                    if (count % 10_000 == 0)
                    {
                        log($"Saved {count} movies...");
                    }

                    count++;
                }
            }

            log($"Done! Total saved: {count}");
        }

        private static async Task AddMoviesAttachmentsAsync(IDocumentStore store, Action<string> log, int maxCount)
        {
            log("Movies Attachments");
        
            using var reader = new StreamReader(Path.Combine("Csvs", "movies.csv"));
            using var csv = new CsvHelper.CsvReader(reader, CultureInfo.InvariantCulture);
        
            var count = 0;
        
            var tasks = new List<Task>();
        
            var remoteAt1 = DateTime.UtcNow - TimeSpan.FromDays(1);

            var limit = 0;
            bool keepRunning = true;
            while (keepRunning)
            {
                foreach (var row in csv.GetRecords<MovieCsvRow>())
                {
                    keepRunning = false;
                    if (count >= maxCount)
                    {
                        break;
                    }
                    if (count < limit)
                    {
                        count++;
                        continue;
                    }

                    var count1 = count;
                    var t = Task.Run(async () =>
                    {
                        using var bigJpeg = new StreamingBmpStream(46, seed: count1+1);

                        var movie = row.ToMovie();
                        if (movie == null)
                            return;
                        var movieId = movie.Id;
                        var parameters1 = count1 >= maxCount * 0.9
                            ? new StoreAttachmentParameters("cover1.png", bigJpeg)
                            : new StoreAttachmentParameters("cover2.png", bigJpeg)
                            {
                                RemoteParameters = new RemoteAttachmentParameters("conf-identifier", remoteAt1)
                            };
                        var putOp1 = new PutAttachmentOperation(movieId, parameters1);
                        await store.Operations.SendAsync(putOp1);
                    });

                    tasks.Add(t);
                    if (tasks.Count >= 500)
                    {
                        try
                        {
                            await Task.WhenAll(tasks);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Exception "+DateTime.Now+" "+e.GetType().Name+" "+e.Message.Substring(Int32.Min(e.Message.Length, 100)));
                            keepRunning = true;
                            tasks.Clear();
                            await Task.Delay(15_000);
                            break;
                        }

                        tasks.Clear();
                        limit = count;
                    }

                    if (count % 500 == 0)
                    {
                        log($"Saved {count} attachments... limit {limit}");
                    }

                    count++;
                }
            }

            log($"Done! attachments {count} - limit {limit}");
        }

        private class MovieCsvRow
        {
            public int movieId { get; set; }
            public string title { get; set; }
            public string genres { get; set; }

            public Movie ToMovie()
            {
                var match = Regex.Match(this.title, @"^(.*) \((\d{4})\)$");
                if (match.Success == false)
                    return null;

                var title = match.Groups[1].Value.Trim();
                var yearStr = match.Groups[2].Value;

                if (int.TryParse(yearStr, out int year) == false)
                    return null;

                var genresArr = genres.Split('|', StringSplitOptions.RemoveEmptyEntries);

                return new Movie
                {
                    Id = $"Movies/{movieId}",
                    Title = title,
                    Year = year,
                    Genres = genresArr
                };
            }

            public override string ToString()
            {
                return $"{movieId}, {title}, {genres}";
            }
        }
    }
}