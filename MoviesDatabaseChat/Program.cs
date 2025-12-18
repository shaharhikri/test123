using MoviesDatabaseChat;
using Raven.Client.Documents;

class Program
{
    public static async Task Main()
    {
        using var store = new DocumentStore
        {
            Urls = new[] { "http://localhost:8080" },
            Database = "MoviesDB2"
        }.Initialize();

        if (await DatabaseBootstrapper.CreateDatabaseAsync(store, log: Console.WriteLine, smallDb: true))
        {
            Console.WriteLine($"Database '{store.Database}' is ready on your local server, run again for chat");
            return;
        }
    }

}