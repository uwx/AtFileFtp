// See https://aka.ms/new-console-template for more information

// Setup dependency injection

using AtFileWebDav;
using dotenv.net;
using FishyFlip.Models;
using FubarDev.FtpServer;
using FubarDev.FtpServer.FileSystem;
using Microsoft.Extensions.DependencyInjection;

DotEnv.Load(options: new DotEnvOptions(probeForEnv: true, probeLevelsToSearch: 999));

var services = new ServiceCollection();

// Add FTP server services
// DotNetFileSystemProvider = Use the .NET file system functionality
// AnonymousMembershipProvider = allow only anonymous logins
services
    .AddSingleton<IFileSystemClassFactory>(new AtFileFileSystemProvider(
        ATDid.Create(Environment.GetEnvironmentVariable("BSKY_USERNAME")!)!,
        Environment.GetEnvironmentVariable("BSKY_PASSWORD")!,
        Environment.GetEnvironmentVariable("BSKY_PDS")!
    ))
    .AddFtpServer(builder => builder
        .EnableAnonymousAuthentication()); // allow anonymous logins

// Configure the FTP server
services.Configure<FtpServerOptions>(opt => opt.ServerAddress = "127.0.0.1");

// Build the service provider
await using (var serviceProvider = services.BuildServiceProvider())
{
    // Initialize the FTP server
    var ftpServerHost = serviceProvider.GetRequiredService<IFtpServerHost>();

    // Start the FTP server
    await ftpServerHost.StartAsync(CancellationToken.None);

    Console.WriteLine("Press ENTER/RETURN to close the test application.");
    Console.ReadLine();

    // Stop the FTP server
    await ftpServerHost.StopAsync(CancellationToken.None);
}