﻿// See https://aka.ms/new-console-template for more information

// Setup dependency injection

using System.Security.Claims;
using System.Security.Principal;
using AtFileWebDav;
using dotenv.net;
using FishyFlip.Models;
using FubarDev.FtpServer;
using FubarDev.FtpServer.AccountManagement;
using FubarDev.FtpServer.AccountManagement.Compatibility;
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
    .AddSingleton<IMembershipProvider>(new AllowAnyMembershipProvider())
    .AddFtpServer(builder =>
    {
        
    }); // allow anonymous logins

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

public class AllowAnyMembershipProvider : IMembershipProvider
{
    public Task<MemberValidationResult> ValidateUserAsync(string username, string password)
    {
        return Task.FromResult(new MemberValidationResult(MemberValidationStatus.AuthenticatedUser, new ClaimsPrincipal(new BlueskyIdentity(username, password))));
    }
}

public class BlueskyIdentity(string name, string password) : IIdentity
{
    public string? AuthenticationType => "bluesky";
    public bool IsAuthenticated => true;
    public string? Name { get; } = name;
    public string Password { get; } = password;
}