using FishyFlip;
using FishyFlip.Lexicon.Blue.Zio.Atfile;
using FishyFlip.Lexicon.Com.Atproto.Repo;
using FishyFlip.Lexicon.Com.Atproto.Sync;
using FishyFlip.Models;
using FishyFlip.Tools;
using FubarDev.FtpServer;
using FubarDev.FtpServer.BackgroundTransfer;
using FubarDev.FtpServer.FileSystem;
using Microsoft.Extensions.Logging.Debug;

namespace AtFileWebDav;

public class AtFileFileSystemProvider : IFileSystemClassFactory
{
    private readonly ATDid _accountName;
    private readonly string _password;
    private readonly ATProtocol _atProtocol;
        
    /// <summary>
    /// Gets or sets a value indicating whether deletion of non-empty directories is allowed.
    /// </summary>
    public bool AllowNonEmptyDirectoryDelete { get; set; }

    public AtFileFileSystemProvider(ATDid accountName, string password)
    {
        _accountName = accountName;
        _password = password;
        // Include a ILogger if you want additional logging from the base library.
        var debugLog = new DebugLoggerProvider();
        var atProtocolBuilder = new ATProtocolBuilder()
            .EnableAutoRenewSession(true)
            // Set the instance URL for the PDS you wish to connect to.
            // Defaults to bsky.social.
            .WithInstanceUrl(new Uri("https://bsky.social"))
            .WithLogger(debugLog.CreateLogger("FishyFlipDebug"));
        _atProtocol = atProtocolBuilder.Build();
    }

    public Task<IUnixFileSystem> Create(IAccountInformation accountInformation)
    {
        return Task.FromResult<IUnixFileSystem>(new AtFileFileSystem(_atProtocol, _accountName, _password));
    }
}

public class AtFileFileSystemEntry : IUnixFileSystemEntry
{
    public AtFileFileSystemEntry(string name, string? rkey = null)
    {
        Name = name;
        Rkey = rkey ?? Rkeys.FromFilePath(Name);
    }

    public string Owner => "owner";
    public string Group => "group";
    public string Name { get; }
    public IUnixPermissions Permissions { get; } = new DefaultPermissions();
    public DateTimeOffset? LastWriteTime => null;
    public DateTimeOffset? CreatedTime => null;
    public long NumberOfLinks => 1;

    public string Rkey { get; }
}

public class AtFileDirectoryEntry : AtFileFileSystemEntry, IUnixDirectoryEntry
{
    public AtFileDirectoryEntry(string name, bool isRoot, string? rkey = null) : base(name, rkey)
    {
        IsRoot = isRoot;
    }

    public bool IsRoot { get; }
    public bool IsDeletable => !IsRoot;
    
    public IDictionary<string, AtFileFileEntry> Files { get; } = new Dictionary<string, AtFileFileEntry>();
}

public class AtFileFileEntry : AtFileFileSystemEntry, IUnixFileEntry
{
    public AtFileFileEntry(string name, long size, string? rkey = null) : base(name, rkey)
    {
        Size = size;
    }

    public long Size { get; }
}

public class DefaultPermissions : IUnixPermissions
{
    public IAccessMode User { get; } = new DefaultAccessMode();
    public IAccessMode Group { get; } = new DefaultAccessMode();
    public IAccessMode Other { get; } = new DefaultAccessMode();
}

public class DefaultAccessMode : IAccessMode
{
    public bool Read => true;
    public bool Write => true;
    public bool Execute => true;
}

public class AtFileFileSystem : IUnixFileSystem
{
    private readonly ATProtocol _atProtocol;
    private readonly ATDid _did;
    private readonly string _password;

    public AtFileFileSystem(ATProtocol atProtocol, ATDid did, string password)
    {
        _atProtocol = atProtocol;
        _did = did;
        _password = password;
    }

    public bool SupportsAppend => false;
    public bool SupportsNonEmptyDirectoryDelete => false;
    public StringComparer FileSystemEntryComparer => StringComparer.Ordinal;
    public IUnixDirectoryEntry Root { get; } = new AtFileDirectoryEntry(".", true);
    
    public async Task<IReadOnlyList<IUnixFileSystemEntry>> GetEntriesAsync(IUnixDirectoryEntry directoryEntry, CancellationToken cancellationToken)
    {
        var uploads = (await _atProtocol.ListUploadAsync(_did, cancellationToken: cancellationToken))
            .HandleResult();

        return uploads?.Records?
            .Select(CreateFileEntry)
            .ToArray() ?? [];
    }

    private static AtFileFileEntry CreateFileEntry(Record e)
    {
        var upload = (e.Value as Upload)!;
        var rkey = e.Uri!.Rkey;
        return CreateFileEntry(upload, rkey);
    }

    private static AtFileFileEntry CreateFileEntry(Upload upload, string rkey)
    {
        return new AtFileFileEntry($"{rkey}:{upload.File?.Name ?? "UNK"}", upload.File?.Size ?? upload.Blob?.Size ?? 0, rkey);
    }

    public async Task<IUnixFileSystemEntry?> GetEntryByNameAsync(IUnixDirectoryEntry directoryEntry, string name, CancellationToken cancellationToken)
    {
        var rkey = name[..name.IndexOf(':')];

        var output = (await _atProtocol.GetUploadAsync(_did, rkey, cancellationToken: cancellationToken))
            .HandleResult();
        
        var upload = (output?.Value as Upload)!;
        
        return new AtFileFileEntry(upload.File?.Name ?? "UNK", upload.File?.Size ?? upload.Blob?.Size ?? 0, rkey);
    }

    public Task<IUnixFileSystemEntry> MoveAsync(IUnixDirectoryEntry parent, IUnixFileSystemEntry source, IUnixDirectoryEntry target, string fileName,
        CancellationToken cancellationToken)
    {
        throw new NotSupportedException();
    }

    public Task UnlinkAsync(IUnixFileSystemEntry entry, CancellationToken cancellationToken)
    {
        throw new NotSupportedException();
    }

    public Task<IUnixDirectoryEntry> CreateDirectoryAsync(IUnixDirectoryEntry targetDirectory, string directoryName, CancellationToken cancellationToken)
    {
        throw new NotSupportedException();
    }

    public async Task<Stream> OpenReadAsync(IUnixFileEntry fileEntry, long startPosition, CancellationToken cancellationToken)
    {
        var upload = (await _atProtocol.GetUploadAsync(
            _did,
            (fileEntry as AtFileFileEntry)!.Rkey,
            cancellationToken: cancellationToken
        )).HandleResult()?.Value as Upload;

        if (upload?.Blob?.Ref?.Link is not { } refLink)
        {
            throw new InvalidOperationException();
        }

        var blob = (await _atProtocol.GetBlobAsync(_did, refLink, cancellationToken: cancellationToken))
            .HandleResult();

        return new MemoryStream(blob!);
    }

    public Task<IBackgroundTransfer?> AppendAsync(IUnixFileEntry fileEntry, long? startPosition, Stream data, CancellationToken cancellationToken)
    {
        throw new NotSupportedException();
    }

    public Task<IBackgroundTransfer?> CreateAsync(IUnixDirectoryEntry targetDirectory, string fileName, Stream data,
        CancellationToken cancellationToken)
    {
        throw new NotSupportedException();
    }

    public Task<IBackgroundTransfer?> ReplaceAsync(IUnixFileEntry fileEntry, Stream data, CancellationToken cancellationToken)
    {
        throw new NotSupportedException();
    }

    public Task<IUnixFileSystemEntry> SetMacTimeAsync(IUnixFileSystemEntry entry, DateTimeOffset? modify, DateTimeOffset? access, DateTimeOffset? create,
        CancellationToken cancellationToken)
    {
        throw new NotSupportedException();
    }
}