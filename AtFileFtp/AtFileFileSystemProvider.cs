using System.Net.Http.Headers;
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
using File = FishyFlip.Lexicon.Blue.Zio.Atfile.File;

namespace AtFileWebDav;

public class AtFileFileSystemProvider : IFileSystemClassFactory
{
    private readonly ATDid _accountName;
    private readonly string _password;
    private readonly ATProtocol _atProtocol;

    public AtFileFileSystemProvider(ATDid accountName, string password, string pds = "https://bsky.social")
    {
        _accountName = accountName;
        _password = password;
        // Include a ILogger if you want additional logging from the base library.
        var debugLog = new DebugLoggerProvider();
        var atProtocolBuilder = new ATProtocolBuilder()
            .EnableAutoRenewSession(true)
            // Set the instance URL for the PDS you wish to connect to.
            // Defaults to bsky.social.
            .WithInstanceUrl(new Uri(pds))
            .WithLogger(debugLog.CreateLogger("FishyFlipDebug"));
        _atProtocol = atProtocolBuilder.Build();
    }

    public async Task<IUnixFileSystem> Create(IAccountInformation accountInformation)
    {
        (await _atProtocol.AuthenticateWithPasswordResultAsync(_accountName.ToString(), _password)).HandleResult();
        return new AtFileFileSystem(_atProtocol, _accountName, _password);
    }
}

public class AtFileFileSystemEntry : IUnixFileSystemEntry
{
    public AtFileFileSystemEntry(string name, string? rkey = null, DateTimeOffset? modified = null, DateTimeOffset? created = null)
    {
        if (name == null && rkey == null) throw new ArgumentNullException(null, "Either name or rkey must be provided");
        rkey ??= Rkeys.FromFilePath(name!);

        Name = name;

        RealName = name;
        Rkey = rkey;

        LastWriteTime = modified;
        CreatedTime = created;
    }

    public string Owner => "owner";
    public string Group => "group";
    public string Name { get; }
    public IUnixPermissions Permissions { get; } = new DefaultPermissions();
    public DateTimeOffset? LastWriteTime { get; }
    public DateTimeOffset? CreatedTime { get; }
    public long NumberOfLinks => 1;

    public string? RealName { get; }
    public string Rkey { get; }

    private class DefaultPermissions : IUnixPermissions
    {
        public IAccessMode User { get; } = new DefaultAccessMode();
        public IAccessMode Group { get; } = new DefaultAccessMode();
        public IAccessMode Other { get; } = new DefaultAccessMode();

        private class DefaultAccessMode : IAccessMode
        {
            public bool Read => true;
            public bool Write => true;
            public bool Execute => true;
        }
    }
}

public class AtFileDirectoryEntry(string name, bool isRoot, string? rkey = null, DateTimeOffset? modified = null, DateTimeOffset? created = null)
    : AtFileFileSystemEntry(name, name.EndsWith(':') || isRoot ? rkey : rkey + ':', modified, created), IUnixDirectoryEntry
{
    public bool IsRoot { get; } = isRoot;
    public bool IsDeletable => true;
}

public class AtFileFileEntry(string name, long size, string? rkey = null, DateTimeOffset? modified = null, DateTimeOffset? created = null)
    : AtFileFileSystemEntry(name, rkey, modified, created), IUnixFileEntry
{
    public long Size { get; } = size;
}

public class AtFileFileSystem(ATProtocol atProtocol, ATDid did, string password) : IUnixFileSystem
{
    public const string IsDirectoryKey = "This file is a directory. Please do not replace it with data.";
    
    public bool SupportsAppend => false;
    public bool SupportsNonEmptyDirectoryDelete => false;
    public StringComparer FileSystemEntryComparer => StringComparer.Ordinal;
    public IUnixDirectoryEntry Root { get; } = new AtFileDirectoryEntry("/", true);
    
    public async Task<IReadOnlyList<IUnixFileSystemEntry>> GetEntriesAsync(IUnixDirectoryEntry directoryEntry, CancellationToken cancellationToken)
    {
        // TODO paginate
        var uploads = (await atProtocol.ListUploadAsync(did, limit: 100, cancellationToken: cancellationToken))
            .HandleResult();

        return uploads?.Records?
            .Where(record => IsMemberOfDirectory((directoryEntry as AtFileDirectoryEntry)!, record))
            .Select(CreateFileSystemEntry)
            .ToArray() ?? [];
    }

    private bool IsMemberOfDirectory(AtFileDirectoryEntry directoryEntry, Record record)
    {
        return record.Uri!.Rkey.StartsWith(directoryEntry.Rkey) && record.Uri!.Rkey != directoryEntry.Rkey;
    }

    private static AtFileFileSystemEntry CreateFileSystemEntry(Record e)
    {
        var upload = (e.Value as Upload)!;
        var rkey = e.Uri!.Rkey;

        if (upload.Meta?.Reason == IsDirectoryKey)
        {
            return new AtFileDirectoryEntry(upload.File?.Name ?? rkey, false, rkey);
        }
        else
        {
            return new AtFileFileEntry(upload.File?.Name ?? rkey, upload.File?.Size ?? upload.Blob?.Size ?? 0, rkey);
        }
    }

    public async Task<IUnixFileSystemEntry?> GetEntryByNameAsync(IUnixDirectoryEntry directoryEntry, string name, CancellationToken cancellationToken)
    {
        var path = S3Path.Combine((directoryEntry as AtFileDirectoryEntry)!.RealName, name);
        var rkey = Rkeys.FromFilePath(path);

        return (await (await atProtocol.GetUploadAsync(did, rkey, cancellationToken: cancellationToken))
            .MatchAsync<Result<GetRecordOutput?>>(
                output => Task.FromResult<Result<GetRecordOutput?>>(output),
                async error => await atProtocol.GetUploadAsync(did, name, cancellationToken: cancellationToken)))
            .Match<IUnixFileSystemEntry?>(
                output =>
                {
                    var upload = (output?.Value as Upload)!;

                    return upload.Meta?.Reason == IsDirectoryKey
                        ? new AtFileDirectoryEntry(upload.File?.Name ?? rkey, false, rkey)
                        : new AtFileFileEntry(upload.File?.Name ?? rkey, upload.File?.Size ?? upload.Blob?.Size ?? 0, rkey);
                },
                error => null
            );
    }

    public Task<IUnixFileSystemEntry> MoveAsync(IUnixDirectoryEntry parent, IUnixFileSystemEntry source, IUnixDirectoryEntry target, string fileName,
        CancellationToken cancellationToken)
    {
        throw new NotSupportedException();
    }

    public async Task UnlinkAsync(IUnixFileSystemEntry entry, CancellationToken cancellationToken)
    {
        (await atProtocol.DeleteUploadAsync(
            repo: did,
            rkey: (entry as AtFileFileSystemEntry)!.Rkey,
            cancellationToken: cancellationToken
        )).HandleResult();

    }

    public async Task<IUnixDirectoryEntry> CreateDirectoryAsync(IUnixDirectoryEntry targetDirectory, string directoryName, CancellationToken cancellationToken)
    {
        var path = S3Path.Combine((targetDirectory as AtFileDirectoryEntry)!.RealName, directoryName);
        var rkey = Rkeys.FromFilePath(path);

        (await atProtocol.CreateUploadAsync(
            new Upload
            {
                File = new File
                {
                    Name = path,
                },
                Meta = new Unknown
                {
                    Reason = IsDirectoryKey
                },
            },
            rkey: rkey,
            cancellationToken: cancellationToken
        )).HandleResult();

        return new AtFileDirectoryEntry(path, false, rkey);
    }

    public async Task<Stream> OpenReadAsync(IUnixFileEntry fileEntry, long startPosition, CancellationToken cancellationToken)
    {
        var upload = (await atProtocol.GetUploadAsync(
            did,
            (fileEntry as AtFileFileEntry)!.Rkey,
            cancellationToken: cancellationToken
        )).HandleResult()?.Value as Upload;

        if (upload?.Blob?.Ref?.Link is not { } refLink)
        {
            throw new InvalidOperationException();
        }

        var blob = (await atProtocol.GetBlobAsync(did, refLink, cancellationToken: cancellationToken))
            .HandleResult();

        return new MemoryStream(blob!);
    }

    public Task<IBackgroundTransfer?> AppendAsync(IUnixFileEntry fileEntry, long? startPosition, Stream data, CancellationToken cancellationToken)
    {
        throw new NotSupportedException();
    }

    public async Task<IBackgroundTransfer?> CreateAsync(IUnixDirectoryEntry targetDirectory, string fileName, Stream data, CancellationToken cancellationToken)
    {
        var path = S3Path.Combine((targetDirectory as AtFileDirectoryEntry)!.RealName, fileName);

        using var content = new StreamContent(data);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream"); // TODO

        var result = (await atProtocol.UploadBlobAsync(content, cancellationToken: cancellationToken))
            .HandleResult()!;
        
        (await atProtocol.CreateUploadAsync(
            new Upload
            {
                Blob = result.Blob,
                File = new File
                {
                    Name = path,
                    Size = data.CanSeek ? data.Length : null,
                },
            },
            rkey: Rkeys.FromFilePath(path),
            cancellationToken: cancellationToken
        )).HandleResult();

        return null;
    }

    public async Task<IBackgroundTransfer?> ReplaceAsync(IUnixFileEntry fileEntry, Stream data, CancellationToken cancellationToken)
    {
        using var content = new StreamContent(data);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream"); // TODO

        var result = (await atProtocol.UploadBlobAsync(content, cancellationToken: cancellationToken))
            .HandleResult()!;
        
        (await atProtocol.CreateUploadAsync(
            new Upload
            {
                Blob = result.Blob,
                File = new File
                {
                    Name = (fileEntry as AtFileFileEntry)!.RealName,
                    Size = data.CanSeek ? data.Length : null,
                },
            },
            rkey: (fileEntry as AtFileFileEntry)!.Rkey,
            cancellationToken: cancellationToken
        )).HandleResult();

        return null;
    }

    public Task<IUnixFileSystemEntry> SetMacTimeAsync(IUnixFileSystemEntry entry, DateTimeOffset? modify, DateTimeOffset? access, DateTimeOffset? create,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(entry);
    }
}
