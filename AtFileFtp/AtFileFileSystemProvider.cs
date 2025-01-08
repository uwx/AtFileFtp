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
    private readonly ATProtocol _atProtocol;

    public AtFileFileSystemProvider()
    {
        // Include a ILogger if you want additional logging from the base library.
        var debugLog = new DebugLoggerProvider();
        var atProtocolBuilder = new ATProtocolBuilder()
            .EnableAutoRenewSession(true)
            .WithLogger(debugLog.CreateLogger("FishyFlipDebug"));
        _atProtocol = atProtocolBuilder.Build();
    }

    public async Task<IUnixFileSystem> Create(IAccountInformation accountInformation)
    {
        var identity = (accountInformation.FtpUser.Identity as BlueskyIdentity)!;
        
        var session = (await _atProtocol.AuthenticateWithPasswordResultAsync(identity.Name, identity.Password)).HandleResult();
        return new AtFileFileSystem(_atProtocol, session!.Did);
    }
}

public class AtFileFileSystemEntry : IUnixFileSystemEntry
{
    public AtFileFileSystemEntry(string rkey, DateTimeOffset? modified = null, DateTimeOffset? created = null)
    {
        Rkey = rkey;
        Name = Rkeys.ToFilePath(Rkeys.GetFileName(Rkey));

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

public class AtFileDirectoryEntry(bool isRoot, string rkey, DateTimeOffset? modified = null, DateTimeOffset? created = null)
    : AtFileFileSystemEntry(rkey, modified, created), IUnixDirectoryEntry
{
    public bool IsRoot { get; } = isRoot;
    public bool IsDeletable => true;
}

public class AtFileFileEntry(long size, string rkey, DateTimeOffset? modified = null, DateTimeOffset? created = null)
    : AtFileFileSystemEntry(rkey, modified, created), IUnixFileEntry
{
    public long Size { get; } = size;
}

public class AtFileFileSystem(ATProtocol atProtocol, ATDid did) : IUnixFileSystem
{
    public const string IsDirectoryKey = "This file is a directory. Please do not replace it with data.";
    
    public bool SupportsAppend => false;
    public bool SupportsNonEmptyDirectoryDelete => false;
    public StringComparer FileSystemEntryComparer => StringComparer.Ordinal;
    public IUnixDirectoryEntry Root { get; } = new AtFileDirectoryEntry(true, "");

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
        var rkey = record.Uri!.Rkey;
        if (!rkey.StartsWith(directoryEntry.Rkey))
        {
            return false;
        }

        if (rkey == directoryEntry.Rkey)
        {
            return false;
        }

        var idx = rkey.IndexOf(':', directoryEntry.Rkey.Length + 1);
        if (idx > -1)
        {
            return false;
        }

        return true;
    }

    private static AtFileFileSystemEntry CreateFileSystemEntry(Record e)
    {
        var upload = (e.Value as Upload)!;
        var rkey = e.Uri!.Rkey;

        if (upload.Meta?.Reason == IsDirectoryKey)
        {
            return new AtFileDirectoryEntry(false, rkey);
        }
        else
        {
            return new AtFileFileEntry(upload.File?.Size ?? upload.Blob?.Size ?? 0, rkey);
        }
    }

    public async Task<IUnixFileSystemEntry?> GetEntryByNameAsync(IUnixDirectoryEntry directoryEntry, string name, CancellationToken cancellationToken)
    {
        var fullRkey = Rkeys.Combine(
            (directoryEntry as AtFileDirectoryEntry)!.Rkey,
            Rkeys.FromFilePath(name)
        );

        return (await (await atProtocol.GetUploadAsync(did, fullRkey, cancellationToken: cancellationToken))
            .MatchAsync<Result<GetRecordOutput?>>(
                output => Task.FromResult<Result<GetRecordOutput?>>(output),
                async error => await atProtocol.GetUploadAsync(did, name, cancellationToken: cancellationToken)))
            .Match<IUnixFileSystemEntry?>(
                output =>
                {
                    var upload = (output?.Value as Upload)!;

                    return upload.Meta?.Reason == IsDirectoryKey
                        ? new AtFileDirectoryEntry(false, fullRkey)
                        : new AtFileFileEntry(upload.File?.Size ?? upload.Blob?.Size ?? 0, fullRkey);
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
        var fullRkey = Rkeys.Combine(
            (targetDirectory as AtFileDirectoryEntry)!.Rkey,
            Rkeys.FromFilePath(directoryName)
        );

        (await atProtocol.CreateUploadAsync(
            new Upload
            {
                File = new File
                {
                    Name = Rkeys.ToFilePath(fullRkey),
                },
                Meta = new Unknown
                {
                    Reason = IsDirectoryKey
                },
            },
            rkey: fullRkey,
            cancellationToken: cancellationToken
        )).HandleResult();

        return new AtFileDirectoryEntry(false, fullRkey);
    }

    public async Task<Stream> OpenReadAsync(IUnixFileEntry fileEntry, long startPosition, CancellationToken cancellationToken)
    {
        var upload = (await atProtocol.GetUploadAsync(did, (fileEntry as AtFileFileEntry)!.Rkey, cancellationToken: cancellationToken))
            .Match<Result<Upload?>>(
                output => output?.Value as Upload,
                error => error
            )
            .HandleResult();

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
        var fullRkey = Rkeys.Combine(
            (targetDirectory as AtFileDirectoryEntry)!.Rkey,
            Rkeys.FromFilePath(fileName)
        );

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
                    Name = Rkeys.ToFilePath(fullRkey),
                    Size = data.CanSeek ? data.Length : null,
                },
            },
            rkey: fullRkey,
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
                    Name = Rkeys.ToFilePath((fileEntry as AtFileFileEntry)!.Rkey),
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
