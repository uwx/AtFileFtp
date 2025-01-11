using System.Collections.Concurrent;
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
using Ipfs;
using Microsoft.Extensions.Logging.Debug;
using File = FishyFlip.Lexicon.Blue.Zio.Atfile.File;

namespace AtFileFtp;

public class AtFileFileSystemProvider : IFileSystemClassFactory
{
    private readonly ATProtocol _atProtocol;
    private readonly HttpClient _httpClient;

    public AtFileFileSystemProvider()
    {
        // Include a ILogger if you want additional logging from the base library.
        var debugLog = new DebugLoggerProvider();
        var atProtocolBuilder = new ATProtocolBuilder()
            .EnableAutoRenewSession(true)
            .WithLogger(debugLog.CreateLogger("FishyFlipDebug"));
        _atProtocol = atProtocolBuilder.Build();
        _httpClient = new HttpClient();
    }

    public async Task<IUnixFileSystem> Create(IAccountInformation accountInformation)
    {
        var identity = (accountInformation.FtpUser.Identity as BlueskyIdentity)!;
        
        var session = (await _atProtocol.AuthenticateWithPasswordResultAsync(identity.Name, identity.Password)).HandleResult();
        return new AtFileFileSystem(_httpClient, _atProtocol, session!.Did, session.DidDoc!.GetPDSEndpointUrl());
    }
}

public class AtFileFileSystemEntry : IUnixFileSystemEntry
{
    public AtFileFileSystemEntry(string rkey, DateTimeOffset? modified = null, DateTimeOffset? created = null, string? alternateRkey = null, Cid? blobLink = null, string? realName = null)
    {
        Rkey = rkey;
        Name = Rkeys.ToFilePath(Rkeys.GetFileName(Rkey));

        if (realName != null && realName != Name)
        {
            Name += $":{realName}";
        }

        LastWriteTime = modified;
        CreatedTime = created;
        AlternateRkey = alternateRkey;
        BlobLink = blobLink;
    }

    public string Owner => "owner";
    public string Group => "group";
    public string Name { get; }
    public IUnixPermissions Permissions { get; } = new DefaultPermissions();
    public DateTimeOffset? LastWriteTime { get; }
    public DateTimeOffset? CreatedTime { get; }
    public long NumberOfLinks => 1;

    public string Rkey { get; }
    public string? AlternateRkey { get; }
    public Cid? BlobLink { get; }

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

public class AtFileDirectoryEntry(bool isRoot, string rkey, DateTimeOffset? modified = null, DateTimeOffset? created = null, string? alternateRkey = null)
    : AtFileFileSystemEntry(rkey, modified, created, alternateRkey), IUnixDirectoryEntry
{
    public bool IsRoot { get; } = isRoot;
    public bool IsDeletable => true;
}

public class AtFileFileEntry(long size, string rkey, DateTimeOffset? modified = null, DateTimeOffset? created = null, string? alternateRkey = null, Cid? blobLink = null, string? realName = null)
    : AtFileFileSystemEntry(rkey, modified, created, alternateRkey, blobLink, realName), IUnixFileEntry
{
    public long Size { get; } = size;
}

public class AtFileFileSystem(HttpClient httpClient, ATProtocol atProtocol, ATDid did, Uri pdsEndpoint) : IUnixFileSystem
{
    public const string IsDirectoryKey = "This file is a directory. Please do not replace it with data.";
    
    public bool SupportsAppend => false;
    public bool SupportsNonEmptyDirectoryDelete => false;
    public StringComparer FileSystemEntryComparer => StringComparer.Ordinal;
    public IUnixDirectoryEntry Root { get; } = new AtFileDirectoryEntry(true, "");

    private readonly SemaphoreSlim _entryCacheLock = new(1, 1);
    private readonly OrderedDictionary<string, AtFileFileSystemEntry> _entryCache = new();

    public async Task<IReadOnlyList<IUnixFileSystemEntry>> GetEntriesAsync(IUnixDirectoryEntry directoryEntry, CancellationToken cancellationToken)
    {
        // TODO paginate
        await _entryCacheLock.WaitAsync(cancellationToken);

        try
        {
            // if (_entryCache.Count == 0)
            // {
            _entryCache.Clear();
            await PopulateEntryCache(cancellationToken);
            // }

            return _entryCache
                .Select(kvp => kvp.Value)
                .Where(entry => IsMemberOfDirectory((AtFileDirectoryEntry)directoryEntry, entry))
                .ToArray();
        }
        finally
        {
            _entryCacheLock.Release();
        }
    }

    private async Task PopulateEntryCache(CancellationToken cancellationToken)
    {
        var uploads = (await atProtocol.ListUploadAsync(did, limit: 100, cancellationToken: cancellationToken))
            .HandleResult();

        var records = uploads?.Records ?? [];

        foreach (var entry in records.Select(CreateFileSystemEntry))
        {
            _entryCache.Add(entry.Rkey, entry);
        }
    }

    private bool IsMemberOfDirectory(AtFileDirectoryEntry directoryEntry, AtFileFileSystemEntry record)
    {
        var rkey = record.Rkey;
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
            return new AtFileDirectoryEntry(
                false,
                rkey,
                alternateRkey: rkey
            );
        }
        else
        {
            return new AtFileFileEntry(
                upload.File?.Size ?? upload.Blob?.Size ?? 0,
                rkey,
                alternateRkey: rkey,
                blobLink: upload.Blob?.Ref?.Link,
                realName: upload.File?.Name
            );
        }
    }

    public async Task<IUnixFileSystemEntry?> GetEntryByNameAsync(IUnixDirectoryEntry directoryEntry, string name, CancellationToken cancellationToken)
    {
        // RealName separator
        if (name.Contains(':'))
        {
            name = name[..name.IndexOf(':')];
        }
        
        var fullRkey = Rkeys.Combine(
            (directoryEntry as AtFileDirectoryEntry)!.Rkey,
            Rkeys.FromFilePath(name)
        );

        await _entryCacheLock.WaitAsync(cancellationToken);
        try
        {
            if (_entryCache.Count == 0)
            {
                await PopulateEntryCache(cancellationToken);
            }

            if (_entryCache.TryGetValue(fullRkey, out var entry))
            {
                return entry;
            }
            else if (_entryCache.TryGetValue(name, out entry))
            {
                return entry;
            }
        }
        finally
        {
            _entryCacheLock.Release();
        }

        return null;
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

        await InvalidateCache(cancellationToken);
    }

    private async Task InvalidateCache(CancellationToken cancellationToken)
    {
        await _entryCacheLock.WaitAsync(cancellationToken);
        try
        {
            _entryCache.Clear();
        }
        finally
        {
            _entryCacheLock.Release();
        }
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
        
        await InvalidateCache(cancellationToken);

        return new AtFileDirectoryEntry(false, fullRkey);
    }

    public async Task<Stream> OpenReadAsync(IUnixFileEntry fileEntry, long startPosition, CancellationToken cancellationToken)
    {
        var entry = (AtFileFileEntry)fileEntry;

        if (entry.BlobLink is not { } blobCid)
        {
            throw new InvalidOperationException("No blob?");
        }
        
        var uri = new Uri(pdsEndpoint, $"/xrpc/com.atproto.sync.getBlob?did={Uri.EscapeDataString(did.ToString())}&cid={Uri.EscapeDataString(blobCid.Encode())}");
        
        var response = await httpClient.GetAsync(uri, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();

        return await response.Content.ReadAsStreamAsync(cancellationToken);

        // var blob = (await atProtocol.GetBlobAsync(did, refLink, cancellationToken: cancellationToken))
        //     .HandleResult();

        // return new MemoryStream(blob!);
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
            record: new Upload
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
        
        await InvalidateCache(cancellationToken);

        return null;
    }

    public async Task<IBackgroundTransfer?> ReplaceAsync(IUnixFileEntry fileEntry, Stream data, CancellationToken cancellationToken)
    {
        using var content = new StreamContent(data);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream"); // TODO

        var result = (await atProtocol.UploadBlobAsync(content, cancellationToken: cancellationToken))
            .HandleResult()!;

        (await atProtocol.PutUploadAsync(
            repo: did,
            record: new Upload
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
        
        await InvalidateCache(cancellationToken);

        return null;
    }

    public Task<IUnixFileSystemEntry> SetMacTimeAsync(IUnixFileSystemEntry entry, DateTimeOffset? modify, DateTimeOffset? access, DateTimeOffset? create,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(entry);
    }
}
