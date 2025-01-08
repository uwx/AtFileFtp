using System.Text.RegularExpressions;

namespace AtFileFtp;

public static class Rkeys
{
    public static string FromFilePath(string filepath)
    {
        if (filepath == "") return "";

        if (filepath.Contains(":")) throw new InvalidOperationException("`:` character not allowed in file path!");

        filepath = filepath.Replace('\\', '/');

        if (filepath.StartsWith("./")) {
            filepath = filepath.Substring(2);
        }

        if (filepath.StartsWith("/")) {
            filepath = filepath.Substring(1);
        }

        if (filepath.Contains("../") || filepath.Contains("/..")) {
            throw new InvalidOperationException("Backwards directory navigation not supported in rkey");
        }

        filepath = Regex.Replace(filepath, "[^A-Za-z0-9.\\-]", match => {
            // regex excludes : and _ because we use those as control characters
            // regex excludes ~ because using it gives us internal server InvalidOperationException
            if (match.ValueSpan[0] == '\\' || match.ValueSpan[0] == '/') {
                return ":";
            }
            return $"_{(int)match.ValueSpan[0]:x4}";
        });

        filepath = filepath.ToLowerInvariant();

        if (filepath.Length > 512) throw new InvalidOperationException("File path too long!");

        return filepath;
    }

    public static string ToFilePath(string rkey)
    {
        rkey = rkey
            .Replace(':', '/');
            
        rkey = Regex.Replace(
            rkey,
            "_([0-9a-fA-F]{4})",
            match => new string([(char)Convert.ToUInt16(match.Groups[1].Value, 16)])
        );

        return rkey;
    }

    public static string GetFileName(string rkey)
    {
        return rkey.LastIndexOf(':') is var idx && idx > -1 && idx < rkey.Length - 1 ? rkey[(idx + 1)..] : rkey;
    }
    
    /// <summary>
    /// Combine two paths.
    /// </summary>
    /// <param name="first">The first part of the resulting path.</param>
    /// <param name="second">The second part of the resulting path.</param>
    /// <returns>The combination of <paramref name="first"/> and <paramref name="second"/> with a <c>/</c> in between.</returns>
    public static string Combine(string? first, string? second)
    {
        if (string.IsNullOrEmpty(first))
        {
            return second ?? string.Empty;
        }

        if (string.IsNullOrEmpty(second))
        {
            return first;
        }

        return string.Join(':', first.TrimEnd(':'), second);
    }
}
