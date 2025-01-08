using System.Text.RegularExpressions;

namespace AtFileWebDav;

public static class Rkeys
{
    public static string FromFilePath(string filepath)
    {
        if (filepath == "") throw new InvalidOperationException("File path is empty!");

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
            return $"_{(int)match.ValueSpan[0]:xxxx}";
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
}
