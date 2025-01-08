using FishyFlip.Models;
using FishyFlip.Tools;

namespace AtFileFtp
{
    public static class Extensions
    {
        public static void ThrowIfError(this ATError? error)
        {
            if (error != null)
                throw new ATNetworkErrorException(error);
        }
    }
}