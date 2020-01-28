using System.Runtime.InteropServices;

namespace Algoverse.DataBase
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct ArrayStorageItem
    {
        public long Position;
        public long Length;
    }
}