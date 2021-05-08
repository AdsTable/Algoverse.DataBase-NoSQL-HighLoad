using System;

namespace Algoverse.DataBase
{
    public interface IWriter
    {
        void Write(bool val);
        void Write(byte val);
        void Write(sbyte val);
        void Write(short val);
        void Write(ushort val);
        void Write(char val);
        void Write(int val);
        void Write(uint val);
        void Write(float val);
        void Write(long val);
        void Write(ulong val);
        void Write(double val);
        void Write(string val);
        void Write(int[] val);
        void Write(uint[] val);
        void Write(byte[] val);
        void Write(DateTime val);
        void Write(TimeSpan val);
    }
}