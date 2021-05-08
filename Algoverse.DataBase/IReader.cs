using System;

namespace Algoverse.DataBase
{
    public interface IReader
    {
        int Position { get; set; }
        bool ReadBoolean();
        byte ReadUInt8();
        sbyte ReadInt8();
        short ReadInt16();
        ushort ReadUInt16();
        char ReadChar();
        int ReadInt32();
        uint ReadUInt32();
        float ReadSingle();
        long ReadInt64();
        ulong ReadUInt64();
        double ReadDouble();
        string ReadString();
        int[] ReadInt32Array();
        uint[] ReadUInt32Array();
        byte[] ReadBytes();
        DateTime ReadDateTime();
        TimeSpan ReadTimeSpan();
    }
}