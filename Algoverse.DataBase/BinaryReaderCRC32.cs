using System;
using System.IO;

namespace Algoverse.DataBase
{
    public class BinaryReaderCRC32 : IReader
    {
        readonly BinaryReader br;
        readonly CRC32 crc32;

        public BinaryReaderCRC32(BinaryReader br)
        {
            this.br = br;
            
            crc32 = new CRC32();
        }

        public BinaryReaderCRC32(Stream st) : this(new BinaryReader(st))
        {
        }

        public Stream Stream 
        {
            get
            {
                return br.BaseStream;
            }
        }

        public int CRC32
        {
            get
            {
                return crc32.Value;
            }
        }

        public int Position
        {
            get
            {
                return (int)br.BaseStream.Position;
            }
            set
            {
                br.BaseStream.Position = value;
            }
        }

        public bool ReadBoolean()
        {
            var val = br.ReadBoolean();

            crc32.Update(val);

            return val;
        }

        public byte ReadUInt8()
        {
            var val = br.ReadByte();

            crc32.Update(val);

            return val;
        }

        public sbyte ReadInt8()
        {
            var val = br.ReadSByte();

            crc32.Update(val);

            return val;
        }

        public short ReadInt16()
        {
            var val = br.ReadInt16();

            crc32.Update(val);

            return val;
        }

        public ushort ReadUInt16()
        {
            var val = br.ReadUInt16();

            crc32.Update(val);

            return val;
        }

        public char ReadChar()
        {
            var val = br.ReadChar();

            crc32.Update(val);

            return val;
        }

        public int ReadInt32()
        {
            var val = br.ReadInt32();

            crc32.Update(val);

            return val;
        }

        public uint ReadUInt32()
        {
            var val = br.ReadUInt32();

            crc32.Update(val);

            return val;
        }

        public float ReadSingle()
        {
            var val = br.ReadSingle();

            crc32.Update(val);

            return val;
        }

        public long ReadInt64()
        {
            var val = br.ReadInt64();

            crc32.Update(val);

            return val;
        }

        public ulong ReadUInt64()
        {
            var val = br.ReadUInt64();

            crc32.Update(val);

            return val;
        }

        public double ReadDouble()
        {
            var val = br.ReadDouble();

            crc32.Update(val);

            return val;
        }

        public string ReadString()
        {
            var val = br.ReadString();

            crc32.Update(val);

            return val;
        }

        public int[] ReadInt32Array()
        {
            var count = br.ReadInt32();
            var arr = new int[count];

            crc32.Update(count);

            for (int i = 0; i < count; ++i)
            {
                arr[i] = br.ReadInt32();
                crc32.Update(arr[i]);
            }

            return arr;
        }

        public uint[] ReadUInt32Array()
        {
            var count = br.ReadInt32();
            var arr = new uint[count];

            crc32.Update(count);

            for (int i = 0; i < count; ++i)
            {
                arr[i] = br.ReadUInt32();
                crc32.Update(arr[i]);
            }

            return arr;
        }

        public byte[] ReadBytes()
        {
            var count = br.ReadInt32();
            var arr = br.ReadBytes(count);

            crc32.Update(count);
            crc32.Update(arr, 0, arr.Length);

            return arr;
        }

        public DateTime ReadDateTime()
        {
            var ticks = br.ReadInt64();

            crc32.Update(ticks);

            return new DateTime(ticks);
        }

        public TimeSpan ReadTimeSpan()
        {
            var ticks = br.ReadInt64();

            crc32.Update(ticks);

            return new TimeSpan(ticks);
        }

        public void ResetCRC()
        {
            crc32.Reset();
        }

        public bool ReadAndCheckCRC()
        {
            var val = br.ReadInt32();

            return val == crc32.Value;
        }

        public bool ReadCheckResetCRC()
        {
            var val = br.ReadInt32() == crc32.Value;

            crc32.Reset();

            return val;
        }
    }
}
