using System;
using System.IO;

namespace Algoverse.DataBase
{
    public unsafe class BinaryReaderFast
    {
        Stream stream;
        byte[] buffer;

        public BinaryReaderFast(Stream stream)
        {
            this.stream = stream;

            buffer = new byte[16];
        }

        public void SetStream(Stream stream)
        {
            this.stream = stream;
        }

        public bool ReadBoolean()
        {
            var val = stream.ReadByte();

            return val != 0;
        }

        public byte ReadByte()
        {
            var val = stream.ReadByte();

            return (byte)val;
        }

        public sbyte ReadSByte()
        {
            var val = stream.ReadByte();

            return (sbyte)val;
        }

        public short ReadInt16()
        {
            stream.Read(buffer, 0, 2);

            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (short*)ptr;

                return *tmp;
            }
        }

        public ushort ReadUInt16()
        {
            stream.Read(buffer, 0, 2);

            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (ushort*)ptr;

                return *tmp;
            }
        }

        public char ReadChar()
        {
            stream.Read(buffer, 0, 2);

            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (char*)ptr;

                return *tmp;
            }
        }

        public int ReadInt32()
        {
            stream.Read(buffer, 0, 4);

            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (int*)ptr;

                return *tmp;
            }
        }

        public uint ReadUInt32()
        {
            stream.Read(buffer, 0, 4);

            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (uint*)ptr;

                return *tmp;
            }
        }

        public decimal ReadDecimal()
        {
            stream.Read(buffer, 0, 16);

            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (decimal*)ptr;

                return *tmp;
            }
        }

        public float ReadSingle()
        {
            stream.Read(buffer, 0, 4);

            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (float*)ptr;

                return *tmp;
            }
        }

        public long ReadInt64()
        {
            stream.Read(buffer, 0, 8);

            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (long*)ptr;

                return *tmp;
            }
        }

        public ulong ReadUInt64()
        {
            stream.Read(buffer, 0, 8);

            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (ulong*)ptr;

                return *tmp;
            }
        }

        public double ReadDouble()
        {
            stream.Read(buffer, 0, 8);

            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (double*)ptr;

                return *tmp;
            }
        }

        public string ReadString()
        {
            int len;

            stream.Read(buffer, 0, 4);

            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (int*)ptr;

                len = *tmp * 2;
            }

            if (buffer.Length < len)
            {
                buffer = new byte[len];
            }

            stream.Read(buffer, 0, len);

            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (char*)ptr;

                return new string(tmp, 0, len/2);
            }
        }

        public DateTime ReadDateTime()
        {
            var ticks = ReadInt64();

            var dt = new DateTime(ticks);

            return dt;
        }

        public byte[] Read(int len)
        {
            var tmp = new byte[len];

            stream.Read(tmp, 0, len);

            return tmp;
        }
    }
}
