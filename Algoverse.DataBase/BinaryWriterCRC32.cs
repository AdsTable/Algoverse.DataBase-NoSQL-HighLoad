using System;
using System.IO;

namespace Algoverse.DataBase
{
    public class BinaryWriterCRC32 : IWriter
    {
        readonly BinaryWriter bw;
        readonly CRC32 crc32;

        public BinaryWriterCRC32(BinaryWriter bw)
        {
            this.bw = bw;
            this.crc32 = new CRC32();
        }

        public BinaryWriterCRC32(Stream st)
        {
            this.bw = new BinaryWriter(st);
            this.crc32 = new CRC32();
        }

        public int CRC32
        {
            get
            {
                return crc32.Value;
            }
        }

        public void Write(bool val)
        {
            crc32.Update(val);
            bw.Write(val);
        }

        public void Write(byte val)
        {
            crc32.Update(val);
            bw.Write(val);
        }

        public void Write(sbyte val)
        {
            crc32.Update(val);
            bw.Write(val);
        }

        public void Write(short val)
        {
            crc32.Update(val);
            bw.Write(val);
        }

        public void Write(ushort val)
        {
            crc32.Update(val);
            bw.Write(val);
        }

        public void Write(char val)
        {
            crc32.Update(val);
            bw.Write(val);
        }

        public void Write(int val)
        {
            crc32.Update(val);
            bw.Write(val);
        }

        public void Write(uint val)
        {
            crc32.Update(val);
            bw.Write(val);
        }

        public void Write(float val)
        {
            crc32.Update(val);
            bw.Write(val);
        }

        public void Write(long val)
        {
            crc32.Update(val);
            bw.Write(val);
        }

        public void Write(ulong val)
        {
            crc32.Update(val);
            bw.Write(val);
        }

        public void Write(double val)
        {
            crc32.Update(val);
            bw.Write(val);
        }

        public void Write(string val)
        {
            crc32.Update(val);
            bw.Write(val);
        }

        public void Write(int[] val)
        {
            crc32.Update(val.Length);
            bw.Write(val.Length);

            for (int i = 0; i < val.Length; ++i)
            {
                crc32.Update(val[i]);
                bw.Write(val[i]);
            }
        }

        public void Write(uint[] val)
        {
            crc32.Update(val.Length);
            bw.Write(val.Length);

            for (int i = 0; i < val.Length; ++i)
            {
                crc32.Update(val[i]);
                bw.Write(val[i]);
            }
        }

        public void Write(byte[] val)
        {
            crc32.Update(val.Length);
            bw.Write(val.Length);

            crc32.Update(val, 0, val.Length);
            bw.Write(val);
        }

        public void Write(DateTime val)
        {
            crc32.Update(val.Ticks);
            bw.Write(val.Ticks);
        }

        public void Write(TimeSpan val)
        {
            crc32.Update(val.Ticks);
            bw.Write(val.Ticks);
        }

        public void WriteAndResetCRC()
        {
            bw.Write(crc32.Value);
            crc32.Reset();
        }

        public void ResetCRC()
        {
            crc32.Reset();
        }

        public void WriteCRC()
        {
            bw.Write(crc32.Value);
        }
    }
}
