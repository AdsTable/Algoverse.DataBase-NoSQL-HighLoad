using System;

namespace Algoverse.DataBase
{
    public class WriterCRC32 : PageWriter
    {
        readonly CRC32 crc32;

        internal WriterCRC32( int page, int count,PageFile file, ArrayStorage ars) : base( page, count, file, ars)
        {
            this.crc32 = new CRC32();
        }

        public int CRC32
        {
            get
            {
                return crc32.Value;
            }
        }

        public override void Write(bool val)
        {
            crc32.Update(val);
            base.Write(val);
        }

        public override void Write(byte val)
        {
            crc32.Update(val);
            base.Write(val);
        }

        public override void Write(sbyte val)
        {
            crc32.Update(val);
            base.Write(val);
        }

        public override void Write(short val)
        {
            crc32.Update(val);
            base.Write(val);
        }

        public override void Write(ushort val)
        {
            crc32.Update(val);
            base.Write(val);
        }

        public override void Write(char val)
        {
            crc32.Update(val);
            base.Write(val);
        }

        public override void Write(int val)
        {
            crc32.Update(val);
            base.Write(val);
        }

        public override void Write(uint val)
        {
            crc32.Update(val);
            base.Write(val);
        }

        public override void Write(float val)
        {
            crc32.Update(val);
            base.Write(val);
        }

        public override void Write(long val)
        {
            crc32.Update(val);
            base.Write(val);
        }

        public override void Write(ulong val)
        {
            crc32.Update(val);
            base.Write(val);
        }

        public override void Write(double val)
        {
            crc32.Update(val);
            base.Write(val);
        }

        public override void Write(string val)
        {
            long offset;

            Write(val, out offset);

            crc32.Update(offset);
            crc32.Update(val);
        }

        public override void Write(int[] val)
        {
            long offset;

            Write(val, out offset);

            crc32.Update(offset);
            crc32.Update(val, 0, val.Length);
        }

        public override void Write(uint[] val)
        {
            long offset;

            Write(val, out offset);

            crc32.Update(offset);
            crc32.Update(val, 0, val.Length);
        }

        public override void Write(byte[] val)
        {
            long offset;

            Write(val, out offset);

            crc32.Update(offset);
            crc32.Update(val, 0, val.Length);
        }

        public override void Write(DateTime val)
        {
            crc32.Update(val.Ticks);
            base.Write(val.Ticks);
        }

        public override void Write(TimeSpan val)
        {
            crc32.Update(val.Ticks);
            base.Write(val.Ticks);
        }

        public void WriteAndResetCRC()
        {
            base.Write(crc32.Value);
            crc32.Reset();
        }

        public void ResetCRC()
        {
            crc32.Reset();
        }

        public void WriteCRC()
        {
            base.Write(crc32.Value);
        }

        public void Write(decimal val)
        {
            throw new NotImplementedException();
        }
    }
}
