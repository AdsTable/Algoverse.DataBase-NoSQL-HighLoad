using System;
using System.IO.MemoryMappedFiles;

namespace Algoverse.DataBase
{
    public class ReaderCRC32 : PageReader
    {
        readonly CRC32 crc32;

        public ReaderCRC32(int page, int count, PageFile file, ArrayStorage storage) : base(page, count, file, storage)
        {
            this.crc32 = new CRC32();
        }

        public override bool ReadBoolean()
        {
            var val = base.ReadBoolean();

            crc32.Update(val);

            return val;
        }

        public override byte ReadUInt8()
        {
            var val = base.ReadUInt8();

            crc32.Update(val);

            return val;
        }

        public override sbyte ReadInt8()
        {
            var val = base.ReadInt8();

            crc32.Update(val);

            return val;
        }

        public override short ReadInt16()
        {
            var val = base.ReadInt16();

            crc32.Update(val);

            return val;
        }

        public override ushort ReadUInt16()
        {
            var val = base.ReadUInt16();

            crc32.Update(val);

            return val;
        }

        public override char ReadChar()
        {
            var val = base.ReadChar();

            crc32.Update(val);

            return val;
        }

        public override int ReadInt32()
        {
            var val = base.ReadInt32();

            crc32.Update(val);

            return val;
        }

        public override uint ReadUInt32()
        {
            var val = base.ReadUInt32();

            crc32.Update(val);

            return val;
        }

        public override float ReadSingle()
        {
            var val = base.ReadSingle();

            crc32.Update(val);

            return val;
        }

        public override long ReadInt64()
        {
            var val = base.ReadInt64();

            crc32.Update(val);

            return val;
        }

        public override ulong ReadUInt64()
        {
            var val = base.ReadUInt64();

            crc32.Update(val);

            return val;
        }

        public override double ReadDouble()
        {
            var val = base.ReadDouble();

            crc32.Update(val);

            return val;
        }

        public override string ReadString()
        {
            long offset;
            var val = ReadString(out offset);

            crc32.Update(offset);
            crc32.Update(val);

            return val;
        }

        public override int[] ReadInt32Array()
        {
            long offset;
            var val = ReadInt32Array(out offset);

            crc32.Update(offset);
            crc32.Update(val, 0, val.Length);

            return val;
        }

        public override uint[] ReadUInt32Array()
        {
            long offset;
            var val = ReadUInt32Array(out offset);

            crc32.Update(offset);
            crc32.Update(val, 0, val.Length);

            return val;
        }

        public override byte[] ReadBytes()
        {
            long offset;
            var val = ReadBytes(out offset);

            crc32.Update(offset);
            crc32.Update(val, 0, val.Length);

            return val;
        }

        public override DateTime ReadDateTime()
        {
            var ticks = base.ReadInt64();

            crc32.Update(ticks);

            return new DateTime(ticks);
        }

        public override TimeSpan ReadTimeSpan()
        {
            var ticks = base.ReadInt64();

            crc32.Update(ticks);

            return new TimeSpan(ticks);
        }

        public void ResetCRC()
        {
            crc32.Reset();
        }

        public bool ReadAndCheckCRC()
        {
            var val = base.ReadInt32();

            return val == crc32.Value;
        }

        public bool ReadCheckResetCRC()
        {
            var val = base.ReadInt32() == crc32.Value;

            crc32.Reset();

            return val;
        }

        public decimal ReadDecimal()
        {
            throw new NotImplementedException();
        }
    }
}
