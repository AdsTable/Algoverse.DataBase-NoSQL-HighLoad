using System;

namespace Algoverse.DataBase
{
    public unsafe class CRC32
    {
        #region ' Static '

        private const uint kCrcPoly = 0xEDB88320;
        private const uint kInitial = 0xFFFFFFFF;
        private static readonly uint[] Table;
        private const uint CRC_NUM_TABLES = 8;

        static CRC32()
        {
            unchecked
            {
                Table = new uint[256 * CRC_NUM_TABLES];
                uint i;
                for (i = 0; i < 256; i++)
                {
                    uint r = i;
                    for (int j = 0; j < 8; j++)
                        r = (r >> 1) ^ (kCrcPoly & ~((r & 1) - 1));
                    Table[i] = r;
                }
                for (; i < 256 * CRC_NUM_TABLES; i++)
                {
                    uint r = Table[i - 256];
                    Table[i] = Table[r & 0xFF] ^ (r >> 8);
                }
            }
        }

        public static int Compute(byte[] data, int offset, int size)
        {
            var crc = new CRC32();
            crc.Update(data, offset, size);
            return crc.Value;
        }

        public static int Compute(byte[] data)
        {
            return Compute(data, 0, data.Length);
        }

        public static int Compute(ArraySegment<byte> block)
        {
            return Compute(block.Array, block.Offset, block.Count);
        }

        #endregion

        uint value;

        public CRC32()
        {
            value = kInitial;
        }

        /// <summary>
        /// Reset CRC
        /// </summary>
        public void Reset()
        {
            value = kInitial;
        }

        public int Value
        {
            get { return (int)~value; }
        }

        public void Update(byte val)
        {
            value = (value >> 8) ^ Table[(byte)value ^ val];
        }

        public void Update(sbyte val)
        {
            value = (value >> 8) ^ Table[(byte)value ^ val];
        }

        public void Update(bool val)
        {
            byte x = val ? (byte)1 : (byte)0;

            value = (value >> 8) ^ Table[(byte)value ^ x];
        }

        public void Update(short val)
        {
            var b = (byte*)&val;

            value = (value >> 8) ^ Table[(byte)value ^ b[0]];
            value = (value >> 8) ^ Table[(byte)value ^ b[1]];
        }

        public void Update(ushort val)
        {
            var b = (byte*)&val;

            value = (value >> 8) ^ Table[(byte)value ^ b[0]];
            value = (value >> 8) ^ Table[(byte)value ^ b[1]];
        }

        public void Update(char val)
        {
            var b = (byte*)&val;

            value = (value >> 8) ^ Table[(byte)value ^ b[0]];
            value = (value >> 8) ^ Table[(byte)value ^ b[1]];
        }

        public void Update(int val)
        {
            var b = (byte*)&val;

            value = (value >> 8) ^ Table[(byte)value ^ b[0]];
            value = (value >> 8) ^ Table[(byte)value ^ b[1]];
            value = (value >> 8) ^ Table[(byte)value ^ b[2]];
            value = (value >> 8) ^ Table[(byte)value ^ b[3]];
        }

        public void Update(uint val)
        {
            var b = (byte*)&val;

            value = (value >> 8) ^ Table[(byte)value ^ b[0]];
            value = (value >> 8) ^ Table[(byte)value ^ b[1]];
            value = (value >> 8) ^ Table[(byte)value ^ b[2]];
            value = (value >> 8) ^ Table[(byte)value ^ b[3]];         
        }

        public void Update(float val)
        {
            var b = (byte*)&val;

            value = (value >> 8) ^ Table[(byte)value ^ b[0]];
            value = (value >> 8) ^ Table[(byte)value ^ b[1]];
            value = (value >> 8) ^ Table[(byte)value ^ b[2]];
            value = (value >> 8) ^ Table[(byte)value ^ b[3]];
        }

        public void Update(long val)
        {
            var b = (byte*)&val;

            value = (value >> 8) ^ Table[(byte)value ^ b[0]];
            value = (value >> 8) ^ Table[(byte)value ^ b[1]];
            value = (value >> 8) ^ Table[(byte)value ^ b[2]];
            value = (value >> 8) ^ Table[(byte)value ^ b[3]];
            value = (value >> 8) ^ Table[(byte)value ^ b[4]];
            value = (value >> 8) ^ Table[(byte)value ^ b[5]];
            value = (value >> 8) ^ Table[(byte)value ^ b[6]];
            value = (value >> 8) ^ Table[(byte)value ^ b[7]];
        }

        public void Update(ulong val)
        {
            var b = (byte*)&val;

            value = (value >> 8) ^ Table[(byte)value ^ b[0]];
            value = (value >> 8) ^ Table[(byte)value ^ b[1]];
            value = (value >> 8) ^ Table[(byte)value ^ b[2]];
            value = (value >> 8) ^ Table[(byte)value ^ b[3]];
            value = (value >> 8) ^ Table[(byte)value ^ b[4]];
            value = (value >> 8) ^ Table[(byte)value ^ b[5]];
            value = (value >> 8) ^ Table[(byte)value ^ b[6]];
            value = (value >> 8) ^ Table[(byte)value ^ b[7]];
        }

        public void Update(double val)
        {
            var b = (byte*)&val;

            value = (value >> 8) ^ Table[(byte)value ^ b[0]];
            value = (value >> 8) ^ Table[(byte)value ^ b[1]];
            value = (value >> 8) ^ Table[(byte)value ^ b[2]];
            value = (value >> 8) ^ Table[(byte)value ^ b[3]];
            value = (value >> 8) ^ Table[(byte)value ^ b[4]];
            value = (value >> 8) ^ Table[(byte)value ^ b[5]];
            value = (value >> 8) ^ Table[(byte)value ^ b[6]];
            value = (value >> 8) ^ Table[(byte)value ^ b[7]];
        }

        public void Update(decimal val)
        {
            var b = (byte*)&val;

            value = (value >> 8) ^ Table[(byte)value ^ b[0]];
            value = (value >> 8) ^ Table[(byte)value ^ b[1]];
            value = (value >> 8) ^ Table[(byte)value ^ b[2]];
            value = (value >> 8) ^ Table[(byte)value ^ b[3]];
            value = (value >> 8) ^ Table[(byte)value ^ b[4]];
            value = (value >> 8) ^ Table[(byte)value ^ b[5]];
            value = (value >> 8) ^ Table[(byte)value ^ b[6]];
            value = (value >> 8) ^ Table[(byte)value ^ b[7]];
            value = (value >> 8) ^ Table[(byte)value ^ b[8]];
            value = (value >> 8) ^ Table[(byte)value ^ b[9]];
            value = (value >> 8) ^ Table[(byte)value ^ b[10]];
            value = (value >> 8) ^ Table[(byte)value ^ b[11]];
            value = (value >> 8) ^ Table[(byte)value ^ b[12]];
            value = (value >> 8) ^ Table[(byte)value ^ b[13]];
            value = (value >> 8) ^ Table[(byte)value ^ b[14]];
            value = (value >> 8) ^ Table[(byte)value ^ b[15]];
        }

        public void Update(string val)
        {
            for (int i = 0; i < val.Length; ++i)
            {
                var ch = val[i];
                var b = (byte*)&ch;

                value = (value >> 8) ^ Table[(byte)value ^ b[0]];
                value = (value >> 8) ^ Table[(byte)value ^ b[1]];
            }
        }

        public void Update(byte[] data, int offset, int count)
        {
            //new ArraySegment<byte>(data, offset, count);     // check arguments
            if (count == 0) return;

            var table = CRC32.Table;        // important for performance!

            uint crc = value;

            for (; (offset & 7) != 0 && count != 0; count--)
            {
                crc = (crc >> 8) ^ table[(byte)crc ^ data[offset++]];
            }

            if (count >= 8)
            {
                /*
                 * Idea from 7-zip project sources (http://7-zip.org/sdk.html)
                 */

                int to = (count - 8) & ~7;
                count -= to;
                to += offset;

                while (offset != to)
                {
                    crc ^= (uint)(data[offset] + (data[offset + 1] << 8) + (data[offset + 2] << 16) + (data[offset + 3] << 24));

                    uint high = (uint)(data[offset + 4] + (data[offset + 5] << 8) + (data[offset + 6] << 16) + (data[offset + 7] << 24));
                    offset += 8;

                    crc = table[(byte)crc + 0x700]
                        ^ table[(byte)(crc >>= 8) + 0x600]
                        ^ table[(byte)(crc >>= 8) + 0x500]
                        ^ table[/*(byte)*/(crc >> 8) + 0x400]
                        ^ table[(byte)(high) + 0x300]
                        ^ table[(byte)(high >>= 8) + 0x200]
                        ^ table[(byte)(high >>= 8) + 0x100]
                        ^ table[/*(byte)*/(high >> 8) + 0x000];
                }
            }

            while (count-- != 0)
            {
                crc = (crc >> 8) ^ table[(byte)crc ^ data[offset++]];
            }

            value = crc;
        }

        public void Update(sbyte[] data, int offset, int count)
        {
            //new ArraySegment<sbyte>(data, offset, count);     // check arguments
            if (count == 0) return;

            var table = CRC32.Table;        // important for performance!

            uint crc = value;

            for (; (offset & 7) != 0 && count != 0; count--)
            {
                crc = (crc >> 8) ^ table[(byte)crc ^ data[offset++]];
            }

            if (count >= 8)
            {
                /*
                 * Idea from 7-zip project sources (http://7-zip.org/sdk.html)
                 */

                int to = (count - 8) & ~7;
                count -= to;
                to += offset;

                while (offset != to)
                {
                    crc ^= (uint)(data[offset] + (data[offset + 1] << 8) + (data[offset + 2] << 16) + (data[offset + 3] << 24));

                    uint high = (uint)(data[offset + 4] + (data[offset + 5] << 8) + (data[offset + 6] << 16) + (data[offset + 7] << 24));
                    offset += 8;

                    crc = table[(byte)crc + 0x700]
                        ^ table[(byte)(crc >>= 8) + 0x600]
                        ^ table[(byte)(crc >>= 8) + 0x500]
                        ^ table[/*(byte)*/(crc >> 8) + 0x400]
                        ^ table[(byte)(high) + 0x300]
                        ^ table[(byte)(high >>= 8) + 0x200]
                        ^ table[(byte)(high >>= 8) + 0x100]
                        ^ table[/*(byte)*/(high >> 8) + 0x000];
                }
            }

            while (count-- != 0)
            {
                crc = (crc >> 8) ^ table[(byte)crc ^ data[offset++]];
            }

            value = crc;
        }

        public void Update(int[] data, int offset, int length)
        {
            for (int i = offset; i < length; ++i)
            {
                Update(data[i]);
            }
        }

        public void Update(uint[] data, int offset, int length)
        {
            for (int i = offset; i < length; ++i)
            {
                Update(data[i]);
            }            
        }

        public void Update(long[] data, int offset, int length)
        {
            for (int i = offset; i < length; ++i)
            {
                Update(data[i]);
            }
        }

        public void Update(ulong[] data, int offset, int length)
        {
            for (int i = offset; i < length; ++i)
            {
                Update(data[i]);
            }
        }

        public void Update(double[] data, int offset, int length)
        {
            for (int i = offset; i < length; ++i)
            {
                Update(data[i]);
            }
        }

        public void Update(string[] data, int offset, int length)
        {
            for (int i = offset; i < length; ++i)
            {
                Update(data[i]);
            }
        }

        public void Update(char[] data, int offset, int length)
        {
            for (int i = offset; i < length; ++i)
            {
                Update(data[i]);
            }
        }

        public void Update(short[] data, int offset, int length)
        {
            for (int i = offset; i < length; ++i)
            {
                Update(data[i]);
            }
        }

        public void Update(ushort[] data, int offset, int length)
        {
            for (int i = offset; i < length; ++i)
            {
                Update(data[i]);
            }
        }

        public void Update(float[] data, int offset, int length)
        {
            for (int i = offset; i < length; ++i)
            {
                Update(data[i]);
            }
        }
    }
}
