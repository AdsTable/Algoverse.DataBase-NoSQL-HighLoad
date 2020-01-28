using System;
using System.IO;

namespace Algoverse.DataBase
{
    public unsafe class BinaryWriterFast
    {
        Stream stream;
        byte[] buffer;

        public BinaryWriterFast(Stream stream)
        {
            this.stream = stream;

            buffer = new byte[16];
        }
        
        public void Write(bool val)
        {
            stream.WriteByte((byte)(val ? 1 : 0));
        }

        public void Write(byte val)
        {
            stream.WriteByte(val);
        }

        public void Write(sbyte val)
        {
            stream.WriteByte((byte)val);
        }

        public void Write(short val)
        {
            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (short*)ptr;

                *tmp = val;
            }

            stream.Write(buffer, 0, 2);
        }

        public void Write(ushort val)
        {
            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (ushort*)ptr;

                *tmp = val;
            }

            stream.Write(buffer, 0, 2);
        }

        public void Write(char val)
        {
            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (char*)ptr;

                *tmp = val;
            }

            stream.Write(buffer, 0, 2);
        }

        public void Write(int val)
        {
            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (int*) ptr;

                *tmp = val;
            }

            stream.Write(buffer, 0, 4);
        }

        public void Write(uint val)
        {
            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (uint*)ptr;

                *tmp = val;
            }

            stream.Write(buffer, 0, 4);
        }

        public void Write(float val)
        {
            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (float*)ptr;

                *tmp = val;
            }

            stream.Write(buffer, 0, 4);
        }

        public void Write(long val)
        {
            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (long*)ptr;

                *tmp = val;
            }

            stream.Write(buffer, 0, 8);
        }

        public void Write(ulong val)
        {
            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (ulong*)ptr;

                *tmp = val;
            }

            stream.Write(buffer, 0, 8);
        }

        public void Write(double val)
        {
            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (double*)ptr;

                *tmp = val;
            }

            stream.Write(buffer, 0, 8);
        }

        public void Write(decimal val)
        {
            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (decimal*)ptr;

                *tmp = val;
            }

            stream.Write(buffer, 0, 16);
        }

        public void Write(string val)
        {
            var l = 4 + val.Length * 2;

            if (buffer.Length < l)
            {
                buffer = new byte[l];
            }

            // Write length
            fixed (byte* ptr = &buffer[0])
            {
                var tmp = (int*)ptr;

                *tmp = val.Length;
            }

            fixed (byte* ptr = &buffer[0])
            fixed (char* str = val)
            {
                var c = (val.Length * 2) / sizeof(ulong);

                var tmp0    = (ulong*)(ptr + 4);
                var tmp1    = (ulong*)(str);

                for (var i = 0; i < c; ++i)
                {
                    tmp0[i] = tmp1[i];
                }
            }
            
            var p = val.Length % sizeof(ulong);

            for (var i = val.Length - p; i < val.Length; ++i)
            {
                fixed (byte* ptr = &buffer[0])
                {
                    var tmp = (char*)(ptr + i * 2 + 4);

                    *tmp = val[i];
                }
            }

            stream.Write(buffer, 0, l);
        }

        //public void Write(DateTime val)
        //{
        //    fixed (byte* ptr = &buffer[0])
        //    {
        //        var tmp = (long*)ptr;

        //        *tmp = val.Ticks;
        //    }

        //    stream.Write(buffer, 0, 8);
        //}

        //public void Write(TimeSpan val)
        //{
        //    fixed (byte* ptr = &buffer[0])
        //    {
        //        var tmp = (long*)ptr;

        //        *tmp = val.Ticks;
        //    }

        //    stream.Write(buffer, 0, 8);
        //}

        public void Write(byte[] buf)
        {
            stream.Write(buf, 0, buf.Length);
        }

        public void Write(byte[] buf, int position, int length)
        {
            stream.Write(buf, position, length);
        }

        public void SetStream(Stream stream)
        {
            this.stream = stream;
        }
    }
}
