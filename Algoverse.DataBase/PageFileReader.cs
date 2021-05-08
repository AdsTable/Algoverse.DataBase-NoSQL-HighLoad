using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Algoverse.DataBase
{
    public class PageFileReader
    {
        readonly FileStream stream;
        readonly int size;
        readonly byte[] buf;

        public PageFileReader(FileStream stream, int size)
        {
            this.stream = stream;
            this.size = size;

            buf = new byte[size];
        }

        public unsafe byte[] Read(int code, out long time)
        {
            var pos = PageFile.HeaderSize + code * size;

            if (stream.Position != pos)
            {
                stream.Position = pos;
            }

            stream.Read(buf, 0, size);

            var rec_size = size - 8;
            var ret = new byte[rec_size];

            Array.Copy(buf, ret, rec_size);

            fixed (byte* p = &buf[rec_size])
            {
                var ptr_time = (long*)p;

                time = *ptr_time;
            }

            return ret;
        }

        public byte[] ReadRaw(int code, int length)
        {
            var pos = PageFile.HeaderSize + code * size;

            if (stream.Position != pos)
            {
                stream.Position = pos;
            }

            var tmp = new byte[size * length];

            stream.Read(tmp, 0, tmp.Length);

            return tmp;
        }

        // Данная функция читает данные, поле времени и поле crc. 
        public unsafe byte[] ReadWithCRC(int code, out long time, out int crc32)
        {
            var pos = PageFile.HeaderSize + code * size;

            if (stream.Position != pos)
            {
                stream.Position = pos;
            }

            stream.Read(buf, 0, size);

            var rec_size = size - 12;
            var ret = new byte[rec_size];

            Array.Copy(buf, ret, rec_size);

            fixed (byte* p = &buf[rec_size])
            {
                var ptr_time = (long*)p;

                time = *ptr_time;

                var ptr_crc = (int*)(p + 8);

                crc32 = *ptr_crc;
            }

            return ret;
        }
    }
}
