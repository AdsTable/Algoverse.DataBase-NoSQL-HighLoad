using System;
using System.IO;

namespace Algoverse.DataBase
{
    public class PageFileWriter
    {
        readonly FileStream stream;
        readonly int size;
        readonly PageFileIOMode mode;
        readonly byte[] buf;
        public CRC32 Crc32 { get; }

        public PageFileWriter(FileStream stream, int size, PageFileIOMode mode)
        {
            this.stream = stream;
            this.size = size;
            this.mode = mode;

            if (mode == PageFileIOMode.Standart)
            {
                buf = new byte[8];
            }
            else
            {
                buf = new byte[12];
            }

            Crc32 = new CRC32();
        }

        // Метод для записи данных с быстрой проверкой crc
        public unsafe void WriteWithCRC(int code, byte[] data)
        {
            // todo узнать что быстрее работает, две записи массивов или предварительное соединение их в один
            var pos = PageFile.HeaderSize + code * size;

            if (stream.Position != pos)
            {
                stream.Position = pos;
            }

            var ticks = DateTime.Now.Ticks;

            Crc32.Update(data, 0, size - 12);
            Crc32.Update(ticks);

            fixed (byte* p = &buf[0])
            {
                *((long*)p) = ticks;
                *((int*)(p + 8)) = Crc32.Value;
            }

            // запись данных
            stream.Write(data, 0, size - 12);
            // запись времени и crc
            stream.Write(buf, 0, buf.Length);

            stream.Flush();

            Crc32.Reset();
        }

        // Запись для таблиц с отключенной проверкой crc
        public unsafe void Write(int code, byte[] data)
        {
            var pos = PageFile.HeaderSize + code * size;

            if (stream.Position != pos)
            {
                stream.Position = pos;
            }

            var ticks = DateTime.Now.Ticks;

            fixed (byte* p = &buf[0])
            {
                *((long*)p) = ticks;
            }

            // запись данных
            stream.Write(data, 0, size);
            // запись времени
            stream.Write(buf, 0, buf.Length);

            Crc32.Reset();
        }
    }
}