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

        // ����� ��� ������ ������ � ������� ��������� crc
        public unsafe void WriteWithCRC(int code, byte[] data)
        {
            // todo ������ ��� ������� ��������, ��� ������ �������� ��� ��������������� ���������� �� � ����
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

            // ������ ������
            stream.Write(data, 0, size - 12);
            // ������ ������� � crc
            stream.Write(buf, 0, buf.Length);

            stream.Flush();

            Crc32.Reset();
        }

        // ������ ��� ������ � ����������� ��������� crc
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

            // ������ ������
            stream.Write(data, 0, size);
            // ������ �������
            stream.Write(buf, 0, buf.Length);

            Crc32.Reset();
        }
    }
}