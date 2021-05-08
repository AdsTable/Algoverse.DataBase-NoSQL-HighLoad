using System;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Algoverse.DataBase
{
    public unsafe class PageReader : IDisposable, IReader
    {
        int size;
        readonly PageFile file;
        readonly ArrayStorage ars;
        bool disposed;
        int pageSize;
        byte[] buf;
        FileStream stream;
        int startPage;

        internal PageReader(int page, int count, PageFile file, ArrayStorage storage)
        {
            this.file = file;

            pageSize = file.PageSize;
            stream = file.CreateStream(FileAccess.Read);
            buf = new byte[pageSize];
            ars = storage;
            

            Init(page, count);
        }

        public void Init(int page, int count)
        {
            size = pageSize * count;
            startPage = page;
            Position = 0;
        }

        public int Position { get; set; }

        internal ArrayStorage Storage
        {
            get
            {
                return ars;
            }
        }

        public virtual bool ReadBoolean()
        {
            if (Position >= size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 1;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (bool*)ptr;

                return *p;
            }
        }

        public virtual byte ReadUInt8()
        {
            if (Position >= size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 1;

            return buf[offset];
        }

        public virtual sbyte ReadInt8()
        {
            if (Position >= size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 1;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (sbyte*)ptr;

                return *p;
            }
        }

        public virtual short ReadInt16()
        {
            if (Position + 2 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 2;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (short*)ptr;

                return *p;
            }
        }

        public virtual ushort ReadUInt16()
        {
            if (Position + 2 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 2;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (ushort*)ptr;

                return *p;
            }
        }

        public virtual char ReadChar()
        {
            if (Position + 2 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 2;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (char*)ptr;

                return *p;
            }
        }

        public virtual int ReadInt32()
        {
            if (Position + 4 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk(); 
            }

            Position += 4;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (int*)ptr;

                return *p;
            }
        }

        public virtual uint ReadUInt32()
        {
            if (Position + 4 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 4;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (uint*)ptr;

                return *p;
            }
        }

        public virtual float ReadSingle()
        {
            if (Position + 4 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 4;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (float*)ptr;

                return *p;
            }
        }

        public virtual long ReadInt64()
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 8;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (long*)ptr;

                return *p;
            }
        }

        public virtual ulong ReadUInt64()
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 8;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (ulong*)ptr;

                return *p;
            }
        }

        public virtual double ReadDouble()
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 8;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (double*)ptr;

                return *p;
            }
        }

        public virtual string ReadString()
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 8;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (long*)ptr;

                return ars.ReadString(*p);
            }
        }

        protected string ReadString(out long offset)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset2 = Position % pageSize;

            if (offset2 == 0)
            {
                ReadFromDisk();
            }

            Position += 8;

            fixed (byte* ptr = &buf[offset2])
            {
                var p = (long*)ptr;
                
                offset = *p;

                return ars.ReadString(*p);
            }
        }

        public virtual int[] ReadInt32Array()
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 8;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (long*)ptr;

                //return ars.ReadInt32Array(*p);
                return ars.Read<int>(*p);
            }
        }

        protected int[] ReadInt32Array(out long offset)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset2 = Position % pageSize;

            if (offset2 == 0)
            {
                ReadFromDisk();
            }

            Position += 8;

            fixed (byte* ptr = &buf[offset2])
            {
                var p = (long*)ptr; 
                
                offset = *p;

                //return ars.ReadInt32Array(*p);
                return ars.Read<int>(*p);
            }
        }

        public virtual uint[] ReadUInt32Array()
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 8;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (long*)ptr;

                //return ars.ReadUInt32Array(*p);
                return ars.Read<uint>(*p);
            }
        }

        protected uint[] ReadUInt32Array(out long offset)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset2 = Position % pageSize;

            if (offset2 == 0)
            {
                ReadFromDisk();
            }

            Position += 8;

            fixed (byte* ptr = &buf[offset2])
            {
                var p = (long*)ptr;

                offset = *p;

                //return ars.ReadUInt32Array(*p);
                return ars.Read<uint>(*p);
            }
        }

        public virtual byte[] ReadBytes()
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 8;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (long*)ptr;

                return ars.ReadBytes(*p);
            }
        }

        protected byte[] ReadBytes(out long offset)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset2 = Position % pageSize;

            if (offset2 == 0)
            {
                ReadFromDisk();
            }

            Position += 8;

            fixed (byte* ptr = &buf[offset2])
            {
                var p = (long*)ptr;

                offset = *p;

                return ars.ReadBytes(*p);
            }
        }

        public virtual DateTime ReadDateTime()
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 8;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (long*)ptr;

                return new DateTime(*p);
            }
        }

        public virtual TimeSpan ReadTimeSpan()
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            if (offset == 0)
            {
                ReadFromDisk();
            }

            Position += 8;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (long*)ptr;

                return new TimeSpan(*p);
            }
        }

        public void ReadFromDisk()
        {
            var pos = PageFile.HeaderSize + (startPage + Position / pageSize) * pageSize;
            
            //var b = new byte[stream.Length];

            //stream.Read(b, 0, b.Length);

            if (stream.Position != pos)
            {
                stream.Position = pos;
            }

            stream.Read(buf, 0, buf.Length);
        }

        public virtual void Dispose()
        {
            if (!disposed)
            {
                disposed = true;

                stream.Close();
            }
        }
    }
}
