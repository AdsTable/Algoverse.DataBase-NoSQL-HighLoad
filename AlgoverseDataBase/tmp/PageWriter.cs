using System;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Algoverse.DataBase
{
    public unsafe class PageWriter : IDisposable, IWriter
    {
        int size;
        readonly ArrayStorage ars;
        readonly int pageSize;
        readonly PageFile file;
        int startPage;

        bool disposed;

        byte[] buf;
        FileStream stream;

        internal PageWriter(int page, int count, PageFile file, ArrayStorage ars)
        {
            pageSize = file.PageSize;
            buf = new byte[pageSize];
            stream = file.CreateStream(FileAccess.Write);

            this.file = file;
            this.ars = ars;

            Init(page, count);
        }

        public void Init(int page, int count)
        {
            size = pageSize * count;
            startPage = page;
            Position = 0;
        }

        internal int GetCurrentCode()
        {
            return startPage + Position / pageSize;
        }
        
        public int Position { get; set; }

        internal ArrayStorage Storage
        {
            get
            {
                return ars;
            }
        }

        public virtual void Write(bool val)
        {
            if (Position >= size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (bool*)ptr;

                *p = val;
            }

            Position += 1;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        public virtual void Write(byte val)
        {
            if (Position >= size) throw new Exception("Position out of range");

            var offset = Position % pageSize;

            buf[offset] = val;

            Position += 1;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        public virtual void Write(sbyte val)
        {
            if (Position >= size) throw new Exception("Position out of range");

            var offset = Position % pageSize;

            buf[offset] = (byte)val;

            Position += 1;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        public virtual void Write(short val)
        {
            if (Position + 2 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (short*)ptr;

                *p = val;
            }

            Position += 2;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        public virtual void Write(ushort val)
        {
            if (Position + 2 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (ushort*)ptr;

                *p = val;
            }

            Position += 2;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        public virtual void Write(char val)
        {
            if (Position + 2 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (char*)ptr;

                *p = val;
            }

            Position += 2;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        public virtual void Write(int val)
        {
            if (Position + 4 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (int*)ptr;

                *p = val;
            }

            Position += 4;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        public virtual void Write(uint val)
        {
            if (Position + 4 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (uint*)ptr;

                *p = val;
            }

            Position += 4;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        public virtual void Write(float val)
        {
            if (Position + 4 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (float*)ptr;

                *p = val;
            }

            Position += 4;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        public virtual void Write(long val)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (long*)ptr;

                *p = val;
            }

            Position += 8;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        public virtual void Write(ulong val)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (ulong*)ptr;

                *p = val;
            }

            Position += 8;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        public virtual void Write(double val)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (double*)ptr;

                *p = val;
            }

            Position += 8;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        public virtual void Write(string val)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var v = ars.Write(val);
            var offset = Position % pageSize;
            
            fixed (byte* ptr = &buf[offset])
            {
                var p = (long*)ptr;

                *p = v;
            }

            Position += 8;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        protected void Write(string val, out long offset)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            offset = ars.Write(val);
            var offset2 = Position % pageSize;

            fixed (byte* ptr = &buf[offset2])
            {
                var p = (long*)ptr;

                *p = offset;
            }

            Position += 8;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        public virtual void Write(int[] val)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var v = ars.Write(val);
            var offset = Position % pageSize;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (long*)ptr;

                *p = v;
            }

            Position += 8;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        protected void Write(int[] val, out long offset)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            offset = ars.Write(val);
            var offset2 = Position % pageSize;

            fixed (byte* ptr = &buf[offset2])
            {
                var p = (long*)ptr;

                *p = offset;
            }

            Position += 8;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }
        
        public virtual void Write(uint[] val)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var v = ars.Write(val);
            var offset = Position % pageSize;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (long*)ptr;

                *p = v;
            }

            Position += 8;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        protected void Write(uint[] val, out long offset)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            offset = ars.Write(val);
            var offset2 = Position % pageSize;

            fixed (byte* ptr = &buf[offset2])
            {
                var p = (long*)ptr;

                *p = offset;
            }

            Position += 8;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        public virtual void Write(byte[] val)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var v = ars.Write(val);
            var offset = Position % pageSize;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (long*)ptr;

                *p = v;
            }

            Position += 8;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        protected void Write(byte[] val, out long offset)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            offset = ars.Write(val);
            var offset2 = Position % pageSize;

            fixed (byte* ptr = &buf[offset2])
            {
                var p = (long*)ptr;

                *p = offset;
            }

            Position += 8;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }

        public virtual void Write(DateTime val)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (long*)ptr;

                *p = val.Ticks;
            }

            Position += 8;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }        

        public virtual void Write(TimeSpan val)
        {
            if (Position + 8 > size)
            {
                throw new Exception("Position out of range");
            }

            var offset = Position % pageSize;

            fixed (byte* ptr = &buf[offset])
            {
                var p = (long*)ptr;

                *p = val.Ticks;
            }

            Position += 8;

            if (Position % pageSize == 0)
            {
                WriteToDisk();
            }
        }
        
        void WriteToDisk()
        {
            var pos = PageFile.HeaderSize + (startPage + Position / pageSize - 1) * pageSize;

            if (stream.Position != pos)
            {
                stream.Position = pos;
            }

            stream.Write(buf, 0, buf.Length);
            stream.Flush();
            //(file.Stream as FileStream).Write(buf, 0, buf.Length);
            //Debug.WriteLine(file.Key + "   " + pos);
        }

        public void Dispose()
        {
            if (disposed)
            {
                return;
            }

            disposed = true;
        }
    }
}
