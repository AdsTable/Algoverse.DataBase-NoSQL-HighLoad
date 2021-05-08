using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Algoverse.Threading;

namespace Algoverse.DataBase
{

    // TODO Нужно ввести понятие бинарных строк. Это позволит не переводить строки этого хранилища в обычные
    // это позволит значительно ускорить сравнение записей в индексах. Так можно реализовать метод записи такой
    // строки в бинарный поток, что так же ускорит работу. null copy рулит
    // Строки фиксированной длинны с возможностью переполнения или обрезания, должно настраиваться
    public unsafe class ArrayStorage : IDisposable
    {
        #region ' Core '

        readonly string fullPath;
        public static readonly string MarketId;

        static ArrayStorage()
        {
            MarketId = Guid.NewGuid().ToString();
        }

        const int HeaderSize = 100;

        long length;
        string ind;
        bool disposed;
        MemoryMappedFile file;
        MemoryMappedViewAccessor header_mma;
        long* header_ptr;
        FileStream fs;
        Int64Stack deleted;
        Int64Stack free;
        ArrayStorageList list;
        Pool<FileStream> poolReaders;
        Pool<FileStream> poolWriters;

        public ArrayStorage(string fullPath)
        {
            this.fullPath = fullPath;
            var inf = new FileInfo(fullPath);

            poolReaders = new Pool<FileStream>();
            poolWriters = new Pool<FileStream>();

            Key = fullPath.CalculateHashString();

            // Создаем заголовок файла.
            if (!inf.Exists)
            {
                //fullPath.CheckPath();

                fs = new FileStream(fullPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                fs.SetLength(HeaderSize);

                InitFile();

                header_ptr[0] = (long)HeaderSize;
            }
            else if (inf.Length < HeaderSize)
            {
                throw new Exception("File corrupt.");
            }
            else
            {
                fs = new FileStream(fullPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite); //

                InitFile();
            }

            if (inf.IsReadOnly)
            {
                inf.IsReadOnly = false;
            }

            list = new ArrayStorageList(fullPath + ".asl");
            deleted = new Int64Stack(fullPath + ".del");
            free = new Int64Stack(fullPath + ".fre");
        }

        public string Key { get; private set; }

        ~ArrayStorage()
        {
            Dispose();
        }

        #endregion

        #region ' Write '
        
        public long Write(string val)
        {
            return Write(System.Text.Encoding.Unicode.GetBytes(val));
        }

        public long Write(byte[] val)
        {
            var len = val.Length;
            var ret = free.Count > 0 ? free.Pop() : list.CreateItem();
            var pos = CreateItem(len);

            var itm = list[ret];

            itm->Position = pos;
            itm->Length = len;

            var w = GetWriter();

            if (w.Position != pos)
            {
                w.Position = pos;
            }

            w.Write(val, 0, val.Length);
            w.Flush();

            poolWriters.ReleaseInstance(w);

            return ret;
        }

        public long Write<T>(T[] val) where T : struct 
        {
            var len = val.Length * 4;
            var ret = free.Count > 0 ? free.Pop() : list.CreateItem();
            var pos = CreateItem(len);

            var itm = list[ret];

            itm->Position = pos;
            itm->Length = len;

            var w = GetWriter();

            if (w.Position != pos)
            {
                w.Position = pos;
            }

            var buf = new byte[val.Length * sizeof(int)];

            Buffer.BlockCopy(val, 0, buf, 0, buf.Length);

            w.Write(buf, 0, val.Length);
            w.Flush();

            poolWriters.ReleaseInstance(w);

            return ret;
        }

        #endregion 

        #region ' Read '

        public string ReadString(long key)
        {
            var buf = ReadBytes(key);

            if (buf.Length > 0 && buf[1] == 0 && buf[0] == 0)
            {
                Console.WriteLine(System.Text.Encoding.Unicode.GetString(buf));

                int bp = 0;
            }

            return System.Text.Encoding.Unicode.GetString(buf);
        }

        public byte[] ReadBytes(long key)
        {
            var itm = list[key];

            var pos = itm->Position;
            var len = itm->Length;
            
            var r = GetReader();

            if (r.Position != pos)
            {
                r.Position = pos;
            }
            
            var buf = new byte[len];

            if (len == 64)
            {
                int bp = 0;
            }

            //r.Read(buf, 0, buf.Length);

            var task = r.ReadAsync(buf, 0, buf.Length);

            task.Wait();

            //await r.CopyToAsync(r);

            if (buf.Length > 1 && buf[1] == 0 && buf[0] == 0)
            {
                Console.WriteLine("WARNING");
                Console.WriteLine("WARNING");
                Console.WriteLine("WARNING");
                Console.WriteLine("WARNING");
                Console.WriteLine("WARNING");
                Console.WriteLine("WARNING");
                Console.WriteLine("WARNING");
                Console.WriteLine("WARNING");
            }

            poolReaders.ReleaseInstance(r);

            return buf;
        }


        public string ReadString2(long key)
        {
            var buf = ReadBytes(key);

            return System.Text.Encoding.Unicode.GetString(buf);
        }

        public byte[] ReadStringAsByteArray(long key)
        {
            var buf = ReadBytes(key);

            return buf;
        }

        public byte[] ReadBytes2(long key)
        {
            var itm = list[key];

            var pos = itm->Position;
            var len = itm->Length;

            var r = GetReader();

            if (r.Position != pos)
            {
                r.Position = pos;
            }

            var buf = new byte[len];

            r.Read(buf, 0, buf.Length);

            poolReaders.ReleaseInstance(r);

            return buf;
        }

        public T[] Read<T>(long key) where T : struct
        {
            var itm = list[key];

            var pos = itm->Position;
            var len = itm->Length;

            var r = GetReader();

            if (r.Position != pos)
            {
                r.Position = pos;
            }

            var buf = new byte[len];

            r.Read(buf, 0, buf.Length);

            poolReaders.ReleaseInstance(r);

            var ret = new T[len / SizeOf<T>()];

            Buffer.BlockCopy(buf, 0, ret, 0, buf.Length);

            return ret;
        }
        
        #endregion

        #region ' Helper '

        public void Clear()
        {
            header_ptr[0] = (long)HeaderSize;

            list.Clear();
        }

        static Dictionary<Type, Int32> ht_size = new Dictionary<Type,int>(200);

        public static int SizeOf<T>()
        {
            var t = typeof(T);

            if (!ht_size.ContainsKey(t))
            {
                lock (ht_size)
                {
                    if (!ht_size.ContainsKey(t))
                    {
                        var s = SizeOf(t);

                        ht_size.Add(t, s);
                    }
                }
            }

            return ht_size[t];
        }

        static int SizeOf(Type type)
        {
            var dynamicMethod = new DynamicMethod("SizeOf", typeof(int), Type.EmptyTypes);
            var generator = dynamicMethod.GetILGenerator();

            generator.Emit(OpCodes.Sizeof, type);
            generator.Emit(OpCodes.Ret);

            var function = (Func<int>)dynamicMethod.CreateDelegate(typeof(Func<int>));

            return function();
        }

        public void Remove(long key)
        {
            deleted.Push(key);
        }

        // Создание нового элемента
        public long CreateItem(int len)
        {
            // TODO может сздесь лучше будет выглядеть ValueLock или interlocked
            lock (fullPath)
            {
                var c = header_ptr[0];

                header_ptr[0] += (long)len;

                if (header_ptr[0] >= length)
                {
                    length = header_ptr[0] * 2;

                    fs.SetLength(length); 
                }

                return c;
            }
        }

        // Инициализая файла
        void InitFile()
        {
            if (header_mma != null)
            {
                header_mma.SafeMemoryMappedViewHandle.ReleasePointer();
                header_mma.Dispose();
            }

            if (file != null)
            {
                file.Dispose();
            }

            length = fs.Length;

            this.file = fs.CreateMMF(Key);

            header_mma = file.CreateViewAccessor(0, HeaderSize, MemoryMappedFileAccess.ReadWrite);
            header_ptr = (long*)header_mma.Pointer(0);
        }

        //
        FileStream GetReader()
        {
            var obj = poolReaders.GetInstance();

            if (obj == null)
            {
                obj = new FileStream(fullPath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
            }

            return obj;
        }

        //
        FileStream GetWriter()
        {
            var obj = poolWriters.GetInstance();

            if (obj == null)
            {
                obj = new FileStream(fullPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite); //FileOptions.RandomAccess
            }

            return obj;
        }

        internal void FlushWriters()
        {
            var list = poolWriters.GetInstanceList();

            for (var i = 0; i < list.Length; ++i)
            {
                var itm = list[i];

                itm.Flush();
            }
        }


        // Освобождение ресурсов
        public void Dispose()
        {
            if (!disposed)
            {
                if (header_mma != null)
                {
                    header_mma.SafeMemoryMappedViewHandle.ReleasePointer();
                    header_mma.Dispose();
                }

                if (file != null)
                {
                    file.Dispose();
                }

                //if (fs != null)
                //{
                //    fs.Close();
                //}
            }

            disposed = true;
        }

        #endregion

        #region ' Temp '

        //public int[] ReadInt32Array(long key)
        //{
        //    var itm = list[key];

        //    var pos = itm->Position;
        //    var len = itm->Length / 4;

        //    var mma = file.CreateViewAccessor(pos, len * 4, MemoryMappedFileAccess.ReadWrite);
        //    var ptr = (int*)mma.Pointer(pos);
        //    var ar = new int[len];

        //    for (int i = 0; i < len; ++i)
        //    {
        //        ar[i] = ptr[i];
        //    }

        //    mma.SafeMemoryMappedViewHandle.ReleasePointer();
        //    mma.Dispose();

        //    return ar;
        //}

        //public uint[] ReadUInt32Array(long key)
        //{
        //    var itm = list[key];

        //    var pos = itm->Position;
        //    var len = itm->Length / 4;

        //    var mma = file.CreateViewAccessor(pos, len * 4, MemoryMappedFileAccess.ReadWrite);
        //    var ptr = (uint*)mma.Pointer(pos);
        //    var ar = new uint[len];

        //    for (int i = 0; i < len; ++i)
        //    {
        //        ar[i] = ptr[i];
        //    }

        //    mma.SafeMemoryMappedViewHandle.ReleasePointer();
        //    mma.Dispose();

        //    return ar;
        //}

        #endregion
    }
}
