using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;
using Algoverse.Threading;

namespace Algoverse.DataBase
{
    // Потокобезопасный класс
    public unsafe class PageFile : IDisposable
    {
        public const int HeaderSize = 128;

        long                        length;
        string                      ind;
        readonly string             fullPath;
        int                         pageSize;
        PageFileIOMode              ioMode;
        bool                        disposed;
        FileStream                  stream;
        internal UInt32Stack        deleted;
        ValueLock                   locker;
        MemoryMappedFile            file;
        MemoryMappedViewAccessor    acc;
        int*                        header;
        Pool<PageFileReader>        poolReaders;
        Pool<PageFileWriter>        poolWriters;

        public PageFile(string fullPath, int pageSize, PageFileIOMode ioMode)
        {
            this.fullPath = fullPath;
            this.pageSize = pageSize;
            this.ioMode = ioMode;

            Capacity = 1000;
            locker = new ValueLock();
            Key = fullPath.CalculateHashString();
            poolReaders = new Pool<PageFileReader>();
            poolWriters = new Pool<PageFileWriter>();

            var inf = new FileInfo(fullPath);

            // Создаем заголовок файла.
            if (!inf.Exists)
            {
                //fullPath.CheckPath();

                stream = CreateStream();
                stream.SetLength(HeaderSize + Capacity * pageSize);

                InitFile();

                header[1] = pageSize;
            }
            else if (inf.Length < HeaderSize)
            {
                throw new Exception("File corrupt.");
            }
            else
            {
                stream = CreateStream();

                InitFile();

                if (header[1] != pageSize)
                {
                    throw new Exception("Wrong page size.");
                }
            }

            if (inf.IsReadOnly)
            {
                inf.IsReadOnly = false;
            }

            deleted = new UInt32Stack(fullPath + ".del");
        }

        ~PageFile()
        {
            Dispose();
        }

        // Ключ файла
        public string Key { get; }

        // Количество физически выделенных страниц. 
        // Если страница удаляется или используется повторно это не влияет на Count
        public int Count
        {
            get
            {
                return header[0];
            }
        }

        internal FileStream Stream 
        {
            get
            {
                return stream;
            }
        }

        internal FileStream CreateStream()
        {
            Debug.WriteLine("!" + Thread.CurrentThread.ManagedThreadId + "  |  " + fullPath);

            var str = new FileStream(fullPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite, 1024, true);
            
            return str;
        }

        //internal FileStream CreateStream(FileAccess fa)
        //{
        //    var str = new FileStream(fullPath, FileMode.OpenOrCreate, fa, FileShare.ReadWrite, 1024);

        //    return str;
        //}

        int AddCount(int val)
        {
            header[0] += val;

            return header[0];
        }

        // Количество памяти под новые элементы, выделяется когда свободная память заканчивается
        public int Capacity { get; set; }
        
        // Размер страницы
        public int PageSize
        {
            get
            {
                return pageSize;
            }
        }

        // Создает новую страницу
        public int Alloc()
        {
            var page = deleted.Pop();

            //var page = 0;
           
            //if (deleted.Count > 0)
            //{
            //    page = deleted.Pop();
            //}
            if (page == 0)
            {
                page = AddCount(1);

                if (HeaderSize + pageSize * Count >= length)
                {
                    try
                    {
                        locker.Lock();

                        if (HeaderSize + pageSize * Count >= length)
                        {
                            length = HeaderSize + pageSize * Count * 2;

                            stream.SetLength(length);
                        }
                    }
                    finally
                    {
                        locker.Unlock();
                    }
                }
            }

            return page;
        }

        // Создает указанное число страниц
        public int Alloc(int count)
        {
            if (count == 0)
            {
                return 0;
            }

            var page = 0;

            try
            {            
                locker.Lock();

                page = Count + 1;

                AddCount(count);

                if (HeaderSize + pageSize * Count >= length)
                {
                    length = HeaderSize + pageSize * Count * 2;

                    stream.SetLength(length);
                }
            }
            finally
            {
                locker.Unlock();
            }

            return page;
        }

        // Удаляем страницу
        public void Remove(int page)
        {
            deleted.Push(page);
        }

        // Очистка страниц
        public void Clear()
        {
            try
            {
                locker.Lock();

                // count
                header[0] = 0;

                deleted.Clear();

            }
            finally
            {
                locker.Unlock();
            }
        }
        
        // Здесь мы сохраним данные удаленных страниц
        public void Dispose()
        {
            if (!disposed)
            {
                try
                {
                    if (stream != null)
                    {
                        stream.Close();
                    }
                }
                finally
                {
                    disposed = true;
                }
            }
        }

        // Показывает существует ли запись с данным кодом
        public bool Contains(int page)
        {
            if (deleted.Contains(page))
            {
                return false;
            }

            return page <= Count;
        }

        // Возвращает записывающий поток
        public PageFileWriter GetWriter()
        {
            var obj = poolWriters.GetInstance();

            if (obj == null)
            {
                var st = CreateStream();
                //var st = CreateStream(FileAccess.Write);

                obj = new PageFileWriter(st, PageSize, ioMode);
            }

            return obj;
        }

        // Возвращает читающий поток
        public PageFileReader GetReader()
        {
            var obj = poolReaders.GetInstance();

            if (obj == null)
            {
                var st = CreateStream();
                //var st = CreateStream(FileAccess.Read);
                obj = new PageFileReader(st, PageSize);
            }

            return obj;
        }

        // Заносим в пул поток чтения
        public void ReleaseReader(PageFileReader device)
        {
            poolReaders.ReleaseInstance(device);
        }

        // Заносим в пул поток записи
        public void ReleaseWriter(PageFileWriter device)
        {
            poolWriters.ReleaseInstance(device);
        }

        // Инициализая файла
        void InitFile()
        {
            var tmp_acc = acc;
            var tmp_file = file;

            length = stream.Length;

            this.file = stream.CreateMMF(Key);

            acc = file.CreateViewAccessor(0, HeaderSize, MemoryMappedFileAccess.ReadWrite);
            header = (int*)acc.Pointer(0);

            if (tmp_acc != null)
            {
                tmp_acc.SafeMemoryMappedViewHandle.ReleasePointer();
                tmp_acc.Dispose();                
            }
            if (tmp_file != null)
            {
                tmp_file.Dispose();
            }
        }

        // Установка количество, необходимо при восстановлении из бекапа
        internal void SetCount(int count)
        {
            try
            {
                header[0] = count;

                if (HeaderSize + pageSize * Count > length)
                {
                    length = HeaderSize + pageSize * count * 2;

                    stream.SetLength(length);
                }
            }
            finally
            {
                locker.Unlock();
            }
        }

        //static MemoryMappedFileSecurity fileSecurity;
        //static MemoryMappedFileSecurity FileSecurity
        //{
        //    get
        //    {
        //        if (fileSecurity == null)
        //        {
        //            var mmfs = new MemoryMappedFileSecurity();
        //            var ppc = WindowsIdentity.GetCurrent();
        //            mmfs.AddAccessRule(new AccessRule<MemoryMappedFileRights>(ppc.User, MemoryMappedFileRights.ReadWrite, AccessControlType.Allow));

        //            fileSecurity = mmfs;
        //        }

        //        return fileSecurity;
        //    }
        //}

        // Получить карту удаленных кодов
        public HashSet<int> GetDeletedMap()
        {
            var ht = new HashSet<int>();

            for (int i = 0; i < deleted.Count; ++i)
            {
                ht.Add(deleted[i]);
            }

            return ht;
        }
        
        #region ' Temp '

        //Pool<ReaderCRC32> poolReaders;
        //Pool<WriterCRC32> poolWriters;

        //// Возвращает записывающее устройство
        //public IWriter GetWriter(int page, ArrayStorage storage)
        //{
        //    var obj = poolWriters.GetInstance();

        //    if (obj == null)
        //    {
        //        obj = new WriterCRC32(page, 1, this, storage);
        //    }
        //    else
        //    {
        //        obj.Init(page, 1);
        //    }

        //    return obj;
        //}

        //// Возвращает записывающее устройство
        //public IWriter GetWriter(int page, int count, ArrayStorage storage)
        //{
        //    var obj = poolWriters.GetInstance();

        //    if (obj == null)
        //    {
        //        obj = new WriterCRC32(page, count, this, storage);
        //    }
        //    else
        //    {
        //        obj.Init(page, count);
        //    }

        //    return obj;
        //}

        //// Возвращает читающее устройство
        //public PageReader GetReader(int page, ArrayStorage storage)
        //{
        //    var obj = poolReaders.GetInstance();

        //    if (obj == null)
        //    {
        //        obj = new ReaderCRC32(page, 1, this, storage);
        //    }
        //    else
        //    {
        //        obj.Init(page, 1);
        //    }

        //    return obj;
        //}

        //// Возвращает читающее устройство
        //public PageReader GetReader(int page, int count, ArrayStorage storage)
        //{
        //    if (count == 0)
        //    {
        //        return null;
        //    }

        //    var obj = poolReaders.GetInstance();

        //    if (obj == null)
        //    {
        //        obj = new ReaderCRC32(page, count, this, storage);
        //    }
        //    else
        //    {
        //        obj.Init(page, count);
        //    }

        //    return obj;
        //}

        //// Заносим в пул устройство чтения
        //public void ReleaseReader(ReaderCRC32 device)
        //{
        //    poolReaders.ReleaseInstance(device);
        //}

        //// Заносим в пул устройство записи
        //public void ReleaseWriter(WriterCRC32 device)
        //{
        //    poolWriters.ReleaseInstance(device);
        //}

        #endregion
    }

    public enum PageFileIOMode
    {
        /// <summary>
        /// Only write records
        /// </summary>
        Standart = 0,

        /// <summary>
        /// Write records and their metadata crc32 signature
        /// </summary>
        CRC32 = 1,

    }
}


//fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 1024, FileOptions.Asynchronous | FileOptions.RandomAccess);
//bw = new BinaryWriter(fs);
//br = new BinaryReader(fs);