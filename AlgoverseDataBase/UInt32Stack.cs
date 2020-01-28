using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;
using Algoverse.Threading;

namespace Algoverse.DataBase
{
    public unsafe class UInt32Stack : IDisposable
    {
        int capacity;
        long length;
        string key;
        MemoryMappedFile file;
        FileStream fs;
        MemoryMappedViewAccessor body;
        int* bodyPtr;
        internal ValueLockRW Locker = new ValueLockRW();

        // Хеш таблица используется для оптимизации проверки на присутсвие
        HashSet<int> map;

        // Конструктор
        public UInt32Stack(string fullPath)
        {
            capacity = 1024;
            key = fullPath.CalculateHashString();

            var inf = new FileInfo(fullPath);
            
            // Создаем заголовок файла.
            if (!inf.Exists)
            {
                fs = new FileStream(fullPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                fs.SetLength(capacity);
            }
            else
            {
                fs = new FileStream(fullPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
            }

            if (inf.IsReadOnly)
            {
                inf.IsReadOnly = false;
            }

            InitFile();
        }

        // Получаем по индексу
        public int this[int index]
        {
            get
            {
                return bodyPtr[index + 1];
            }
        }

        // Инициализация файла
        void InitFile()
        {
            var tmp_body = body;
            var tmp_file = file;

            length = fs.Length;

            this.file = fs.CreateMMF(key);

            body = file.CreateViewAccessor(0, length, MemoryMappedFileAccess.ReadWrite);

            bodyPtr = (int*)body.Pointer(0);

            if (tmp_body != null)
            {
                tmp_body.SafeMemoryMappedViewHandle.ReleasePointer();
                tmp_body.Dispose();                
            }

            tmp_file?.Dispose();
        }

        // Количество
        public int Count
        {
            get => bodyPtr[0];
            private set => bodyPtr[0] = value;
        }

        // Проверить есть ли элемент в коллекции
        public bool Contains(int val)
        {
            try
            {
                Locker.ReadLock();

                if (map == null)
                {
                    GetMap();
                }

                return map.Contains(val);
            }
            finally
            {
                Locker.Unlock();
            }
        }
        
        public HashSet<int> GetMap()
        {
            try
            {   // TODO Возможно ли сделать гибрид хеш таблицы и стека? Что бы экономить память
                if (map == null)
                {
                    Locker.WriteLock();

                    if (map == null)
                    {
                        map = new HashSet<int>();

                        for (var i = 1; i < Count + 1; i++)
                        {
                            var itm = bodyPtr[i];

                            if (!map.Contains(itm))
                            {
                                map.Add(itm);
                            }
                        }

                        return map;
                    }
                }

                return map;
            }
            finally
            {
                Locker.Unlock();
            }
        }

        // Добавление
        public void Push(int value)
        {
            // Todo как насчет interlocked ?
            try
            {
                Locker.WriteLock();

                var pos = ++Count;
                
                CheckFileSize(pos);

                bodyPtr[pos] = value;

                if (map != null)
                {
                    if (!map.Contains(value))
                    {
                        map.Add(value);
                    }
                }
            }
            finally
            {
                Locker.Unlock();
            }
        }

        // Получение последнего элемента
        public int Pop()
        {
            if (Count == 0)
            {
                return 0;
            }

            try
            {
                Locker.WriteLock();

                var ind = Count--;
                var val = bodyPtr[ind];

                if (map != null)
                {
                    if (map.Contains(val))
                    {
                        map.Remove(val);
                    }
                }

                return val;
            }
            finally
            {
                Locker.Unlock();
            }
        }

        // Очистка стека
        public void Clear()
        {
            try
            {
                Locker.WriteLock();

                Count = 0;

                if (map != null)
                {
                    map.Clear();
                }
            }
            finally
            {
                Locker.Unlock();
            }            
        }

        // Проверка размера файла
        void CheckFileSize(int maxIndex)
        {
            var ind = maxIndex * 4;

            if (ind >= length)
            {
                lock (fs)
                {
                    if (ind >= length)
                    {
                        fs.SetLength(ind * 2);

                        InitFile();
                    }
                }
            }
        }

        // Освобождение ресурсов
        public void Dispose()
        {
            if (body != null)
            {
                body.SafeMemoryMappedViewHandle.ReleasePointer();
                body.Dispose();
            }

            if (file != null)
            {
                file.Dispose();
            }

            if (fs != null)
            {
                fs.Dispose();
            }
        }
    }
}
