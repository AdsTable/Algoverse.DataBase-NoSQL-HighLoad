using System;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Algoverse.DataBase
{
    public unsafe class Int64Stack : IDisposable
    {
        int capacity;
        int itemSize;
        long length;
        string key;
        MemoryMappedFile file;
        FileStream fs;
        MemoryMappedViewAccessor body;
        long* bodyPtr;

        // Конструктор
        public Int64Stack(string fullPath)
        {
            itemSize = 8;
            capacity = 1024;

            var inf = new FileInfo(fullPath);

            key = fullPath.CalculateHashString();

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

        // Инициализация файла
        void InitFile()
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

            length = fs.Length;

            //file = MemoryMappedFile.CreateFromFile(fs, key, length, MemoryMappedFileAccess.ReadWrite, new MemoryMappedFileSecurity(), HandleInheritability.Inheritable, true);
            
            this.file = fs.CreateMMF(key);

            body = file.CreateViewAccessor(0, length, MemoryMappedFileAccess.ReadWrite);

            bodyPtr = (long*)body.Pointer(0);
        }

        // Количество
        public long Count
        {
            get
            {
                return (long)bodyPtr[0];
            }
            set
            {
                bodyPtr[0] = value;
            }
        }

        // Добавление
        public void Push(long value)
        {
            var pos = ++Count;

            CheckFileSize(pos);

            bodyPtr[pos] = value;
        }

        // Получение последнего элемента
        public long Pop()
        {
            return bodyPtr[Count--];
        }

        // Проверка размера файла
        void CheckFileSize(long maxIndex)
        {
            var ind = maxIndex * (long)itemSize;

            if (ind >= (long)length)
            {
                lock (fs)
                {
                    if (ind >= (long)length)
                    {
                        fs.SetLength(length * 2);

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

            file?.Dispose();
            fs?.Dispose();
        }
    }
}
