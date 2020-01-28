using System;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Threading;
using Algoverse.Threading;

namespace Algoverse.DataBase
{
    public unsafe class ArrayStorageList : IDisposable
    {
        #region ' Core '

        const int HeaderSize = 80;
        readonly int itemSize;

        long length;
        string ind;
        bool disposed;
        MemoryMappedFile file;
        MemoryMappedViewAccessor header;
        FileStream fs;
        long* memory;
        ValueLockRW locker;
        ArrayStorageItem* content;

        // Конструктор
        public ArrayStorageList(string fullPath)
        {
            itemSize = sizeof (ArrayStorageItem);

            var inf = new FileInfo(fullPath);

            Key = fullPath.CalculateHashString();

            locker = new ValueLockRW();

            var exist = inf.Exists;

            // Создаем заголовок файла.
            if (!exist)
            {
                fs = new FileStream(fullPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                fs.SetLength(HeaderSize + 1000*itemSize);
            }
            else if (inf.Length < HeaderSize)
            {
                throw new Exception("File corrupt.");
            }
            else
            {
                fs = new FileStream(fullPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            }

            if (inf.IsReadOnly)
            {
                inf.IsReadOnly = false;
            }

            InitFile();

            // Смещения:
            // 0 - Количество всех элементов. 
            // 1 - Односвязанный список удаленных элементов. Когда элемент удаляется создается еще один типа ArrayStorageRemovedItem для описания удаленного блока.
            // 2 - Свободные блоки. Они образуется на месте служебных блоков используемых для списка удаленных элементов.
            //Debug.WriteLine("ArrayStorageList count - " + CountMax + ", exist: " + exist);
        }

        ~ArrayStorageList()
        {
            Dispose();
        }

        #endregion

        // Получение элемента по индексу
        public ArrayStorageItem* this[long index]
        {
            get
            {
                try
                {
                    locker.ReadLock();

                    if (index <= memory[0])
                    {
                        return content + index;
                    }

                    throw new IndexOutOfRangeException();
                }
                finally
                {
                    locker.Unlock();
                }
            }
        }
        
        // Ключ файла
        public string Key { get; }
        
        // Количество элементов без учета удаленных элементов
        public long CountMax
        {
            get
            {
                try
                {
                    locker.ReadLock();

                    return memory[0];
                }
                finally
                {
                    locker.Unlock();
                }
            }
        }

        // Создание нового элемента
        public long CreateItem(int len, out long pos)
        {
            try
            {
                locker.WriteLock();

                var rem = memory[1];

                // Если есть удаленные записи
                if (rem > 0)
                {
                    var itm = (ArrayStorageRemovedItem*)(content + rem);

                    // если блок нужного размера
                    if (content[itm->Index].Length == len)
                    {
                        memory[1] = itm->Next;

                        pos = content[itm->Index].Position;

                        // Переносим блок в список освобожденных
                        itm->Next = memory[2];
                        memory[2] = rem;

                        return itm->Index;
                    }
                }

                pos = 0;
                rem = memory[2];

                // Если есть свободные блоки
                if (rem > 0)
                {
                    var itm = (ArrayStorageRemovedItem*)(content + rem);

                    memory[2] = itm->Next;
                    
                    return rem;
                }
            }
            finally
            {
                locker.Unlock();
            }

            // Создаем новую запись
            return CreateItem();
        }

        // Создание новой записи в каталоге
        long CreateItem()
        {
            locker.WriteLock();

            var c = ++memory[0];

            locker.Unlock();

            var ind = HeaderSize + c * itemSize + itemSize;

            if (ind >= length)
            {
                locker.WriteLock();

                if (ind >= length)
                {
                    fs.SetLength(ind * 2);

                    InitFile();
                }

                locker.Unlock();
            }

            return c;            
        }

        // Инициализая файла
        void InitFile()
        {
            if (header != null)
            {
                header.SafeMemoryMappedViewHandle.ReleasePointer();
                header.Dispose();
            }

            file?.Dispose();

            length = fs.Length;

            file = fs.CreateMMF(Key);
            
            header = file.CreateViewAccessor(0, length, MemoryMappedFileAccess.ReadWrite);
            memory = (long*)header.Pointer(0);
            content = (ArrayStorageItem*)(memory + HeaderSize / 8);
        }

        // Очистить
        public void Clear()
        {
            locker.WriteLock();

            memory[0] = 0;
            memory[1] = 0;
            memory[2] = 0;

            locker.Unlock();
        }

        // Удаление элемента
        public void Remove(long key)
        {
            ArrayStorageRemovedItem* itm;

            locker.WriteLock();

            var rem = memory[2];
            
            // Если есть свободные блоки
            if (rem > 0)
            {
                itm = (ArrayStorageRemovedItem*)(content + rem);

                memory[2] = itm->Next;
            }
            else
            {
                rem = CreateItem(); // Вызов функции разблокирует locker

                itm = (ArrayStorageRemovedItem*)(content + rem);
            }

            locker.WriteLock();

            itm->Index = key;
            itm->Next = memory[1];

            memory[1] = rem;
            
            locker.Unlock();
        }

        // Освобождение ресурса
        public void Dispose()
        {
            if (!disposed)
            {
                if (header != null)
                {
                    header.SafeMemoryMappedViewHandle.ReleasePointer();
                    header.Dispose();
                }

                file?.Dispose();
                fs?.Close();

                Debug.WriteLine("ArrayStorageList Dispose");

                disposed = true;
            }
        }

        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct ArrayStorageRemovedItem
        {
            public long Index;
            public long Next;
        }
    }
}