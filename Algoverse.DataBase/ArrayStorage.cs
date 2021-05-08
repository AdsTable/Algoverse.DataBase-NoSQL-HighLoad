using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Algoverse.Threading;
using Algoverse.Threading.Collections;

namespace Algoverse.DataBase
{

    // TODO Нужно ввести понятие бинарных строк. Это позволит не переводить строки этого хранилища в обычные
    // это позволит значительно ускорить сравнение записей в индексах. Так можно реализовать метод записи такой
    // строки в бинарный поток, что так же ускорит работу. null copy рулит
    // Строки фиксированной длинны с возможностью переполнения или обрезания, должно настраиваться
    // 
    // TODO Структура list содержит всю исчерпывающую информацию для дефрагментации файла
    // Дефрагментация происходит по такому алгоритму ведется поиск пустых областей. После этого блоки копируются, 
    // и меняется позиция на блок в объекте list.Position
    //
    // TODO Идея для дальнейшего развития хранилища. При записи брать CRC32 с байтового массива, и отдавать его записи.
    // Такой подход позволит экономить место на одних и тех же данных. Коллизии можно решать второй половиной ключа 32 бита.
    // Таким образом первая половина ключа это CRC32 вторая половина это номер коллизии. Структура данных будет хеш таблица, 
    // так же как и сечай будет содержать структуру ArrayStorageItem, указывающая позицию и длинну блока данных. Побочный эффект
    // контроль целостности данных. Для мониторинга количества ссылок будет введено дополнительное поле - счетчик.
    //
    public unsafe class ArrayStorage : IDisposable
    {
        #region ' Core '

        bool disposed = false;

        readonly string fullPath;
        public static readonly string MarketId;

        static ArrayStorage()
        {
            MarketId = Guid.NewGuid().ToString();
        }
        
        AsVars vars;
        ArrayStorageList list;
        Pool<FileStream> poolReaders;
        Pool<FileStream> poolWriters;
        ValueLock locker;
        DictionarySafe<long, byte[]> cacheByte;
        DictionarySafe<long, string> cacheString;
        
        private byte[] bbuf;

        public ArrayStorage(string fullPath)
        {
            this.fullPath = fullPath;

            locker = new ValueLock();
            cacheByte = new DictionarySafe<long, byte[]>();
            cacheString = new DictionarySafe<long, string>();

            var inf = new FileInfo(fullPath);

            poolReaders = new Pool<FileStream>();
            poolWriters = new Pool<FileStream>();

            vars = new AsVars(fullPath + ".var");
            list = new ArrayStorageList(fullPath + ".asl");

            if (!inf.Exists)
            {
                if (!inf.Directory.Exists)
                {
                    inf.Directory.Create();
                }

                inf.Create().Close();

                // Выставляем первоначальный размер, что бы не дупустить запись по 0 адресу
                vars.Length = 10;
            }
            
            if (inf.IsReadOnly)
            {
                inf.IsReadOnly = false;
            }
        }

        ~ArrayStorage()
        {
            Dispose();
        }

        #endregion

        #region ' Write '

        // Запись строки. key = 0 при добавлении и key != 0 при обновлении
        public long Write(long key, string val)
        {
            if (val.Length == 0)
            {
                return 0;
            }

            var tmp = WriteBuffer(key, System.Text.Encoding.Unicode.GetBytes(val));

            // Строка была обновлена
            if (tmp == key)
            {
                cacheString[key] = val;
            }
            // Старая строка была удалена
            else if (key != 0)
            {
                cacheString.Remove(key);
                cacheString.Add(tmp, val);
            }
            // Сстрока была добавлена впервые
            else
            {
                cacheString.Add(tmp, val);
            }

            return tmp;
        }

        // Запись массива байт. key = 0 при добавлении и key != 0 при обновлении
        public long Write(long key, byte[] val)
        {
            if (val.Length == 0)
            {
                return 0;
            }

            var tmp = WriteBuffer(key, val);

            // Массив был обновлен
            if (tmp == key)
            {
                cacheByte[key] = val;
            }
            // Старый массив был удален
            else if (key != 0)
            {
                cacheByte.Remove(key);
                cacheByte.Add(tmp, val);
            }
            // Массив был добавлен впервые
            else
            {
                cacheByte.Add(tmp, val);
            }
            
            return tmp;
        }

        // Запись массива структур
        public long Write<T>(long key, T[] val) where T : struct 
        {
            if (val.Length == 0)
            {
                return 0;
            }           

            var buf = new byte[Buffer.ByteLength(val)];

            Buffer.BlockCopy(val, 0, buf, 0, buf.Length);

            var ret = WriteBuffer(key, buf);

            return ret;
        }

        #endregion

        #region ' Read '

        // Читаем строку
        public string ReadString(long key)
        {
            //if (key == DataBase.TestKey)
            //{
            //    int bp = 0;
            //}

            if (key == 0)
            {
                return "";
            }

            var tmp = cacheString[key];

            if (tmp != null)
            {
                return tmp;
            }
            
            var buf = ReadBuffer(key);

            //if (buf.Length > 1 && buf[1] == 0 && buf[0] == 0)
            //{
            //    Console.WriteLine("WARNING");
            //    Console.WriteLine("WARNING");
            //    Console.WriteLine("WARNING");
            //    Console.WriteLine("WARNING");
            //    Console.WriteLine("WARNING");
            //    Console.WriteLine("WARNING");
            //    Console.WriteLine("WARNING");
            //    Console.WriteLine("WARNING");
            //}

            tmp = System.Text.Encoding.Unicode.GetString(buf);

            cacheString[key] = tmp;

            return tmp;
        }

        // Читаем байтовый массив
        public byte[] ReadBytes(long key)
        {
            if (key == 0)
            {
                return new byte[0];
            }

            var tmp = cacheByte[key];

            if (tmp != null)
            {
                return tmp;
            }

            var buf = ReadBuffer(key);

            cacheByte[key] = buf;

            return buf;
        }

        // Читаем массив структур
        public T[] Read<T>(long key) where T : struct
        {
            if (key == 0)
            {
                return new T[0];
            }

            var buf = ReadBytes(key);
            var ret = new T[buf.Length / SizeOf<T>()];

            Buffer.BlockCopy(buf, 0, ret, 0, buf.Length);

            return ret;
        }

        #endregion

        #region ' IO '

        // Читаем буфер
        byte[] ReadBuffer(long key)
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

        // Записываем буфер
        internal long WriteBuffer(long key, byte[] val)
        {
            long pos;
            var ret = 0l;

            // Режим обновления
            if (key > 0)
            {
                var itm = list[key];

                // Обновление
                if (itm->Length == val.Length)
                {
                    ret = key;
                    pos = itm->Position;

                    goto write;
                }

                // Удаление старого массива
                list.Remove(key);
            }

            var len = val.Length;

            // Такая сложность связанна с необходимостью повторного использования удаленных или свободных записей в каталоге
            ret = list.CreateItem(len, out pos);

            if (pos == 0)
            {
                pos = CreateItem(len);

                var itm = list[ret];

                itm->Position = pos;
                itm->Length = len;
            }

            write:

            var w = GetWriter();

            if (w.Position != pos)
            {
                w.Position = pos;
            }

            w.Write(val, 0, val.Length);
            w.Flush();

            poolWriters.ReleaseInstance(w);

            //Debug.WriteLine("Write pos:" + pos + ", len:" + val.Length);

            return ret;
        }

        #endregion

        #region ' Internal API '

        // Удаление неиспользуемого массива
        internal void RemoveByteArray(long key)
        {
            cacheByte.Remove(key);

            list.Remove(key);
        }

        // Удаление неиспользуемой строки
        internal void RemoveString(long key)
        {
            cacheString.Remove(key);

            list.Remove(key);
        }

        #endregion

        #region ' Helper '

        // Очищение хранилища
        public void Clear()
        {
            vars.Length = 10;

            list.Clear();

            cacheByte.Clear();
            cacheString.Clear();
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

        // Создание нового элемента
        long CreateItem(int len)
        {
            try
            {
                locker.Lock();

                var c = vars.Length;

                vars.Length += len;

                return c;
            }
            finally
            {
                locker.Unlock();
            }
        }

        // Получение потока чтения
        FileStream GetReader()
        {
            var obj = poolReaders.GetInstance();

            if (obj == null)
            {
                obj = new FileStream(fullPath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
            }

            return obj;
        }

        // Получение потомка записи
        FileStream GetWriter()
        {
            var obj = poolWriters.GetInstance();

            if (obj == null)
            {
                obj = new FileStream(fullPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite); //FileOptions.RandomAccess
            }

            return obj;
        }

        // Запись на диск
        internal void FlushWriters()
        {
            var list = poolWriters.GetInstanceList();

            for (var i = 0; i < list.Length; ++i)
            {
                var itm = list[i];

                itm.Flush();
            }
        }
      
        // Количество записей
        public long CountMax => list.CountMax;

        #endregion

        #region ' Dispose '

        void CloseWriters()
        {
            if (poolWriters != null)
            {
                var list = poolWriters.GetInstanceList();

                for (var i = 0; i < list.Length; ++i)
                {
                    var itm = list[i];

                    itm.Close();
                }
            }
        }

        void CloseReaders()
        {
            if (poolReaders != null)
            {
                var list = poolReaders.GetInstanceList();

                for (var i = 0; i < list.Length; ++i)
                {
                    var itm = list[i];

                    itm.Close();
                }
            }
        }

        public void Dispose()
        {
            if (!disposed)
            {
                CloseWriters();
                CloseReaders();

                disposed = true;

                vars?.Dispose();
                list?.Dispose();
            }
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
