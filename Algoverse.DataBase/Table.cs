using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Algoverse.Threading;
using Algoverse.Threading.Collections;

namespace Algoverse.DataBase
{
    public abstract class Table<T> : IVerifiableTable, IEnumerable<T>, IRecordsGetter<T>, ITable where T : Record
    {
        int lastTimeToClearCash;
        DictionarySafe<int, T> ht;

        // 
        protected Table(DataBase dataBase, TableVersion current, string key)
        {
            Key = key;
            CurrentVersion = current;

            DataBase       = dataBase;
            ht             = new DictionarySafe<int, T>();
            File           = new PageFile(dataBase.DirectoryPath + Key + ".pgf", CurrentVersion.RecordSize, current.Mode);
            CashSizeLow    = 10000;
            CashSizeHight  = 20000;
            Indexes        = new List<object>();

            dataBase.Tables.Add(this);
            InitVersion(current);
        }
        
        // Текущая версия таблицы
        public TableVersion CurrentVersion { get; }
        
        // Размер записи в байтах. Размеры:
        // long    = 8
        // int     = 4
        // char    = 2
        // byte    = 1
        // string  = Lenght * 2 + 4 (например строка "ABSDEF" - 16 байт)
        public DataBase         DataBase        { get; }
        public PageFile         File            { get; }
        public List<object>     Indexes         { get; }
        public T                DefaultRecord   { get; set; }
        
        // Этот ключ является уникальным именем файла с расширением .pgf
        public string Key { get; }

        // Минимальное количество элементов в кеше
        public int CashSizeLow   { get; set; }
        
        // Максильное количество элементов в кеше. 
        // Преодолев этот лимит будет автоматически запущена процедура очищения кеша.
        public int CashSizeHight { get; set; }

        // Количество записей в таблице
        public int Count
        {
            get
            {
                return File.Count - File.deleted.Count;
            }
        }
        
        // Возвращает объект по коду
        public T this[int code]
        {
            get
            {
                if (code < 1 || !File.Contains(code))
                {
                    return DefaultRecord;
                }

                var obj = ht[code];

                if (obj == null)
                {
                    obj = ReadInernal(code);
                    obj = ht.GetOrAdd(code, obj);
                }

                obj.RatingTime = Environment.TickCount;

                if (ht.Count > CashSizeHight)
                {
                    BeginClearMemoryCash();
                }

                return obj;
            }
        }

        // Возвращает оригинальный обьект по коду
        public T GetOriginal(int code, DataIndexBase<T> indexBase)
        {
            var rec = this[code];

            if (rec == null)
            {
                return rec;
            }

            var obj = Create(); 

            obj.Code = code;
            obj.Data = rec.GetOrignalForRead(indexBase.Id);

            return obj;
        }
       
        // Функция резервирует место для объекта и выделяет ей код. После вызова этой функции, обязательно необходимо
        // сделать вызов Insert или Delete.
        public void Reserve(T obj)
        {
            // Создаем страницу для записи в БД
            obj.Code = File.Alloc();
            obj.RatingTime = Environment.TickCount;

            obj.Flag |= RecordFlag.Reserved;

            ht.Add(obj.Code, obj);
        }
        
        // Функция добавляет запись в БД
        public int Insert(T obj)
        {
            // Что бы избежать этой проверки возможно создать массив данных по умолчанию
            if (obj.Data == null)
            {
                throw new Exception("Data is null");
            }

            // Создаем страницу для записи в БД
            if (obj.Code == 0)
            {
                obj.Code = File.Alloc();
                obj.RatingTime = Environment.TickCount;
            }
            else if ((obj.Flag & RecordFlag.Reserved) == RecordFlag.Reserved)
            {
                obj.Flag ^= RecordFlag.Reserved;
            }

            // Запись объекта на носитель
            WriteInernal(obj);
            
            ht.Add(obj.Code, obj);

            // Удаляем оригинальные данные, поскольку при построении индексов они помеха. 
            var cha = obj.GetOrignForUpdate();

            if (cha != null)
            {
                obj.RemoveUpdatedOriginal();
            }

            // Добавление в индексы
            for (int i = 0; i < Indexes.Count; ++i)
            {
                var itm = Indexes[i] as DataIndexBase<T>;

                itm.Insert(obj);
            }

            Inserted(obj);

            return obj.Code;
        }

        // Функция обновляет запись в БД
        public void Update(T obj)
        {
            if (obj.Code < 1)
            {
                throw new Exception("The object has no Code. Maybe record is not exist in database.");
            }

            if (!File.Contains(obj.Code))
            {
                return;
            }

            var cha = obj.GetOrignForUpdate();

            if (cha == null)
            {
                // todo мы обязанны делать обновление записей если хотябы один из индексов связанн с другой таблицей, пока все это на костылях и это печально
                // верояно в будущем нужно научить базу, учитывать изменения в других таблицах
                var hasKostil = false;

                for (var i = 0; i < Indexes.Count; ++i)
                {
                    var ind = Indexes[i] as DataIndexBase<T>;

                    hasKostil = ind.UsedAnotherTable;

                    if (hasKostil)
                    {
                        break;
                    }
                }

                if (!hasKostil)
                {
                    return;
                }

                cha = new OriginalReord(obj.Code, obj.Data, new bool[CurrentVersion.Structure.Length]);
            }

            var old = Create();

            try
            {
                cha.IndexIsCurrent = new bool[Indexes.Count];

                //var old = Create();

                old.Code = cha.Code;
                old.Data = cha.Data;

                // Обновляем только те индексы, поля которых изменились.
                for (var i = 0; i < Indexes.Count; ++i)
                {
                    var ind = Indexes[i] as DataIndexBase<T>;

                    // todo Убрать этот костыль. Он появился в результате того что есть индексы, обновление которых зависит от других объектов...
                    if (ind.UsedAnotherTable)
                    {
                        ind.Update(old, obj);

                        continue;                        
                    }

                    for (var j = 0; j < ind.Fields.Length; ++j)
                    {
                        var fld = ind.Fields[j];

                        if (cha.Fields[fld.Id])
                        {
                            ind.Update(old, obj);

                            break;
                        }
                    }
                }
            }
            finally
            {
                WriteInernal(obj);

                Updated(old, obj);

                obj.RemoveUpdatedOriginal();
            }
        }
        
        // Функция удаляет запись из БД
        public void Delete(T obj)
        {
            if (!File.Contains(obj.Code))
            {
                return;
            }

            // Обновляем индекс только если эта запись не зарезервированна
            if ((obj.Flag & RecordFlag.Reserved) != RecordFlag.Reserved)
            {
                for (int i = 0; i < Indexes.Count; ++i)
                {
                    var itm = Indexes[i] as DataIndexBase<T>;

                    itm.Delete(obj);
                }
            }

            ht.Remove(obj.Code);

            File.Remove(obj.Code);
            
            for (var j = 0; j < CurrentVersion.Structure.Length; ++j)
            {
                var fld = CurrentVersion.Structure[j];

                if (fld.IsStorage) unsafe
                {
                    fixed (byte* ptr = &obj.Data[fld.Offset])
                    {
                        var key = (long*) ptr;

                        if (fld.Type == typeof (string))
                        {
                            fld.Storage.RemoveString(*key);
                        }
                        else
                        {
                            fld.Storage.RemoveByteArray(*key);
                        }
                    }

                    break;
                }
            }

            Deleted(obj);
        }

        // Функция удаляет запись из БД
        public void Delete(int code)
        {
            var obj = this[code];

            if (obj != null)
            {
                Delete(obj);
            }
        }

        // После добавления
        protected virtual void Inserted(T obj)
        {
        }

        // После удаления 
        protected virtual void Deleted(T obj)
        {
        }

        // После обновления
        protected virtual void Updated(T old, T obj)
        {
        }

        // Функция определяет существует ли объект с данным кодом.
        public bool Contains(int code)
        {
            return File.Contains(code);
        }
        
        // Асинхронная очистка кеша до количества CashSizeLow 
        public void BeginClearMemoryCash()
        {
            var now = Environment.TickCount;
            var time = now - lastTimeToClearCash;
            
            if (time > 1000)
            {
                lastTimeToClearCash = now;

                var caller = new Action(ClearMemoryCash);

                caller.BeginInvoke(null, null);
            }
        }

        // Очистка кеша до количества CashSizeLow 
        public void ClearMemoryCash()
        {
            //lock (ht)
            //{
                if (ht.Count < CashSizeLow)
                {
                    return;
                }

                var min = long.MaxValue;
                var max = long.MinValue;

                var delta = (double)CashSizeLow / ht.Count;

                var keys = ht.KeysToArray();

                // Нахождение минимума и максимума
                foreach (var key in keys)
                {
                    var it = ht[key];

                    if (it != null)
                    {
                        var time = it.RatingTime;

                        if (min > time) min = time;
                        if (max < time) max = time;
                    }
                }

                var mimad = (long)(max - (max - min) * delta);

                // Чистим
                foreach (var key in keys)
                {
                    var it = ht[key];

                    if (it != null)
                    {
                        var time = it.RatingTime;

                        if (time < mimad)
                        {
                            ht.Remove(key);
                        }
                    }
                }
                
                lastTimeToClearCash = Environment.TickCount;
            //}
        }
        
        // Полная очистка кеша
        public void ResetMemoryCash()
        {
            ht.Clear();
        }

        // Полная очистка таблицы TODO 123 Требуется оптимизация, удаление всех данных можно сделать по другому
        public void ClearTable()
        {
            var map = File.GetDeletedMap();

            for (int i = 1; i <= File.Count; ++i)
            {
                var code = i;

                if (!map.Contains(code))
                {
                    var obj = ReadInernal(code);

                    Delete(obj);
                }
            }

            //map.Clear();
            File.Clear();
        }

        // Полная очистка таблицы
        public void ClearTable(Func<T, bool> onRemove)
        {
            var map = File.GetDeletedMap();

            for (int i = 1; i <= File.Count; ++i)
            {
                var code = i;

                if (!map.Contains(code))
                {
                    var obj = ReadInernal(code);

                    if (onRemove(obj))
                    {
                        Delete(obj);
                    }
                }
            }
        }

        // Добавить индекс
        internal void AddIndex(DataIndexBase<T> indexBase)
        {
            indexBase.Id = Indexes.Count;
            Indexes.Add(indexBase);
        }
    
        // Чтение с носителя с учетом времени и контрольной суммы.
        // todo Если запись на диске повреждена, то будет ошибка в индексах, ошибки связей объектов. Подумать что с этим делать.
        public T ReadInernal(int code)
        {
            var rdr = File.GetReader();
            var obj = Create();

            long writeTime;

            obj.Code = code;

            if (CurrentVersion.Mode == PageFileIOMode.Standart)
            {
                obj.Data = rdr.Read(code, out writeTime);
            }
            else
            {
                int crc32;

                obj.Data = rdr.ReadWithCRC(code, out writeTime, out crc32);
            }

            obj.WriteTime = writeTime;

            File.ReleaseReader(rdr);

            return obj;
        }

        // Запись на носитель с учетом времени и контрольной суммы
        void WriteInernal(T record)
        {
            var wtr = File.GetWriter();

            if (CurrentVersion.Mode == PageFileIOMode.Standart)
            {
                wtr.Write(record.Code, record.Data);
            }
            else
            {
                wtr.WriteWithCRC(record.Code, record.Data);
            }

            File.ReleaseWriter(wtr);
        }
        
        // Инициализация версии
        void InitVersion(TableVersion version)
        {
            for (int i = 0; i < version.Structure.Length; ++i)
            {
                var str = version.Structure[i];

                if (str.Table != null)
                {
                    throw new Exception("The field " + str.Name + " already used by another table.");
                }

                str.Table = this;

                str.Storage = DataBase.Storage;
            }

            TableVersion.ht.Add(typeof(T), version.RecordSize);
        }

        // Создание пустой записи
        public abstract T Create();

        // Проверка
        public bool Check(Log log)
        {
            throw new NotImplementedException();
        }

        // Инициализация индексов
        public void InitIndexes(Log log)
        {
            //if (Key == "Entrys")
            //{
            //    int bp = 0;

            //    DataBase.Index.PrintRegistredIndexesToDebug();
            //}

            // Инициализация индексов
            for (int i = 0; i < Indexes.Count; ++i)
            {
                var itm = Indexes[i] as IDataIndexBase<T>;

                itm.RegisterIndex();

                log.Append("Index ");
                log.Append(itm.Name);
                log.Append(" registered.\r\n");
            }

            ht.Clear();
        }
        
        // Освобождение ресурсов
        public void Dispose()
        {
            File.Dispose();

            //for (int i = 0; i < Indexes.Count; ++i)
            //{
            //    Indexes[i].Dispose();
            //}
        }

        // Check indexes
        public bool CheckIndex(Log log)
        {
            var flug = false;

            log.Append("-------------------------------------------------------------\r\n");
            log.Append("Table ");
            log.Append(Key);
            log.Append(" has ");
            log.Append(Indexes.Count);
            log.Append(" indexes\r\n");
 
            for (int i = 0; i < Indexes.Count; ++i)
            {
                var itm = Indexes[i] as IVerifiableIndex;

                log.Append(i + 1);
                log.Append(". ");

                flug |= itm.Check(log);
            }

            return flug;
        }

        // Проверка таблицы
        public bool CheckTable(Log log)
        {
            log.Append("---------------------------------------\r\nBegin check data of table: ", Key, ", count: ", Count, "\r\n");

            var flag = false;

            if (CurrentVersion.Mode == PageFileIOMode.Standart)
            {
                log.Append("Check not supported.");

                return flag;
            }
                       
            var count = Count;
            var map = File.GetDeletedMap();
            var rdr = File.GetReader();
            var err = 0;
            var crc32 = new CRC32();

            for (int i = 1; i < count; ++i)
            {
                if (map.Contains(i))
                {
                    continue;
                }

                long time;
                int crc;

                var b = rdr.ReadWithCRC(i, out time, out crc);

                crc32.Update(b, 0, b.Length);
                crc32.Update(time);

                if (crc32.Value != crc)
                {
                    flag = true;
                    err++;

                    if (err < 10)
                    {
                        log.Append("Crc check fail at: " + i + "\r\n");
                    }
                }

                crc32.Reset();

                if (i > 0 && i % 100000 == 0)
                {
                    log.Append("Progresss : ", i, "\r\n");
                }
            }

            File.ReleaseReader(rdr);

            return flag;
        }

        // Получение записи по коду
        T IRecordsGetter<T>.GetRecord(int code)
        {
            return this[code];
        }

        // Полная перестройка индексов. Функция работает исходя из уверенности что файл индексов уже был очищен или пуст.
        public int RebuildIndex(Log log)
        {
            log.Append("Begin rebuild indexes for table: ", Key, "\r\n");
            var sw = Stopwatch.StartNew();

            var map = File.GetDeletedMap();
            var count = File.Count;

            for (int i = 1; i <= File.Count; ++i)
            {
                var code = i;

                if (Key == "Entrys" && i == 753)
                {
                    var bp = 0;
                }

                try
                {
                    if (!map.Contains(code))
                    {
                        var obj = ReadInernal(code);

                        obj.Code = code;

                        // Добавление в индексы
                        for (int n = 0; n < Indexes.Count; ++n)
                        {
                            var itm = Indexes[n] as DataIndexBase<T>;

                            itm.Insert(obj);
                        }
                    }
                }
                catch (Exception)
                {
                    log.Append("Rebuild error. code = " + code);

                    throw;
                }

                //if (i > 0 & i % 100000 == 0)
                //{
                //    log.Append("Progresss : ", i, "\r\n");
                //}

                if (sw.ElapsedMilliseconds >= 1000)
                {
                    log.Append("Progresss : ", i, ", ", (i * 100) / count,"% \r\n");

                    sw.Restart();
                }
            }

            return 0;
        }

        // Полная перестройка индексов. 
        public int RebuildIndex()
        {
            // Очистка индекса таблицы
            //for (int n = 0; n < Indexes.Count; ++n)
            //{
            //    var itm = Indexes[n] as DataIndexBase<T>;

            //    itm.Clear();
            //}
            
            // Построение индекса
            var map = File.GetDeletedMap();

            for (int i = 1; i <= File.Count; ++i)
            {
                var code = i;

                if (!map.Contains(code))
                {
                    var obj = ReadInernal(code);

                    obj.Code = code;

                    // Добавление в индексы
                    for (int n = 0; n < Indexes.Count; ++n)
                    {
                        var itm = Indexes[n] as DataIndexBase<T>;

                        itm.Insert(obj);
                    }
                }

            }

            return 0;
        }

        public void RegisterIndexes()
        {
            // Очистка индекса таблицы
            for (int n = 0; n < Indexes.Count; ++n)
            {
                var ind = Indexes[n] as DataIndexBase<T>;

                DataBase.Index.RegisterIndex(ind);
            }
        }

        #region ' Temp '

        // Таск обрабатывающий ошибку чтения с устройства
        void ReadFail(object record)
        {
            var rec = record as T;

            Debug.WriteLine("Read read fail. Table: " + Key + ", code: " + rec.Code);
        }

        //// Чтение из потока и запись в таблицу
        //int ReadFrom(BinaryReader br, RestoreTask task)
        //{
        //    var status = 0;

        //    //var dev = new ReaderBinary(br);

        //    //dev.ResetCRC();

        //    //var key = dev.ReadString();
        //    //var size = dev.ReadInt32();
        //    //var count = dev.ReadInt32();

        //    //task.RecordsCount = count;
        //    //task.Log.Append("Begin read table: ");
        //    //task.Log.Append(key);
        //    //task.Log.Append(", page size: ");
        //    //task.Log.Append(size);
        //    //task.Log.Append(", count: ");
        //    //task.Log.Append(count);
        //    //task.Log.Append("\r\n");

        //    //if (!dev.ReadCheckResetCRC())
        //    //{
        //    //    task.Log.Append("Error crc32 check of table metadata. Continue reading impossible!\r\n");
        //    //    task.HasError = true;

        //    //    return 2;
        //    //}

        //    //File.SetCount(count);

        //    //if (key != Key)
        //    //{
        //    //    task.Log.Append("Attention: The key of table was changed. It's possible data reading corrupt. But possible simple changed table signature.\r\n");
        //    //    task.HasError = true;

        //    //    return 2;
        //    //}

        //    //if (File.PageSize > size)
        //    //{
        //    //    task.Log.Append("Attention: The page size is more than stream. \r\n");
        //    //    task.HasError = true;

        //    //    return 2;
        //    //}

        //    //if (File.PageSize < size)
        //    //{
        //    //    task.Log.Append("Attention: The page size is less than stream.\r\n");
        //    //    task.HasError = true;

        //    //    return 2;
        //    //}
            
        //    ////if (Key == "Users")
        //    ////{
        //    ////    int bp = 0;
        //    ////}

        //    ////if (Key == "Counters")
        //    ////{
        //    ////    int bp = 0;
        //    ////}

        //    //var wtr = (WriterCRC32)File.GetWriter(1, count, DataBase.Storage);

        //    //for (int i = 0; i < count; ++i)
        //    //{
        //    //    dev.ResetCRC();

        //    //    var code = dev.ReadInt32();

        //    //    if (code < 0)
        //    //    {
        //    //        File.deleted.Push(-code);
        //    //        wtr.Position += File.PageSize;

        //    //        if (!dev.ReadCheckResetCRC())
        //    //        {
        //    //            task.HasError = true;
        //    //            task.Log.Append("Warning: The removed object with code: ", code, " crc32 check failed.\r\n");
        //    //        }
        //    //    }
        //    //    else
        //    //    {
        //    //        var obj = Read(dev);

        //    //        if (!dev.ReadCheckResetCRC())
        //    //        {
        //    //            task.HasError = true;
        //    //            task.Log.Append("Warning: The object in backup with code: ", code, " crc32 check failed.\r\n");
        //    //        }

        //    //        WriteInernal(wtr, obj);
        //    //    }

        //    //    if (i > 0 && i % 100000 == 0)
        //    //    {
        //    //        task.Log.Append("Progresss : ", i, "\r\n");
        //    //    }
        //    //}

        //    //File.ReleaseWriter(wtr);

        //    //File.Stream.Flush();

        //    //if (task.HasError)
        //    //{
        //    //    task.Log.Append("FAIL\r\n+++++++++++++++++++++++++++++++++++\r\n");
        //    //}
        //    //else
        //    //{
        //    //    task.Log.Append(" DONE\r\n+++++++++++++++++++++++++++++++++++\r\n");
        //    //}

        //    return status;
        //}

        //// Версия с паузой, на будущее если будет актуально
        //int ReadFrom2(BinaryReader br, RestoreTask task)
        //{
        //    var status = 0;

        //    //var dev = new ReaderBinary(br);

        //    //dev.ResetCRC();

        //    //if (task.RecordIndex == 0)
        //    //{
        //    //    var key = dev.ReadString();
        //    //    var size = dev.ReadInt32();
        //    //    var count = dev.ReadInt32();

        //    //    task.RecordsCount = count;
        //    //    task.Log.Append("Begin read table: ");
        //    //    task.Log.Append(key);
        //    //    task.Log.Append(", page size: ");
        //    //    task.Log.Append(size);
        //    //    task.Log.Append(", count: ");
        //    //    task.Log.Append(count);
        //    //    task.Log.Append("\r\n");

        //    //    if (!dev.ReadCheckResetCRC())
        //    //    {
        //    //        throw new Exception("Error crc32 check of table metadata. Continue reading impossible!\r\n");
        //    //    }

        //    //    File.SetCount(count);

        //    //    if (key != Key)
        //    //    {
        //    //        throw new Exception("Attention: The key of table was changed. It's possible data reading corrupt. But possible simple changed table signature.\r\n");
        //    //    }

        //    //    if (File.PageSize > size)
        //    //    {
        //    //        throw new Exception("Attention: The page size is more than stream. \r\n");
        //    //    }

        //    //    if (File.PageSize < size)
        //    //    {
        //    //        throw new Exception("Attention: The page size is less than stream.\r\n");
        //    //    }
        //    //}

        //    //var sizeStep = 1000;

        //    //for (int i = task.RecordIndex; i < task.RecordsCount; i += sizeStep)
        //    //{
        //    //    var c = i + sizeStep < task.RecordsCount ? sizeStep : task.RecordsCount - i;
        //    //    var wtr = (PageWriter)File.GetWriter(i, c + 1, DataBase.Storage);

        //    //    task.RecordIndex = i + c;

        //    //    for (int j = 0; j < c; ++j)
        //    //    {
        //    //        dev.ResetCRC();

        //    //        var code = dev.ReadInt32();

        //    //        if (code != i + j)
        //    //        {
        //    //            throw new Exception("Error synchronizing data streams to read and write. Continue reading impossible!\r\n");
        //    //        }

        //    //        if (code == 0)
        //    //        {

        //    //            dev.ReadCheckResetCRC();
        //    //        }
        //    //        else if (code < 0)
        //    //        {
        //    //            File.deleted.Push(-code);

        //    //            if (!dev.ReadCheckResetCRC())
        //    //            {
        //    //                task.HasError = true;
        //    //                task.Log.Append("Warning: The removed object with code: \r\n");
        //    //                task.Log.Append(code);
        //    //                task.Log.Append(" crc32 check failed.");
        //    //            }
        //    //        }
        //    //        else
        //    //        {
        //    //            var obj = Read(dev);

        //    //            if (!dev.ReadCheckResetCRC())
        //    //            {
        //    //                task.HasError = true;
        //    //                task.Log.Append("\r\nWarning: The object with code: ", code);
        //    //                task.Log.Append();
        //    //                task.Log.Append(" crc32 check failed.");
        //    //            }

        //    //            WriteInernal(wtr, obj);
        //    //        }
        //    //    }

        //    //    wtr.Dispose();

        //    //    if (i > 0 & i % (sizeStep * 100) == 0)
        //    //    {
        //    //        task.Log.Append("Progresss : ");
        //    //        task.Log.Append(i);
        //    //        task.Log.Append("\r\n");
        //    //        task.Log.Save();
        //    //    }

        //    //    if (DataBase.IsClosing)
        //    //    {
        //    //        task.ReaderPosition = br.BaseStream.Position;

        //    //        return 1;
        //    //    }
        //    //}


        //    //if (task.HasError)
        //    //{
        //    //    task.Log.Append("FAIL\r\n+++++++++++++++++++++++++++++++++++\r\n");
        //    //}
        //    //else
        //    //{
        //    //    task.Log.Append(" DONE\r\n+++++++++++++++++++++++++++++++++++\r\n");
        //    //}

        //    return status;
        //}

        // Для теста
        bool checkOne(int code)
        {
            //var rdr = File.GetReader(code, 1, DataBase.Storage);

            //var obj = ReadInernal(rdr);

            //File.ReleaseReader((ReaderCRC32)rdr);

            return true;
        }
        
        // Копирование всей таблицы в поток
        bool WriteTo(BinaryWriter bw, Log log)
        {
            //log.Append("Begin write table: ");
            //log.Append(Key);
            //log.Append(", page size: ");
            //log.Append(File.PageSize);
            //log.Append(", count: ");
            //log.Append(File.Count);
            //log.Append(", status: ");
            //log.Append("\r\n");

            //try
            //{
            //    var map = File.GetDeletedMap();
            //    var dev = new BinaryWriterCRC32(bw);

            //    dev.Write(Key);
            //    dev.Write(File.PageSize);
            //    dev.Write(File.Count);
            //    dev.WriteAndResetCRC();


            //    var rdr = File.GetReader(1, File.Count, DataBase.Storage);

            //    for (int i = 1; i <= File.Count; ++i)
            //    {
            //        var code = i;

            //        if (map.Contains(code))
            //        {
            //            dev.Write(-code);
            //            dev.WriteAndResetCRC();

            //            rdr.Position += File.PageSize;
            //        }
            //        else
            //        {
            //            var obj = ReadInernal(rdr);

            //            dev.Write(code);
            //            //Write(dev, obj);
            //            dev.WriteAndResetCRC();
            //        }

            //        if (i > 0 && i % 100000 == 0)
            //        {
            //            log.Append("Progresss : ", i, "\r\n");
            //        }
            //    }

            //    File.ReleaseReader((ReaderCRC32)rdr);
            //}
            //catch (Exception e)
            //{
            //    log.Append(e);
            //    log.Append("FAIL\r\n+++++++++++++++++++++++++++++++++++\r\n");

            //    return false;
            //}

            //log.Append("DONE\r\n+++++++++++++++++++++++++++++++++++\r\n");

            return true;
        }

        //// Function for many inserts
        ////public int Insert(T obj, IWriter manyWriter)
        ////{
        ////    var w = (PageWriter) manyWriter;

        ////    obj.Code = w.GetCurrentCode();

        ////    WriteInernal(manyWriter, obj);

        ////    // Занесение в кэш
        ////    if (!ht.ContainsKey(obj.Code))
        ////    {
        ////        ht.TryAdd(obj.Code, new CashItem(obj));
        ////    }
        ////    else
        ////    {
        ////        ht[obj.Code] = new CashItem(obj);
        ////    }

        ////    // Добавление в индексы
        ////    for (int i = 0; i < Indexes.Count; ++i)
        ////    {
        ////        var itm = Indexes[i] as IDataIndex<T>;

        ////        itm.Insert(obj);
        ////    }

        ////    return obj.Code;
        ////}

        ////
        //public T GetUpdateObject(int code)
        //{
        //    //if (code == 0 || !File.Contains(code))
        //    //{
        //    //    return DefaultRecord;
        //    //}

        //    //var reader = File.GetReader(code, DataBase.Storage);
        //    //var obj = ReadInernal(reader);

        //    //File.ReleaseReader((ReaderCRC32)reader);

        //    //obj.Code = code;

        //    //return obj;

        //    return null;
        //}

        ////
        //public void CompleteUpdate(T obj)
        //{
        //    //var old = this[obj.Code];

        //    //for (int i = 0; i < Indexes.Count; ++i)
        //    //{
        //    //    var itm = Indexes[i] as ;

        //    //    itm.Update(old, obj);
        //    //}

        //    //CashItem tmp;
        //    //CashItem cur = new CashItem(obj);

        //    //if (ht.TryGetValue(obj.Code, out tmp))
        //    //{
        //    //    if (!ht.TryUpdate(obj.Code, cur, tmp))
        //    //    {
        //    //        ht.TryAdd(obj.Code, cur);
        //    //    }
        //    //}
        //    //else
        //    //{
        //    //    ht.TryAdd(obj.Code, cur);
        //    //}

        //    //var w = (WriterCRC32)File.GetWriter(obj.Code, DataBase.Storage);

        //    //WriteInernal(w, obj);
        //    //File.ReleaseWriter(w);

        //    //w.Dispose();
        //}

        #endregion

        #region ' IEnumerator '

        public IEnumerator<T> GetEnumerator()
        {
            return new TableEnumerator(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new TableEnumerator(this);
        }

        public class TableEnumerator: IEnumerator<T>
        {
            readonly Table<T> table;
            HashSet<int> map;
            int curCode;

            public TableEnumerator(Table<T> table)
            {
                this.table = table;
                this.map = table.File.GetDeletedMap();

                curCode = 1;
            }

            public void Dispose()
            {
            }

            public bool MoveNext()
            {
                while (curCode <= table.Count)
                {
                    if (!map.Contains(curCode))
                    {
                        Current = table[curCode];

                        if (Current != null)
                        {
                            return true;
                        }
                    }

                    curCode++;
                }

                return false;
            }

            public void Reset()
            {
                curCode = 1;
                this.map = table.File.GetDeletedMap();
            }

            public T Current { get; private set; }

            object IEnumerator.Current
            {
                get
                {
                    return Current;
                }
            }
        }

        #endregion
    }

    // Класс содержит инфу об изменениях
    class OriginalReord
    {
        public int Code;
        public int Lock;
        public byte[] Data;
        public bool[] Fields;
        // При получении оринальной записи показывает индексу, что для него актуальными данными является текущие данные а не оригинал.
        public bool[] IndexIsCurrent;

        public OriginalReord(int code, byte[] data, bool[] fields)
        {
            Fields = fields;
            Code = code;
            Lock = 0;
            Data = data;
        }
    }
}

// TODO Возможно необходимо сделать контроль на двойной инсерт, 
// что бы избежать возможность обновления записи в обход метода Update

// TODO Если запись создать записать в поле строку и не добавить в таблицу то будет утечка памяти в куче. Нужно мониторить уничтожение объектов с кодом 0

// Индекс строится по запросу
// 
// 
// 
// 
// 
// 
// 
// 
// 
// 
// 
// 
// 
// 
// 
// 
// 
// 
// 
