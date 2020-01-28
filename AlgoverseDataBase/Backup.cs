using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;

namespace Algoverse.DataBase
{
    public unsafe class Backup
    {
        const int size_crc = 4;
        const int size_time = 8;
        const int size_time_crc = 12;

        public DataBase DB { get; }

        public Backup(DataBase db)
        {
            DB = db;
        }
        
        // Функция резервирует базу данных по указанному пути. Таблицы должны распологаться в таком порядке, что бы первыми шли таблицы которые не ссылаются на другие.
        public bool Write(string path, Log log)
        {
            var flag = true;

            FileStream stream_meta = null;
            FileStream stream_data = null;
            
            // Стартовая позиция метаданных таблиц
            var meta_start = 0l;

            // Размер метаданных
            var meta_size = 0;

            try
            {
                var dt = DateTime.Now;

                log.Append("Backup begin at ");
                log.Append(dt.ToString("F"));
                log.Append("\r\n");

                stream_meta = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                stream_data = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);

                var sm = new BinaryWriterCRC32(stream_meta);
                var sd = new BinaryWriter(stream_data);
                var counts = new int[DB.Tables.Count];
                var version = Assembly.GetCallingAssembly().GetName().Version;

                // Запись типа бекапа
                sm.Write(1);
                // Запись времени бекапа
                sm.Write(dt.Ticks);
                // Запись версии базы
                sm.Write(DB.Version);
                // Запись версии кода
                sm.Write(version.Major);
                sm.Write(version.Minor);
                sm.Write(version.Build);
                sm.Write(version.Revision);
                // Запись количества таблиц
                sm.Write(DB.Tables.Count);

                // Оставляем шапку 1024 байта
                stream_meta.Position = 1024;

                // crc шапки
                sm.WriteAndResetCRC();

                // Записываем структуру таблиц
                for (int i = 0; i < DB.Tables.Count; ++i)
                {
                    var itm = DB.Tables[i];

                    // Количество
                    sm.Write(itm.File.Count);
                    // Позиция в файле
                    sm.Write(meta_size);
                    // Название таблицы
                    sm.Write(itm.Key);
                    // Версия таблицы
                    sm.Write(itm.CurrentVersion.Version);
                    // Размер таблицы
                    sm.Write(itm.CurrentVersion.RecordSize);
                    // Режим записи
                    sm.Write((int)itm.CurrentVersion.Mode);
                    // Массивы произвольной длинны
                    sm.Write(itm.CurrentVersion.HasStorage);
                    // Количество полей
                    sm.Write(itm.CurrentVersion.Structure.Length);
                    // Поля
                    for (var j = 0; j < itm.CurrentVersion.Structure.Length; ++j)
                    {
                        var fld = itm.CurrentVersion.Structure[j];

                        sm.Write(fld.IsStorage);
                        sm.Write(fld.Name);
                        sm.Write(fld.Size);
                        sm.Write(fld.Offset);
                    }

                    // Размер мета
                    meta_size += itm.File.Count * 8 + 4;
                    // CRC записи
                    sm.WriteAndResetCRC();

                    counts[i] = itm.File.Count;
                }

                // Инициализируем место для метаданых
                meta_start = stream_meta.Position;
                stream_data.Position = meta_start + meta_size;

                // Записываем даные
                for (int i = 0; i < DB.Tables.Count; ++i)
                {
                    var itm = DB.Tables[i];

                    flag &= WriteTable(itm, sm, sd, counts[i], log);

                    // CRC метаданных таблицы
                    sm.WriteAndResetCRC();
                }
            }
            catch (Exception e)
            {
                flag = false;

                log.Append(e);
            }
            finally
            {
                //var wtf = stream_data.Position;
                //stream_data.Position = Backup.TestPosition;
                //var br = new BinaryReader(stream_data);
                //var ppc = br.ReadInt32();
                //stream_data.Position = wtf;
                //Debug.WriteLine("Final Control: " + ppc);
               
                if (stream_meta != null)
                {
                    if (stream_meta.Position != meta_start + meta_size)
                    {
                        flag = false;

                        log.Append("Stream metada is oversized.");
                    }


                    stream_meta.Close();
                }

                if (stream_data != null)
                {
                    stream_data.Close();
                }
            }

            log.Append("\r\n");

            if (flag)
            {
                log.Append("Backup DONE");
            }
            else
            {
                log.Append("Backup FAIL");
            }

            return flag;
        }

        // Функция копирует данные таблицы в бекап
        bool WriteTable(ITable itm, BinaryWriterCRC32 sm, BinaryWriter sd, int count, Log log)
        {
            log.Append("Begin read table: ");
            log.Append(itm.Key);
            log.Append(", count: ");
            log.Append(count);
            log.Append("\r\n");

            var crc = new CRC32();
            var crcerr = 0;
            var crcused = (itm.CurrentVersion.Mode & PageFileIOMode.CRC32) == PageFileIOMode.CRC32;
            var fs = itm.File.CreateStream();
            var size = itm.CurrentVersion.RecordSize;
            var buf = new byte[size];
            var map = itm.File.GetDeletedMap();
            var ver = itm.CurrentVersion;
            var delta = size_time;
            var flag = true;

            if (crcused)
            {
                delta = size_time_crc;
            }

            fs.Position = PageFile.HeaderSize + size;

            for (int i = 0; i < count; ++i)
            {
                // Записываем позицию метаданных
                sm.Write(sd.BaseStream.Position);

                //Debug.WriteLine("Backup " + itm.Key + ", code: " + (i + 1) + ", offset:" + sd.BaseStream.Position);

                fs.Read(buf, 0, size);

                if (i + 1 == 1350685)
                {
                    var bp = 0;
                }

                var time = 0l;
                var fact = 0l;

                // Читаем время и crc
                fixed (byte* ptr = &buf[size - delta])
                {
                    time = *(long*)ptr;

                    if (crcused)
                    {
                        fact = *(int*)(ptr + size_time);
                    }
                }

                // Пишем время
                if (map.Contains(i + 1))
                {
                    if (time == 0)
                    {
                        time = DateTime.Now.Ticks;
                    }

                    // если запись удалена
                    sd.Write(-time);

                    crc.Reset();

                    continue;
                }
                else
                {
                    // если запись нормальная
                    sd.Write(time);
                }

                // считаем crc данных и времени
                crc.Update(buf, 0, size - delta);
                crc.Update(time);

                // Быстрая проверка без данных переменной длинны
                if (fact != crc.Value)
                {
                    flag = false;

                    crcerr++;

                    if (crcerr < 10)
                    {
                        log.Append("Warning reading: The object with code: ", i + 1, " crc32 check failed.\r\n");
                    }
                }

                crc.Reset();
                crc.Update(time);

                // Массивы произвольной длинны
                if (ver.HasStorage)
                {
                    for (var j = 0; j < ver.Structure.Length; ++j)
                    {
                        var fld = ver.Structure[j];

                        if (fld.IsStorage) 
                        {
                            //Debug.WriteLine("Backup " + itm.Key + ", code: " + (i + 1) + ", field: " + fld.Name + " pos: " + sd.BaseStream.Position);

                            fixed (byte* ptr = &buf[fld.Offset])
                            {
                                var tmp = fld.Storage.ReadBytes(*((long*)ptr));

                                sd.Write(tmp.Length);
                                sd.BaseStream.Write(tmp, 0, tmp.Length);

                                crc.Update(tmp.Length);
                                crc.Update(tmp, 0, tmp.Length);
                            }
                        }
                        else
                        {
                            switch (fld.Size)
                            {
                                case 1:
                                {
                                    sd.Write(buf[fld.Offset]);
                                    crc.Update(buf[fld.Offset]);

                                    break;
                                }
                                case 2:
                                {
                                    fixed (byte* ptr = &buf[fld.Offset])
                                    {
                                        var tmp = (char*)ptr;

                                        sd.Write(*tmp);
                                        crc.Update(*tmp);
                                    }

                                    break;
                                }
                                case 4:
                                {
                                    fixed (byte* ptr = &buf[fld.Offset])
                                    {
                                        var tmp = (uint*)ptr;

                                        sd.Write(*tmp);
                                        crc.Update(*tmp);
                                    }

                                    break;
                                }
                                case 8:
                                {
                                    fixed (byte* ptr = &buf[fld.Offset])
                                    {
                                        var tmp = (ulong*)ptr;

                                        sd.Write(*tmp);
                                        crc.Update(*tmp);
                                    }

                                    break;
                                }
                                case 16:
                                {
                                    fixed (byte* ptr = &buf[fld.Offset])
                                    {
                                        var tmp = (decimal*)ptr;

                                        sd.Write(*tmp);
                                        crc.Update(*tmp);
                                    }

                                    break;
                                }
                            }
                        }
                    }

                    // Пишем CRC
                    sd.Write(crc.Value);
                }
                else
                {
                    // crc данных
                    crc.Update(buf, 0, size - delta);

                    // Пишем данные
                    sd.BaseStream.Write(buf, 0, size - delta);
                    // Пишем CRC
                    sd.Write(crc.Value);
                }

                crc.Reset();

                if (i > 0 && i % 100000 == 0)
                {
                    log.Append("Progresss : ", i, "\r\n");
                }
            }

            fs.Close();

            return flag;
        }
 
        public static long TestPosition { get; set; }

        // Функция восстанавливает базу из бекапа
        public bool Restore(string path, Log log)
        {
            var flag = true;
            var not_critical = true;

            FileStream stream_meta = null;
            FileStream stream_data = null;

            try
            {
                var dt = DateTime.Now;

                log.Append("Restore begin at ");
                log.Append(dt.ToString("F"));
                log.Append("\r\n");

                stream_meta = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                stream_data = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);

                var sm = new BinaryReaderCRC32(stream_meta);
                var db = new MetaDataBase(sm);

                if (!db.IsValid)
                {
                    log.Append("Backup header corrupt. CRC32 Fail.");

                    return false;
                }

                // Очищаем хранилище массивов
                DB.Storage.Clear();

                // Восстановление таблиц
                for (var i = 0; i < db.Count; ++i)
                {
                    var itm = db[i];

                    if (itm.IsValid)
                    {
                        var tbl = DB.GetTableByKey(itm.Key);

                        if (tbl == null)
                        {
                            log.AppendLine("Table " + itm.Key + " not found in current version database.");

                            continue;
                        }

                        log.AppendLine("Restore table " + tbl.Key);

                        var tf = itm.RestoreTable(tbl, DB.Storage, sm, stream_data, log);

                        log.AppendLine(tf ? " Done" : " Fail");

                        flag &= tf;
                    }
                    else
                    {
                        var key = itm.Key.Length < 256 ? itm.Key : itm.Key.Substring(0, 256);
                            
                        log.Append("Table " + key + " header corrupt. CRC32 Fail.");
                    }
                }
            }
            catch (Exception e)
            {
                flag = false;
                not_critical = false;

                log.Append(e);
            }
            finally
            {
                if (stream_meta != null)
                {
                    stream_meta.Flush();
                    stream_meta.Close();
                }

                if (stream_data != null)
                {
                    stream_data.Flush();
                    stream_data.Close();
                }
            }

            log.Append("\r\n");

            if (flag)
            {
                log.Append("Restore DONE\r\n");
            }
            else
            {
                log.Append("Restore FAIL\r\n");
            }

            return not_critical && flag;
        }
        
        // Для работы с метаданными базы бекапа
        public class MetaDataBase
        {
            public MetaDataBase(BinaryReaderCRC32 rdr)
            {
                Type = rdr.ReadInt32();
                Time = new DateTime(rdr.ReadInt64());
                Version = rdr.ReadInt32();
                Major = rdr.ReadInt32();
                Minor = rdr.ReadInt32();
                Build = rdr.ReadInt32();
                Revision = rdr.ReadInt32();
                Count = rdr.ReadInt32();

                rdr.Stream.Position = 1024;

                IsValid = rdr.ReadCheckResetCRC();

                var version = Assembly.GetCallingAssembly().GetName().Version;

                if (version.Major != Major || version.Minor != Minor || version.Build != Build || version.Revision != Revision)
                {
                    IsCodeEqual = false;
                }
                else
                {
                    IsCodeEqual = true;
                }

                if (IsValid)
                {
                    list = new List<MetaTable>(Count);

                    for (var i = 0; i < Count; ++i)
                    {
                        list.Add(new MetaTable(rdr));
                    }

                    // Конвертируем относительные адреса таблиц записей в абсолютные адреса.
                    for (var i = 0; i < Count; ++i)
                    {
                        list[i].Offset += rdr.Stream.Position;
                    }
                }
            }

            List<MetaTable> list;

            // Тип бекапа
            public int Type { get; }
            // Время создания бекапа
            public DateTime Time { get; }
            // Версия данных
            public int Version { get; }
            // Версия кода 
            public int Major { get; }
            public int Minor { get; }
            public int Build { get; }
            public int Revision { get; }
            // Количество таблиц
            public int Count { get; }
            // Если версия кода таже
            public bool IsCodeEqual { get; }
            // Если заголовок бекапа не поврежден
            public bool IsValid { get; }

            // Возвращает описание таблицы
            public MetaTable this[int index]
            {
                get
                {
                    return list[index];
                }
            }
        }

        // Для работы с метаданными таблиц
        public class MetaTable
        {
            public MetaTable(BinaryReaderCRC32 rdr)
            {
                Count = rdr.ReadInt32();
                Offset = rdr.ReadInt32();
                Key = rdr.ReadString();
                Version = rdr.ReadInt32();
                RecordSize = rdr.ReadInt32();
                Mode = (PageFileIOMode)rdr.ReadInt32();
                HasStorage = rdr.ReadBoolean();

                var c = rdr.ReadInt32();

                Fields = new List<MetaField>();

                for (var j = 0; j < c; ++j)
                {
                    Fields.Add(new MetaField(rdr));
                }

                IsValid = rdr.ReadCheckResetCRC();
            }

            // Количество записей
            public int Count { get; }
            // Позиция метаданных
            public long Offset { get; internal set; }
            // Название таблицы
            public string Key { get; }
            // Версия данных таблицы
            public int Version { get; }
            // Размер одной записи
            public int RecordSize { get; }
            // Режим записи
            public PageFileIOMode Mode { get; }
            // Массивы произвольной длинны
            public bool HasStorage{ get; }
            // Если описание таблицы не повреждено
            public bool IsValid { get; }
            // Список полей
            public List<MetaField> Fields;

            // Восстановление таблицы из бекапа
            internal bool RestoreTable(ITable itm, ArrayStorage storage, BinaryReaderCRC32 sm, FileStream sd, Log log)
            {
                if (sm.Stream.Position != Offset)
                {
                    sm.Stream.Position = Offset;
                }

                //Debug.WriteLine("Restore " + itm.Key);

                itm.File.SetCount(Count);

                var flag = true;
                var wtr = itm.File.CreateStream();
                var sd_br = new BinaryReader(sd);
                var crc = new CRC32();
                var crcerr = 0;
                var hasCRC = (itm.CurrentVersion.Mode & PageFileIOMode.CRC32) == PageFileIOMode.CRC32;
                var size = RecordSize;

                if (!hasCRC)
                {
                    size += size_crc;
                }

                var buf = new byte[size];
                var list = new List<byte[]>();

                try
                {

                    if (itm.Key == "Users")
                    {
                        int bp = 0;
                    }

                    var sw = Stopwatch.StartNew();

                    //if (itm.Key == "Regions")
                    //{
                    //    int bp = 0;
                    //}

                    // Если есть массивыв переменной длинны
                    if (HasStorage)
                    {
                        // i + 1 - это код
                        for (var i = 0; i < Count; ++i)
                        {
                            var offset = sm.ReadInt64();

                            //Debug.WriteLine("Restore " + itm.Key + ", code: " + (i + 1) + ", offset:" + offset);

                            if (i + 1 == 1350685)
                            {
                                var bp = 0;
                            }

                            if (sd.Position != offset)
                            {
                                sd.Position = offset;
                            }

                            var time = sd_br.ReadInt64();

                            // Проверяем на удаление
                            if (time < 0)
                            {
                                itm.File.deleted.Push(i + 1);

                                continue;
                            }
                            else
                            {
                                // обновляем crc времени
                                crc.Update(time);
                            }

                            list.Clear();

                            // Считаем CRC
                            for (var j = 0; j < Fields.Count; ++j)
                            {
                                var fld = Fields[j];

                                if (fld.IsStorage)
                                {
                                    //Debug.WriteLine("Restore " + itm.Key + ", code: " + (i + 1) + ", field: " + fld.Name + " pos: " + sd.Position);

                                    var len = sd_br.ReadInt32();
                                    var tmp = new byte[len];

                                    sd.Read(tmp, 0, len);

                                    crc.Update(len);
                                    crc.Update(tmp, 0, tmp.Length);

                                    list.Add(tmp);
                                }
                                else
                                {
                                    var off = fld.Offset;

                                    switch (fld.Size)
                                    {
                                        case 1:
                                        {
                                            var val = sd_br.ReadByte();

                                            buf[off] = val;

                                            crc.Update(val);

                                            break;
                                        }
                                        case 2:
                                        {
                                            var val = sd_br.ReadChar();

                                            fixed (byte* ptr = &buf[off])
                                            {
                                                var tmp = (char*) ptr;

                                                *tmp = val;
                                            }

                                            crc.Update(val);

                                            break;
                                        }
                                        case 4:
                                        {
                                            var val = sd_br.ReadUInt32();

                                            fixed (byte* ptr = &buf[off])
                                            {
                                                var tmp = (uint*) ptr;

                                                *tmp = val;
                                            }

                                            crc.Update(val);

                                            break;
                                        }
                                        case 8:
                                        {
                                            var val = sd_br.ReadUInt64();

                                            fixed (byte* ptr = &buf[off])
                                            {
                                                var tmp = (ulong*) ptr;

                                                *tmp = val;
                                            }

                                            crc.Update(val);

                                            break;
                                        }
                                        case 16:
                                        {
                                            var val = sd_br.ReadDecimal();

                                            fixed (byte* ptr = &buf[off])
                                            {
                                                var tmp = (decimal*) ptr;

                                                *tmp = val;
                                            }

                                            crc.Update(val);

                                            break;
                                        }
                                    }
                                }
                            }

                            // Проверяем crc массивов
                            var crc2 = sd_br.ReadInt32();

                            if (crc.Value != crc2)
                            {
                                flag = false;

                                crc.Reset();

                                crcerr++;

                                if (crcerr < 10)
                                {
                                    log.Append("Record code: " + (i + 1) + " error crc32 array check.\r\n");
                                }

                                continue;
                            }

                            // Записываем массивы
                            var n = 0;

                            for (var j = 0; j < Fields.Count; ++j)
                            {
                                var fld = Fields[j];

                                if (fld.IsStorage)
                                {
                                    var tmp = list[n++];
                                    var key = storage.WriteBuffer(0, tmp);

                                    fixed (byte* b = &buf[fld.Offset])
                                    {
                                        var ptr = (long*) b;

                                        *ptr = key;
                                    }

                                    //if (itm.Key == "Users" && i == 0 && fld.Name == "Awatar")
                                    //{
                                    //    DataBase.TestKey = key;
                                    //    DataBase.TestString = System.Text.Encoding.Unicode.GetString(tmp);
                                    //}
                                }
                            }

                            var wtr_offset = PageFile.HeaderSize + (i + 1)*itm.CurrentVersion.RecordSize;

                            //Debug.WriteLine("Write pos: " + wtr_offset);

                            // Устанавливаем позицию
                            if (wtr.Position != wtr_offset)
                            {
                                wtr.Position = wtr_offset;
                            }

                            // Записываем запись
                            if (hasCRC)
                            {
                                crc.Reset();

                                fixed (byte* ptr = &buf[size - size_time_crc])
                                {
                                    var tmp = (long*) ptr;

                                    *tmp = time;
                                }

                                crc.Update(buf, 0, size - size_crc);

                                fixed (byte* ptr = &buf[size - size_crc])
                                {
                                    var tmp = (int*) ptr;

                                    *tmp = crc.Value;
                                }

                                // Данные, время и CRC
                                wtr.Write(buf, 0, size);
                            }
                            else
                            {
                                fixed (byte* ptr = &buf[size - size_time_crc])
                                {
                                    var tmp = (long*) ptr;

                                    *tmp = time;
                                }

                                // Данные и время
                                wtr.Write(buf, size_time, size - size_crc);
                            }

                            crc.Reset();

                            //if (i > 0 && i%100000 == 0)
                            //{
                            //    log.Append("Progresss : ", i, "\r\n");
                            //}
                            if (sw.ElapsedMilliseconds >= 1000)
                            {
                                log.Append("Progresss : ", i, ", ", (i * 100) / Count, "% \r\n");

                                sw.Restart();
                            }
                        }
                    }
                    else
                    {
                        for (var i = 0; i < Count; ++i)
                        {
                            if (i + 1 == 1350685)
                            {
                                var bp = 0;
                            }

                            var offset = sm.ReadInt64();

                            //Debug.WriteLine("Restore " + itm.Key + ", code: " + (i + 1) + ", offset:" + offset);

                            if (sd.Position != offset)
                            {
                                sd.Position = offset;
                            }

                            // Читаем запись
                            sd.Read(buf, 0, size);

                            // Проверяем на удаление
                            fixed (byte* b = &buf[0])
                            {
                                var ptr = (long*) b;

                                if (*ptr < 0)
                                {
                                    itm.File.deleted.Push(i + 1);

                                    continue;
                                }
                                else
                                {
                                    // обновляем crc времени
                                    crc.Update(*ptr);
                                }
                            }

                            // обновляем crc данных
                            crc.Update(buf, size_time, size - size_time_crc);

                            // Проверяем crc данных
                            fixed (byte* b = &buf[size - size_crc])
                            {
                                var ptr = (int*) b;

                                if (crc.Value != *ptr)
                                {
                                    flag = false;

                                    crc.Reset();

                                    crcerr++;

                                    if (crcerr < 10)
                                    {
                                        log.Append("Record code: " + (i + 1) + " error crc32 data check.\r\n");
                                    }

                                    continue;
                                }
                            }

                            var wtr_offset = PageFile.HeaderSize + (i + 1)*itm.CurrentVersion.RecordSize;

                            // Устанавливаем позицию записи
                            if (wtr.Position != wtr_offset)
                            {
                                wtr.Position = wtr_offset;
                            }

                            // Записываем запись
                            if (hasCRC)
                            {
                                // Пересчитываем crc под формат бд
                                crc.Reset();
                                crc.Update(buf, size_time, size - size_time_crc);
                                crc.Update(buf, 0, size_time);

                                fixed (byte* ptr = &buf[size - size_crc])
                                {
                                    var tmp = (int*) ptr;

                                    *tmp = crc.Value;
                                }

                                // Данные
                                wtr.Write(buf, size_time, size - size_time_crc);
                                // Время
                                wtr.Write(buf, 0, size_time);
                                // CRC
                                wtr.Write(buf, size - size_crc, size_crc);
                            }
                            else
                            {
                                // Данные
                                wtr.Write(buf, size_time, size - size_time_crc);
                                // Время
                                wtr.Write(buf, 0, size_time);
                            }

                            crc.Reset();

                            //if (i > 0 && i%100000 == 0)
                            //{
                            //    log.Append("Progresss : ", i, "\r\n");
                            //}
                            if (sw.ElapsedMilliseconds >= 1000)
                            {
                                log.Append("Progresss : ", i, ", ", (i * 100) / Count, "% \r\n");

                                sw.Restart();
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    log.Append("Exception: ");
                    log.Append(ex.Message);
                }
                finally
                {
                    wtr.Flush();
                    wtr.Close();
                }

                return flag;
            }
        }

        // Метаданные полей
        public class MetaField
        {
            public MetaField(BinaryReaderCRC32 rdr)
            {
                IsStorage = rdr.ReadBoolean();
                Name = rdr.ReadString();
                Size = rdr.ReadInt32();
                Offset = rdr.ReadInt32();
            }

            public bool IsStorage { get; }
            public string Name { get; }
            public int Size { get; }
            public int Offset { get; internal set; }
        }
    }
}
