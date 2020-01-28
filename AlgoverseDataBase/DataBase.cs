using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using Algoverse.Threading;

namespace Algoverse.DataBase
{
    public abstract class DataBase : IDisposable, IVerifiableTable
    {
        //public static long TestKey = -1;
        //public static string TestString = "";

        protected DataBase(string path)
        {
            Tables        = new List<ITable>();
            DirectoryPath = Path.GetDirectoryName(path) + "\\";

            DirectoryPath.CheckPath();

            Logs = new Logs(this);
        }

        int locker = 0;

        public Logs         Logs          { get; }
        public ArrayStorage Storage       { get; private set; }
        public string       DirectoryPath { get; }

        internal Index        Index  { get; private set; }
        internal List<ITable> Tables { get; }

        public bool MemCheck()
        {
            return Index.PrintMemoryToDebug();
        }

        // Отражает текущее состояние базы данных
        public DatabaseStatus Status { get; private set; } = DatabaseStatus.Ready;
        // Лог состояния
        public string StatusLogId { get; private set; } = "";

        // Версия данных
        public abstract int Version { get; }

        // todo 123 хеш-таблица...
        internal ITable GetTableByKey(string key)
        {
            for (var i = 0; i < Tables.Count; ++i)
            {
                var itm = Tables[i];

                if (itm.Key == key)
                {
                    return itm;
                }
            }

            return null;
        }

        // Попытка запустить базу данных
        public void Start()
        {
            try
            {
                ValueLock.Lock(ref locker);

                if (Status != DatabaseStatus.Ready)
                {
                    return;
                }

                GetStatus();

                if (Status == DatabaseStatus.Ready)
                {
                    Storage = new ArrayStorage(DirectoryPath + "DefaultStorage.sto");
                    Index   = new Index(DirectoryPath        + "DefaultIndex.ind");

                    OnStart();

                    Status = DatabaseStatus.Started;
                }
            }
            finally
            {
                locker = 0;
            }
        }

        // Сигнализация об остановке базы данных
        public virtual void Stop()
        {
            Status = DatabaseStatus.Closing;

            Logs.Save();

            OnStop();
        }

        // Обработка запуска наследниками
        protected virtual void OnStart()
        {
        }

        // Обработка остановки наследниками
        protected virtual void OnStop()
        {
        }

        // Освобождение ресурсов
        public void Dispose()
        {
            for (int i = 0; i < Tables.Count; ++i)
            {
                Tables[i].Dispose();
            }
        }

        // Переинддексация индексов
        public void RebuildIndex()
        {
            //Index.Clear();

            for (var i = 0; i < Tables.Count; ++i)
            {
                var table = Tables[i];

                //table.RegisterIndexes();
                table.RebuildIndex();
            }
        }

        #region ' Data Base Tasks '

        // Проверка индексов
        public bool CheckIndex(Log log)
        {
            var sta = SetStatus(DatabaseStatus.CheckIndex, log.Code);

            log.Append("Begin full database check at ");
            log.Append(DateTime.Now.ToString("D"));
            log.Append("\r\n");

            var flug = Index.CheckIndexTree(log);

            for (int i = 0; i < Tables.Count; ++i)
            {
                var itm = Tables[i] as IVerifiableTable;

                try
                {
                    flug |= itm.CheckIndex(log);
                }
                catch (Exception e)
                {
                    flug = false;

                    log.Append(e);
                }
            }

            SetStatus(sta, log.Code);

            return flug;
        }

        // Проверка таблиц
        public bool CheckTable(Log log)
        {
            var sta = SetStatus(DatabaseStatus.CheckTable, log.Code);

            log.Append("Begin full database check at ", DateTime.Now.ToString("D"), "\r\n");

            var flug = false;

            for (int i = 0; i < Tables.Count; ++i)
            {
                var itm = Tables[i] as IVerifiableTable;

                try
                {
                    flug |= itm.CheckTable(log);
                }
                catch (Exception e)
                {
                    flug = false;

                    log.Append(e);
                }
            }

            SetStatus(sta, log.Code);

            return flug;
        }

        // Резервирование данных
        public void Backup(string path, Log log)
        {
            var sta = SetStatus(DatabaseStatus.Backup, log.Code);

            var backup = new Backup(this);

            var f = backup.Write(path, log);

            //var stream_data = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);

            //stream_data.Position = Algoverse.DataBase.Backup.TestPosition;
            //var br = new BinaryReader(stream_data);
            //var ppc = br.ReadInt32();

            //stream_data.Close();

            //return f;

            SetStatus(sta, log.Code);
        }

        // Восстанавливает базу данных из файла
        public void Restore(string path, Log log)
        {
            var sta = SetStatus(DatabaseStatus.Restore, log.Code);

            var hasError = false;

            try
            {
                var backup = new Backup(this);

                // Очищаем кеш записей
                for (int i = 0; i < Tables.Count; ++i)
                {
                    var itm = Tables[i];

                    itm.ResetMemoryCash();
                }

                // Очищаем кеш строк
                Storage.Clear();

                // Очищаем индекс
                Index.Clear();
                
                // Записываем данные
                hasError = !backup.Restore(path, log);

                // Записываем все на диск
                Storage.FlushWriters();

                if (!hasError)
                {
                    log.Append("Begin rebuild index \r\n");

                    // Регистрируем индексы
                    for (int i = 0; i < Tables.Count; ++i)
                    {
                        var itm = Tables[i] as ITable;

                        itm.InitIndexes(log);
                    }

                    log.Append("\r\n");

                    // Перестраиваем индекс каждой таблицы
                    for (int i = 0; i < Tables.Count; ++i)
                    {
                        var itm    = Tables[i];
                        var status = itm.RebuildIndex(log);

                        if (status != 0)
                        {
                            log.Append("Task pause. Status: ");
                            log.Append(status);
                            log.Append("\r\n");

                            return;
                        }

                        //task.RecordIndex = 0;
                    }
                }
            }
            catch (Exception e)
            {
                hasError = true;
                //log.Append("Step: ");
                //log.Append(task.Step);
                //log.Append("\r\n");
                //log.Append("-----------------------\r\n");
                //log.Append("Read: ");
                //log.Append(task.RecordIndex);
                log.Append("\r\n");
                log.Append(e);
                log.Append("\r\n");
            }

            log.Append("\r\n");

            if (hasError)
            {
                log.Append("Restore FAIL  \r\n");

                SetStatus(DatabaseStatus.Error, log.Code);
            }
            else
            {
                log.Append("Restore DONE \r\n");

                SetStatus(sta, log.Code);
            }
        }

        // Установка статуса базы
        DatabaseStatus SetStatus(DatabaseStatus status, string log = "")
        {
            var path = DirectoryPath + "status.cfg";
            var old = Status;
            
            Status = status;
            StatusLogId = log;

            if (File.Exists(path))
            {
                File.Delete(path);
            }

            if (status == DatabaseStatus.Ready || status == DatabaseStatus.Started)
            {
                return old;
            }

            var fs = File.OpenWrite(path);
            var bw = new BinaryWriterFast(fs);

            bw.Write((int)status);
            bw.Write(log);

            fs.Close();

            return old;
        }

        // Чтение статуса базы
        void GetStatus()
        {
            var path = DirectoryPath + "status.cfg";

            if (File.Exists(path))
            {
                var fs = File.OpenRead(path);
                var br = new BinaryReaderFast(fs);

                Status = (DatabaseStatus)br.ReadInt32();
                StatusLogId = br.ReadString();

                fs.Close();
            }
            else
            {
                Status = DatabaseStatus.Ready;
                StatusLogId = "";
            }
        }

        #endregion

        #region ' Temp '

        // Восстанавливает базу данных из файла
        //void __Restore(string path, Log log)
        //{
        //    Debug.WriteLine("Restore begin");

        //    if (HasTask)
        //    {
        //        log.AppendLine("Fail: DataBase already have global task.");

        //        return;
        //    }

        //    var task = GlobalTask.CreateRestore(path, DirectoryPath + "global.task", log);

        //    HasTask = true;

        //    Restore(task);
        //}

        //// Попытка запустить базу данных
        //public GlobalTaskType Start()
        //{
        //    var path = DirectoryPath + "global.task";

        //    if (File.Exists(path))
        //    {
        //        var task = GlobalTask.Load(this, path);

        //        if (task.Status == GlobalTask.RunStatus.Stoped)
        //        {
        //            task.Log.Append("Task continue.\r\n");

        //            return _Start(task);
        //        }

        //        return task.Type;
        //    }
        //    else
        //    {
        //        return _Start(null);
        //    }
        //}

        //GlobalTaskType _Start(GlobalTask task)
        //{
        //    Storage = new ArrayStorage(DirectoryPath + "DefaultStorage.sto");
        //    Index = new Index(DirectoryPath + "DefaultIndex.ind");

        //    if (task != null)
        //    {
        //        switch (task.Type)
        //        {
        //            case GlobalTaskType.RestoreWork:
        //            {
        //                Restore(task as RestoreTask);

        //                break;
        //            }
        //            case GlobalTaskType.BackupWork:
        //            {
        //                break;
        //            }
        //            case GlobalTaskType.CheckIndexWork:
        //            {
        //                break;
        //            }
        //            case GlobalTaskType.CheckTableWork:
        //            {
        //                break;
        //            }
        //            default:
        //            {
        //                throw new ArgumentOutOfRangeException();
        //            }
        //        }
        //    }

        //    IsStarted = true;

        //    OnStart();

        //    return GlobalTaskType.StartComplated;
        //}

        //public void BeginRestore2(RestoreTask task)
        //{
        //    //try
        //    //{
        //    //    if (task.Step == RestoreTask.RestoreStep.CriticalException)
        //    //    {
        //    //        // todo
        //    //        return;
        //    //    }

        //    //    task.Status = GlobalTask.RunStatus.Worked;
        //    //    task.Save();

        //    //    if (task.Step == RestoreTask.RestoreStep.FillTables)
        //    //    {
        //    //        FileStream fs = null;

        //    //        try
        //    //        {
        //    //            fs = File.OpenRead(task.PathBackup);

        //    //            var br = new BinaryReader(fs);

        //    //            if (task.TableIndex == 0 && task.RecordIndex == 0)
        //    //            {
        //    //                var ticks = br.ReadInt64();
        //    //                var dt = new DateTime(ticks);

        //    //                task.Log.Append("Restore begin at ");
        //    //                task.Log.Append(dt.ToString("F"));
        //    //                task.Log.Append("\r\n");
        //    //            }
        //    //            else
        //    //            {
        //    //                fs.Position = task.ReaderPosition;
        //    //            }

        //    //            // Заполняем таблицы данными
        //    //            for (int i = task.TableIndex; i < Tables.Count; ++i)
        //    //            {
        //    //                task.TableIndex = i;

        //    //                var itm = Tables[i] as ITable;
        //    //                var status = itm.ReadFrom(br, task);

        //    //                if (status != 0)
        //    //                {
        //    //                    task.Log.Append("Task pause. Status: ");
        //    //                    task.Log.Append(status);
        //    //                    task.Log.Append("\r\n");
        //    //                    task.Status = GlobalTask.RunStatus.Stoped;
        //    //                    task.Save();

        //    //                    //return;
        //    //                }

        //    //                task.RecordIndex = 0;
        //    //            }

        //    //            task.TableIndex = 0;
        //    //            task.RecordIndex = 0;
        //    //            task.Step = RestoreTask.RestoreStep.RebuildIndex;
        //    //        }
        //    //        catch (Exception e)
        //    //        {
        //    //            task.HasError = true;
        //    //            task.Log.Append("Step: ");
        //    //            task.Log.Append(task.Step);
        //    //            task.Log.Append("\r\n");
        //    //            task.Log.Append("-----------------------\r\n");
        //    //            //task.Log.Append("Table: ");
        //    //            //task.Log.Append((Tables[task.TableIndex] as ITable).Key);
        //    //            //task.Log.Append("\r\n");
        //    //            task.Log.Append("Read: ");
        //    //            task.Log.Append(task.RecordIndex);
        //    //            task.Log.Append("\r\n");
        //    //            task.Log.Append(e);
        //    //            task.Log.Append("\r\n");

        //    //            task.Step = RestoreTask.RestoreStep.CriticalException;

        //    //            //return;
        //    //        }
        //    //        finally
        //    //        {
        //    //            if (fs != null)
        //    //            {
        //    //                fs.Close();
        //    //            }
        //    //        }
        //    //    }

        //    //    if (task.Step == RestoreTask.RestoreStep.RebuildIndex)
        //    //    {
        //    //        try
        //    //        {
        //    //            if (task.TableIndex == 0 && task.RecordIndex == 0)
        //    //            {
        //    //                task.Log.Append("Begin rebuild index \r\n");

        //    //                // Очищаем индекс
        //    //                Index.Clear();

        //    //                // Регистрируем индексы
        //    //                for (int i = 0; i < Tables.Count; ++i)
        //    //                {
        //    //                    var itm = Tables[i] as ITable;

        //    //                    itm.InitIndexes(task.Log);
        //    //                }
        //    //            }

        //    //            // Перестраиваем индекс каждой таблицы
        //    //            for (int i = task.TableIndex; i < Tables.Count; ++i)
        //    //            {
        //    //                var itm = Tables[i] as ITable;
        //    //                var status = itm.ReBuildIndex(task);

        //    //                if (status != 0)
        //    //                {
        //    //                    task.Log.Append("Task pause. Status: ");
        //    //                    task.Log.Append(status);
        //    //                    task.Log.Append("\r\n");
        //    //                    task.Status = GlobalTask.RunStatus.Stoped;
        //    //                    task.Save();

        //    //                    return;
        //    //                }

        //    //                task.RecordIndex = 0;
        //    //            }
        //    //        }
        //    //        catch (Exception e)
        //    //        {
        //    //            task.HasError = true;
        //    //            task.Log.Append("Step: ");
        //    //            task.Log.Append(task.Step);
        //    //            task.Log.Append("\r\n");
        //    //            task.Log.Append("-----------------------\r\n");
        //    //            //task.Log.Append("Table: ");
        //    //            //task.Log.Append((Tables[task.TableIndex] as Table<Read>).Key);
        //    //            //task.Log.Append("\r\n");
        //    //            task.Log.Append("Read: ");
        //    //            task.Log.Append(task.RecordIndex);
        //    //            task.Log.Append("\r\n");
        //    //            task.Log.Append(e);
        //    //            task.Log.Append("\r\n");

        //    //            task.Step = RestoreTask.RestoreStep.CriticalException;
        //    //        }

        //    //        task.Log.Append("\r\n");

        //    //        if (task.HasError)
        //    //        {
        //    //            task.Log.Append("Restore FAIL  \r\n");
        //    //        }
        //    //        else
        //    //        {
        //    //            task.Log.Append("Restore DONE \r\n");
        //    //        }
        //    //    }
        //    //}
        //    //finally
        //    //{
        //    //    task.Status = GlobalTask.RunStatus.Stoped;
        //    //    task.Save();
        //    //}

        //    //if (task.Step != RestoreTask.RestoreStep.CriticalException)
        //    //{
        //    //    if (File.Exists(task.TaskPath))
        //    //    {
        //    //        File.Delete(task.TaskPath);
        //    //    }
        //    //}
        //}

        #endregion

    }

    public enum DatabaseStatus
    {
        // Готово к запуску
        Ready,
        // Запушено в штатном режиме
        Started,
        // В процессе остановки
        Closing,
        // В процессе восстановление данных
        Restore,
        // В процессе резервировиния данных
        Backup,
        // В процессе проверки индексов
        CheckIndex,
        // В процессе проверки таблиц
        CheckTable,
        // Произошла ошибка в процессе выполнения задачи
        Error,
    }
}