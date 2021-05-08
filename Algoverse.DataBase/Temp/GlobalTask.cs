using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Algoverse.DataBase
{
    public abstract class GlobalTask
    {
        public static GlobalTask Load(DataBase db, string pathTask)
        {
            var fs = new FileStream(pathTask, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            var br = new BinaryReader(fs);

            var type   = (GlobalTaskType) br.ReadInt32();
            var status = (RunStatus) br.ReadInt32();
            var lname  = br.ReadString();
            var log    = db.Logs.GetByCode(lname);

            try
            {
                switch (type)
                {
                    case GlobalTaskType.RestoreWork:
                    {
                        return new RestoreTask(br, pathTask, log) {Status = status};
                    }
                    case GlobalTaskType.BackupWork:
                    {
                        return new SomeTask(pathTask, log);
                    }
                }
            }
            finally
            {
                br.Close();
            }

            throw new Exception("Wrong task type.");
        }

        public static RestoreTask CreateRestore(string pathBackup, string pathTask, Log log)
        {
            var task = new RestoreTask(pathBackup, pathTask, log);

            task.Save();

            return task;
        }

        public static SomeTask Create(string pathTask, Log log)
        {
            var task = new SomeTask(pathTask, log);

            task.Save();

            return task;
        }

        protected GlobalTask(string taskPath, Log log)
        {
            TaskPath = taskPath;
            Log      = log;
            Status   = RunStatus.Worked;
        }

        public abstract GlobalTaskType Type     { get; }
        public          RunStatus      Status   { get; set; }
        public          string         TaskPath { get; }
        public          Log            Log      { get; }
        public          bool           HasError { get; set; }

        public void Save()
        {
            var fs = new FileStream(TaskPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            var bw = new BinaryWriter(fs);

            bw.Write((int) Type);
            bw.Write((int) Status);
            bw.Write(Log.Code);

            OnSave(bw);

            fs.Close();
        }

        protected abstract void OnSave(BinaryWriter bw);

        public enum RunStatus
        {
            Worked,
            Stoped
        }
    }

    public class SomeTask : GlobalTask
    {
        public SomeTask(string taskPath, Log log) : base(taskPath, log)
        {
        }

        public override GlobalTaskType Type
        {
            get { return GlobalTaskType.BackupWork; }
        }

        protected override void OnSave(BinaryWriter bw)
        {
        }
    }

    public class RestoreTask : GlobalTask
    {
        public RestoreTask(BinaryReader br, string taskPath, Log log) : base(taskPath, log)
        {
            Step           = (RestoreStep) br.ReadInt32();
            PathBackup     = br.ReadString();
            TableIndex     = br.ReadInt32();
            RecordIndex    = br.ReadInt32();
            RecordsCount   = br.ReadInt32();
            ReaderPosition = br.ReadInt32();
        }

        public RestoreTask(string pathBackup, string taskPath, Log log) : base(taskPath, log)
        {
            Step       = RestoreStep.FillTables;
            PathBackup = pathBackup;
        }

        public override GlobalTaskType Type
        {
            get { return GlobalTaskType.RestoreWork; }
        }

        public RestoreStep Step           { get; set; }
        public string      PathBackup     { get; }
        public int         TableIndex     { get; set; }
        public int         RecordIndex    { get; set; }
        public int         RecordsCount   { get; set; }
        public long        ReaderPosition { get; set; }

        protected override void OnSave(BinaryWriter bw)
        {
            bw.Write((int) Step);
            bw.Write(PathBackup);
            bw.Write(TableIndex);
            bw.Write(RecordIndex);
            bw.Write(RecordsCount);
            bw.Write(ReaderPosition);
        }

        public enum RestoreStep
        {
            FillTables,
            RebuildIndex,
            CriticalException
        }
    }

    public enum GlobalTaskType
    {
        StartComplated,
        RestoreWork,
        BackupWork,
        CheckIndexWork,
        CheckTableWork
    }
}