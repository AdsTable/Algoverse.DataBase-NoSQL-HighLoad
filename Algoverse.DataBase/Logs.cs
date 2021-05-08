using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace Algoverse.DataBase
{
    public class Logs
    {
        readonly DataBase db;
        Dictionary<string, Log> ht;
        Timer timer;

        public Logs(DataBase db)
        {
            this.db = db;
            ht = new Dictionary<string, Log>();
            //var dir = Directory.GetFiles(db.DirectoryPath, "*.log");

            Update();

            timer = new Timer((o) =>
            {
                Save();
                Update();

            }, null, 0, 500);
        }

        void Update()
        {
            var dir = Directory.GetFiles(db.DirectoryPath, "*.log");

            for (var i = 0; i < dir.Length; ++i)
            {
                var itm = dir[i];
                var code = Path.GetFileNameWithoutExtension(itm);
                var date = File.GetLastWriteTime(itm);

                if (ht.ContainsKey(code))
                {
                    var tmp = ht[code];

                    if (tmp.LastWriteTime != date)
                    {
                        ht[code] = new Log(db.DirectoryPath, code) {LastWriteTime = date};
                    }
                }
                else
                {
                    ht.Add(code, new Log(db.DirectoryPath, code){LastWriteTime = date});
                }
            }            
        }

        public Log Create()
        {
            var ret = new Log(db.DirectoryPath);

            ht.Add(ret.Code, ret);

            return ret;
        }

        public Log GetByCode(string code)
        {
            if (ht.ContainsKey(code))
            {
                return ht[code];
            }

            return null;
        }

        public void Save()
        {
            var logs = new Log[ht.Values.Count];
            ht.Values.CopyTo(logs, 0);

            for (var i = 0; i < logs.Length; ++i)
            {
                var itm = logs[i];

                itm.Dispose();
            }
        }

        public void Remove(string code)
        {
            if (ht.ContainsKey(code))
            {
                ht.Remove(code);

                var path = db.DirectoryPath + code + ".log";

                if (File.Exists(path))
                {
                    File.Delete(path);
                }
            }
        }
    }
}
