using System;
using System.Diagnostics;
using Algoverse.Threading;

namespace Algoverse.DataBase
{
    public class HashedListIndex<TKey,T> : DataIndexBase<T>, IGroupComparer<TKey>, IRecordsComparer<T>, IMemoryValidator where T : Record, new() where TKey : IComparable<TKey>
    {
        Index                       dataIndex;
        Table<T>                    table;
        HashedListTrigger<TKey, T>  trigger;
        ValueLockRW locker = new ValueLockRW();

        public HashedListIndex(string uniqueName, Table<T> table, HashedListTrigger<TKey,T> trigger)
        {
            this.Name           = uniqueName;
            this.dataIndex      = table.DataBase.Index;
            this.table          = table;
            this.trigger        = trigger;

            Fields = Helper.Concat(trigger.Fields);

            table.AddIndex(this);
            RegisterIndex();
        }

        // The unique name this index from data structure
        public override string Name { get; protected set; }
        public override int MemoryKey { get; set; }

        // Return data object by key and index
        public T this[TKey key, int index]
        {
            get
            {
                var wtf = this as DataIndexBase<T>;
                var mem = dataIndex.GetGroupTree(wtf.MemoryKey, key, this, locker);

                if (mem != 0)
                {
                    var code = dataIndex.GetByIndex(index, mem, locker);

                    return table[code];
                }

                return null;
            }
        }

        // Return record list for this group
        public RecordsListView<T> this[TKey key]
        {
            get
            {
                return new RecordsListView<T>(key, dataIndex, table, this, this, this, locker);                
            }
        }

        // Total count data objects in index
        //public int Count
        //{
        //    get
        //    {
        //        return dataIndex.Count(((IDataIndex<T>)this).MemoryKey);
        //    }
        //}

        // Check contains the code is in the index
        public bool Contains(int code)
        {
            return Contains(table.GetOriginal(code, this));
        }

        // Check contains the data object is in the index
        public bool Contains(T obj)
        {
            var ret = false;
            var wtf = this as DataIndexBase<T>;
            var grp = trigger.GetHashKey(obj);
            var mem = dataIndex.GetGroupTree(wtf.MemoryKey, grp, this, locker);

            if (mem != 0)
            {
                ret = dataIndex.Contains(obj, mem, this, locker);
            }

            return ret;
        }

        #region ' IDataIndex<T> members '

        // Memory address of the root element
        //int IDataIndexBase<T>.MemoryKey { get; set; }
        public sealed override void RegisterIndex()
        {
            dataIndex.RegisterIndex(this);
        }

        // 
        public override void Insert(T obj)
        {
            if (!trigger.Filter(obj))
            {
                var grp = trigger.GetHashKey(obj);

                //Debug.WriteLine(MemoryKey);
                //dataIndex.PrintGroupToDebug(MemoryKey);

                //var mem = dataIndex.GetGroupTree(MemoryKey, grp, this);
                //var c = 0;

                //if (mem > 0)
                //{
                //    c = dataIndex.Count(mem);
                //}

                //if (c == 2)
                //{
                //    int bp = 0;
                //}

                dataIndex.InsertToGroup(MemoryKey, grp, obj, this, this, locker);

                //var log = table.DataBase.Logs.Create();
                //var chk = Check(log);

                //if (chk)
                //{
                //    Debug.WriteLine(log.ToStringEx());
                //}

                //mem = dataIndex.GetGroupTree(MemoryKey, grp, this);

                //var cc = dataIndex.Count(mem);

                //Debug.WriteLine("count " + cc + ", count list " + this[grp].Count);

                //if (cc != c + 1)
                //{
                //    int bp = this[grp].Count;
                //}
            }
        }

        // 
        public override void Delete(T obj)
        {
            var grp = trigger.GetHashKey(obj);

            dataIndex.DeleteFromGroup(MemoryKey, grp, obj, this, this, locker);

            //var mem = dataIndex.GetGroup(key, wtf.MemoryKey, this);

            //if (mem != 0)
            //{
            //    dataIndex.Delete(obj, mem, this);

            //    if (dataIndex.Count(mem) == 0)
            //    {
            //        //dataIndex.RemoveGroup(key, wtf.MemoryKey, this);
            //    }
            //}
        }

        // Update data object in index
        public override void Update(T oldObj, T newObj)
        {
            //var xk = trigger.GetHashKey(oldObj);
            //var yk = trigger.GetHashKey(newObj);

            //var cmp = xk.CompareTo(yk);

            //if (cmp != 0 || trigger.Filter(oldObj) != trigger.Filter(newObj) || trigger.Compare(oldObj, newObj) != 0)
            //{
            //    var wtf = this as DataIndexBase<T>;

            //    wtf.Delete(oldObj);
            //    wtf.Insert(newObj);

            //}

                Delete(oldObj);

                newObj.SetIndexCurrent(Id);

                Insert(newObj);
        }

        // Очистка индекса
        public override void Clear()
        {
            dataIndex.ClearGroup(MemoryKey, locker);
        }

        public override Field[] Fields { get; }

        public override int Id { get; internal set; }

        public override bool UsedAnotherTable { get; set; } = false;

        #endregion

        // 
        public int CompareGroups(TKey grp, int rec)
        {
            if (rec == 0)
            {
                return 1;
            }

            var yo = table.GetOriginal(rec, this);

            if (yo == null)
            {
                return 0;
            }

            var yk = trigger.GetHashKey(yo);

            return grp.CompareTo(yk);
        }

        // 
        public DataIndexBase<T> Owner { get; set; }

        public int CompareRecords(T xObj, int yCode)
        {
            if (yCode == 0)
            {
                return 1;
            }

            T yObj = table.GetOriginal(yCode, this);

            return trigger.Compare(xObj, yObj);
        }

        public int GetMemoryKey(object grp)
        {
            var key = (TKey)grp;

            var mem = dataIndex.GetGroupTree(MemoryKey, key, this, locker);

            return mem;
        }

        public override bool Check(Log log)
        {
            log.Append("HashedList index (");
            log.Append(Name);
            log.Append(") started check: \r\n");

            var flug = dataIndex.CheckGroup(MemoryKey, this, this, trigger, table, log);

            if (flug)
            {
                log.Append("Fail!\r\n");
            }
            else
            {
                log.Append("Done\r\n");
            }

            return flug;
        }
    }
}
