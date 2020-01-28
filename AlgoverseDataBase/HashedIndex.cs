using System;
using Algoverse.Threading;

namespace Algoverse.DataBase
{
    public class HashedIndex<TKey,T> : DataIndexBase<T>, IRecordsComparer<T>, IKeyComparer<TKey> where T : Record, new() where TKey : IComparable<TKey>
    {
        Index                       dataIndex;
        Table<T>                    table;
        HashedIndexTrigger<TKey,T>  trigger;
        ValueLockRW locker = new ValueLockRW();

        public HashedIndex(string uniqueName, Table<T> table, HashedIndexTrigger<TKey,T> trigger)
        {
            this.Name       = uniqueName;
            this.dataIndex  = table.DataBase.Index;
            this.table      = table;
            this.trigger    = trigger;

            Fields = Helper.Concat(trigger.Fields);

            table.AddIndex(this);
            
            RegisterIndex();
        }

        public override Field[] Fields { get; }

        // The unique name this index from data structure
        public override string Name { get; protected set; }
        public override int MemoryKey { get; set; }

        // Return data object by hash key
        public T this[TKey key]
        {
            get
            {
                var code = dataIndex.Find(key, MemoryKey, this, locker);

                return table[code];
            }
        }

        // Return data object by index
        public T GetByIndex (int index)
        {
            var code = dataIndex.GetByIndex(index, MemoryKey, locker);

            return table[code];
        }

        // Count data objects in index
        public int Count
        {
            get
            {
                return dataIndex.Count(MemoryKey, locker);
            }
        }

        // Check contains the code is in the index
        public bool Contains(int code)
        {
            return Contains(table.GetOriginal(code, this));
        }

        // Check contains the data object is in the index
        public bool Contains(T obj)
        {
            return dataIndex.Contains(obj, MemoryKey, this, locker);
        }

        // Check contains the key is in the index
        public bool Contains(TKey key)
        {
            var code = dataIndex.Find(key, MemoryKey, this, locker);

            if (code == 0)
            {
                return false;
            }

            return true;
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
                dataIndex.Insert(obj, MemoryKey, this, true, locker);
            }
        }

        // 
        public override void Delete(T obj)
        {
            dataIndex.Delete(obj, MemoryKey, this, locker);
        }

        // Update data object in index
        public override void Update(T oldObj, T newObj)
        {
            //var xk = trigger.GetHashKey(oldObj);
            //var yk = trigger.GetHashKey(newObj);

            //var cmp = xk.CompareTo(yk);

            //if (cmp != 0 || trigger.Filter(oldObj) != trigger.Filter(newObj))
            //{
            //    var wtf = this as DataIndexBase<T>;

            //    wtf.Delete(oldObj);
            //    wtf.Insert(newObj);
            //}

            var wtf = this as DataIndexBase<T>;
            
            wtf.Delete(oldObj);

            newObj.SetIndexCurrent(Id);

            wtf.Insert(newObj);

        }

        // Очистка индекса
        public override void Clear()
        {
            dataIndex.ClearTree(MemoryKey, locker);
        }

        public override int Id { get; internal set; }

        public override bool UsedAnotherTable { get; set; } = false;

        #endregion

        public DataIndexBase<T> Owner { get; set; }

        public int CompareRecords(T xObj, int yCode)
        {
            if (yCode == 0)
            {
                return 1;
            }

            var yo = table.GetOriginal(yCode, this);

            if (yo == null)
            {
                int bp = 0;
            }

            var xk = trigger.GetHashKey(xObj);
            var yk = trigger.GetHashKey(yo);

            return xk.CompareTo(yk);
        }

        public int Compare(TKey x, int y)
        {
            if (y == 0)
            {
                return 1;
            }

            var yo = table.GetOriginal(y, this);

            if (yo == null)
            {
                return 0;
            }

            var yk = trigger.GetHashKey(yo);

            return x.CompareTo(yk);     
        }

        public override bool Check(Log log)
        {
            var wtf = this as DataIndexBase<T>;

            log.Append("Hashed index (");
            log.Append(wtf.Name);
            log.Append(") started check: \r\n");

            var flug = dataIndex.CheckTree(wtf.MemoryKey, this, table, log);

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
