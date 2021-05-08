using Algoverse.Threading;

namespace Algoverse.DataBase
{
    public class SortedIndex<T> : DataIndexBase<T>, IDataList<T>, IRecordsComparer<T> where T : Record, new()
    {
        Index                   dataIndex;
        Table<T>                table;
        SortedIndexTrigger<T>   trigger;
        ValueLockRW locker = new ValueLockRW();

        public SortedIndex(string uniqueName, Table<T> table, SortedIndexTrigger<T> trigger)
        {
            this.Name       = uniqueName;
            this.dataIndex  = table.DataBase.Index;
            this.table      = table;
            this.trigger    = trigger;

            Fields = Helper.Concat(trigger.Fields);

            table.AddIndex(this);
            
            RegisterIndex();
        }

        // The unique name this index from data structure
        public override string Name { get; protected set; }
        public override int MemoryKey { get; set; }

        // Return data object by index
        public T this[int index]
        {
            get
            {
                var code = dataIndex.GetByIndex(index, MemoryKey, locker);

                return table[code];
            }
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

        #region ' IDataIndex<T> members '

        // Memory address of the root element
        //int IDataIndexBase<T>.MemoryKey { get; set; }

        public override void RegisterIndex()
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

        // Update data object in index-0+,
        public override void Update(T oldObj, T newObj)
        {
            //var cmp = trigger.Compare(oldObj, newObj);

            //if (cmp != 0 || trigger.Filter(oldObj) != trigger.Filter(newObj))
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
            dataIndex.ClearTree(MemoryKey, locker);
        }

        public override Field[] Fields { get; }

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

            return trigger.Compare(xObj, yo);
        }

        public override bool Check(Log log)
        {
            log.Append("Sorted index (");
            log.Append(Name);
            log.Append(") started check: \r\n");
            
            var flug = dataIndex.CheckTree(MemoryKey, this, table, log);

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
