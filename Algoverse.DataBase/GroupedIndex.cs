using Algoverse.Threading;

namespace Algoverse.DataBase
{
    public class GroupedIndex<T> : DataIndexBase<T>, IGroupComparer<int>, IHashed<int,T>, IRecordsComparer<T>, IMemoryValidator, IDataIndexBase<T> where T : Record
    {
        Index                   dataIndex;
        Table<T>                table;
        GroupedIndexTrigger<T>  trigger;
        ValueLockRW locker = new ValueLockRW();

        public GroupedIndex(string uniqueName, Table<T> table, GroupedIndexTrigger<T> trigger)
        {
            this.Name         = uniqueName;
            this.dataIndex    = table.DataBase.Index;
            this.table        = table;
            this.trigger = trigger;

            Fields = Helper.Concat(trigger.Fields);

            table.AddIndex(this);
            RegisterIndex();
        }

        // The unique name this index from data structure
        public override string Name { get; protected set; }
        public override int MemoryKey { get; set; }

        // Return data object by group and index
        public T this[int group, int index]
        {
            get
            {
                var wtf = this as DataIndexBase<T>;
                var key = dataIndex.GetGroupTree(wtf.MemoryKey, @group, this, locker);

                if (key != 0)
                {
                    var code = dataIndex.GetByIndex(index, key, locker);

                    return table[code];
                }

                return null;
            }
        }

        // Return record list for this group
        public RecordsListView<T> this[int group]
        {
            get
            {
                //var wtf = this as IDataIndex<T>;
                //var mem = dataIndex.GetGroup(group, wtf.MemoryKey, this);

                return new RecordsListView<T>(group, dataIndex, table, this, this, this, locker);
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
            var ret = false;
            var wtf = this as DataIndexBase<T>;
            var grp = trigger.GetGroupCode(obj);
            var mem = dataIndex.GetGroupTree(wtf.MemoryKey, grp, this, locker);

            if (mem != 0)
            {
                ret = dataIndex.Contains(obj, mem, this, locker);
            }

            return ret;
        }

        // Получение количества групп
        public int CountGroups
        {
            get
            {
                return dataIndex.GetGroupsCount(MemoryKey, locker);
            }
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
                var grp = trigger.GetGroupCode(obj);

                dataIndex.InsertToGroup(MemoryKey, grp, obj, this, this, locker);
            }
        }

        // 
        public override void Delete(T obj)
        {
            //var wtf = this as DataIndexBase<T>;
            var grp = trigger.GetGroupCode(obj);

            dataIndex.DeleteFromGroup(MemoryKey, grp, obj, this, this, locker);

            //var mem = dataIndex.GetGroup(grp, wtf.MemoryKey, this);

            //if (mem != 0)
            //{
            //    dataIndex.Delete(obj, mem, this);

            //    if (dataIndex.Count(mem) == 0)
            //    {
            //        //dataIndex.RemoveGroup(grp, wtf.MemoryKey, this);
            //    }
            //}
        }

        // Очистить весь индекс
        public override void Clear()
        {
            dataIndex.ClearGroup(MemoryKey, locker);
        }

        // Update data object in index
        public override void Update(T oldObj, T newObj)
        {
            //if (trigger.Filter(oldObj) != trigger.Filter(newObj) || trigger.GetGroupCode(oldObj) != trigger.GetGroupCode(newObj) || trigger.Compare(oldObj, newObj) != 0)
            //{
            //    
            //}
            
            //var wtf = this as DataIndexBase<T>;
            
            Delete(oldObj);

            newObj.SetIndexCurrent(Id);

            Insert(newObj);
        }

        public override Field[] Fields { get; }

        public override int Id { get; internal set; }

        public override bool UsedAnotherTable { get; set; } = false;

        #endregion

        #region ' Compare '

        public int CompareGroups(int grp, int rec)
        {
            var yo = table.GetOriginal(rec, this);

            if (yo == null)
            {
                return 0;
            }

            var yg = trigger.GetGroupCode(yo);

            return grp.CompareTo(yg);
        }

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

        int IMemoryValidator.GetMemoryKey(object grp)
        {
            var group = (int) grp;

            var wtf = this as DataIndexBase<T>;
            var mem = dataIndex.GetGroupTree(wtf.MemoryKey, @group, this, locker);

            return mem;
        }

        public int GetHashKey(T obj)
        {
            return trigger.GetGroupCode(obj);
        }

        #endregion

        public override bool Check(Log log)
        {
            var wtf = this as DataIndexBase<T>;

            log.Append("Grouped index (");
            log.Append(wtf.Name);
            log.Append(") started check: \r\n");

            var flug = dataIndex.CheckGroup(wtf.MemoryKey, this, this, this, table, log);

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

        public void PrintToDebug()
        {
            var wtf = this as DataIndexBase<T>;

            dataIndex.PrintGroupToDebug(wtf.MemoryKey);
        }
    }
}


// TODO индекс групп можно заменить на хештаблицу, даже в контексте того что дерево может превращаться в массив, все равно в нем будет 
// бинарный поиск. А хеш таблица работает куда быстрее. Это должна быть хеш таблица деревьев. Тоже самое относится и к мултигруппам.