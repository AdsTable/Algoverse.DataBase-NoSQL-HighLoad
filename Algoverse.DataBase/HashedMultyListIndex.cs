using System;
using Algoverse.Threading;

namespace Algoverse.DataBase
{
    // TODO Мульти дерево содержит список полей как в тригере по которому строится дерево группы, так 
    // список полей по которым строится каждое поддерево. Вопрос в том нужно ли обновлять каждое поддерево и 
    // само дерево. Если изменилось поле одного поддерева его и нужно менять, а не всю группу.

    // TODO На этот индекс установлен костыль потому что класс Entrys содержит два индекса этого типа ListByUser и ListByUserLocked
    // которые зависят от других таблиц.
    public class HashedMultyListIndex<TKey,T> : DataIndexBase<T>, IGroupComparer<TKey>, IRecordsComparer<T>, IMemoryValidator, IDataIndexBase<T> where T : Record, new() where TKey : IComparable<TKey>
    {
        Index                       dataIndex;
        Table<T>                    table;
        HashedListTrigger<TKey, T>  trigger;
        ValueLockRW locker = new ValueLockRW();

        readonly IRecordsComparer<T>[] recordsComparers;

        public HashedMultyListIndex(string uniqueName, Table<T> table, HashedListTrigger<TKey, T> trigger, IRecordsComparer<T>[] recordsComparers)
        {
            Name           = uniqueName;
            dataIndex      = table.DataBase.Index;

            this.table          = table;
            this.trigger        = trigger;
            this.recordsComparers = recordsComparers;

            Fields = Helper.Concat(trigger.Fields);

            for (var i = 0; i < recordsComparers.Length; ++i)
            {
                recordsComparers[i].Owner = this;

                Fields = Helper.Concat(Fields, recordsComparers[i].Fields);
            }

            table.AddIndex(this);
            RegisterIndex();
        }

        // The unique name this index from data structure
        public override string Name { get; protected set; }
        public override int MemoryKey { get; set; }

        // Return data object by key, multy and index
        public T this[TKey key, int multy, int index]
        {
            get
            {
                var wtf = this as DataIndexBase<T>;
                var mem = dataIndex.GetMultyTree(key, wtf.MemoryKey, this, multy, locker);

                if (mem != 0)
                {
                    var code = dataIndex.GetByIndex(index, mem, locker);

                    return table[code];
                }

                return null;
            }
        }

        // Return data object by key, multy and index
        public int GetCount(TKey key, int multy)
        {
            var wtf = this as DataIndexBase<T>;
            var mem = dataIndex.GetMultyTree(key, wtf.MemoryKey, this, multy, locker);

            if (mem != 0)
            {
                return dataIndex.Count(mem, locker);
            }

            return 0;
        }

        // Return data list
        public IDataList<T> GetList(TKey key, int multy)
        {
            var list = new RecordsListView<T>(new object[] { key, multy }, dataIndex, table, recordsComparers[multy], this, this, locker);

            return list;
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
            var grp = trigger.GetHashKey(obj);
            var mem = dataIndex.GetGroupTree(wtf.MemoryKey, grp, this, locker);

            if (mem != 0)
            {
                ret = dataIndex.Contains(obj, mem, this, locker);
            }

            return ret;
        }

        // Check contains group key
        public bool ContainsKey(TKey key)
        {
            var wtf = this as DataIndexBase<T>;
            var mem = dataIndex.GetGroupTree(wtf.MemoryKey, key, this, locker);

            return mem != 0;
        }

        // Поиск ключа
        //public int KeyToCode(TKey key)
        //{
        //    return 0;
        //}

        #region ' IDataIndex<T> members '

        // Memory address of the root element
        //int IDataIndexBase<T>.MemoryKey { get; set; }
        public override void RegisterIndex()
        {
            dataIndex.RegisterIndex(this);
        }

        // Flug indicate what possible remove group keys
        public bool DontRemoveKeys { get; set; }

        // 
        public override void Insert(T obj)
        {
            var wtf = this as DataIndexBase<T>;

            if (!trigger.Filter(obj))
            {
                var grp = trigger.GetHashKey(obj);

                dataIndex.InsertToMulty(wtf.MemoryKey, grp, obj, this, recordsComparers, locker);
            }
        }

        // 
        public void Insert(TKey key, T obj)
        {
            var wtf = this as DataIndexBase<T>;

            if (!trigger.Filter(obj))
            {
                dataIndex.InsertToMulty(wtf.MemoryKey, key, obj, this, recordsComparers, locker);
            }
        }

        // 
        public override void Delete(T obj)
        {
            var wtf = this as DataIndexBase<T>;
            var grp = trigger.GetHashKey(obj);

            dataIndex.DeleteFromMulty(wtf.MemoryKey, grp, obj, this, recordsComparers, locker);
        }

        // Update data object in index
        public override void Update(T oldObj, T newObj)
        {
            //var xk = trigger.GetHashKey(oldObj);
            //var yk = trigger.GetHashKey(newObj);

            //var cmp = xk.CompareTo(yk);

            //if (cmp != 0 || trigger.Filter(oldObj) != trigger.Filter(newObj) || trigger.Compare(oldObj, newObj) != 0)
            //{
            //    var wtf = this as IDataIndex<T>;

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
            dataIndex.ClearMulty(MemoryKey, recordsComparers.Length, locker);
        }

        public sealed override Field[] Fields { get; }

        public override int Id { get; internal set; }

        public override bool UsedAnotherTable
        {
            get
            {
                return true;
            }
            set { throw new NotImplementedException(); }
        }

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

        //
        public int GetMemoryKey(object arg)
        {
            var args = arg as object[];
            var key = (TKey)args[0];
            var multy = (int)args[1];

            var wtf = this as DataIndexBase<T>;
            var mem = dataIndex.GetMultyTree(key, wtf.MemoryKey, this, multy, locker);

            return mem;
        }

        //
        public override bool Check(Log log)
        {
            var wtf = this as DataIndexBase<T>;

            log.Append("HashedMultyListIndex index (");
            log.Append(wtf.Name);
            log.Append(") started check: \r\n");

            var flug = false; //dataIndex.CheckGroup(wtf.MemoryKey, this, this, trigger, table, stb);

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

        //#region ' Query '

        //public List<Query> list;

        //public bool QueryContaince(int code)
        //{
        //    return false;
        //}

        //public Query QueryGet(int code)
        //{
        //    return null;
        //}

        //public Query QueryGetOrAdd(int code, Query q)
        //{
        //    return null;
        //}

        //public IDataList<T> QueryGetDataList(Query q)
        //{
        //    return null;
        //}

        //#endregion
    }

    public class Query
    {
        
    }
}
