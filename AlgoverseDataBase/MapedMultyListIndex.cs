using System;
using System.Text;
using Algoverse.Threading;

namespace Algoverse.DataBase
{
    public class MapedMultyListIndex<TKey,T> : IDataIndexBase<T>, IMemoryValidator 
        where T : Record, new() where TKey : IComparable<TKey>
    {
        Index                   dataIndex;
        Table<T>                table;
        IMapedComparer<TKey>    trigger;
        DataIndexBase<T> owner;
        readonly IRecordsComparer<T>[] recordsComparers;
        ValueLockRW locker = new ValueLockRW();

        public MapedMultyListIndex(string uniqueName, Table<T> table, IMapedComparer<TKey> trigger, IRecordsComparer<T>[] recordsComparers, DataIndexBase<T> owner)
        {
            Name           = uniqueName;
            dataIndex      = table.DataBase.Index;

            this.table          = table;
            this.trigger        = trigger;
            this.recordsComparers = recordsComparers;
            this.owner = owner;

            Fields = new Field[0];

            for (var i = 0; i < recordsComparers.Length; ++i)
            {
                recordsComparers[i].Owner = owner;

                Fields = Helper.Concat(Fields, recordsComparers[i].Fields);
            }

            RegisterIndex();
        }

        public Field[] Fields { get; }

        // The unique name this index from data structure
        public string Name { get; set; }

        // Flug indicate what possible remove group keys
        public bool DontRemoveKeys { get; set; }

        // Return data object by key, multy and index
        public T this[TKey key, int multy, int index]
        {
            get
            {
                var wtf = this as IDataIndexBase<T>;
                var mem = dataIndex.GetFreeMultyTree(wtf.MemoryKey, key, trigger, multy, locker);

                if (mem != 0)
                {
                    var code = dataIndex.GetByIndex(index, mem, locker);

                    return table[code];
                }

                return null;
            }
        }

        // Return data list
        public IDataList<T> GetList(TKey key, int multy)
        {
            var list = new RecordsListView<T>(new object[] { key, multy }, dataIndex, table, recordsComparers[multy], this, owner, locker);

            return list;
        }

        // Check contains the data object is in the index
        public bool Contains(TKey key, T obj)
        {
            var ret = false;
            var wtf = this as IDataIndexBase<T>;
            var mem = dataIndex.GetFreeMultyTree(wtf.MemoryKey, key, trigger, 0, locker);

            if (mem != 0)
            {
                ret = dataIndex.Contains(obj, mem, recordsComparers[0], locker);
            }

            return ret;
        }

        // Check contains group key
        public bool ContainsKey(TKey key)
        {
            var wtf = this as IDataIndexBase<T>;
            var mem = dataIndex.GetFreeMultyTree(wtf.MemoryKey, key, trigger, 0, locker);

            return mem != 0;
        }

        // Find code
        public int KeyToCode(TKey key)
        {
            var wtf = this as IDataIndexBase<T>;
            var code = dataIndex.GetFreeMultyCode(wtf.MemoryKey, key, trigger, locker);

            return code;
        }

        #region ' IDataIndexBase<T> members '

        // Memory address of the root element
        int IDataIndexBase<T>.MemoryKey { get; set; }

        public void RegisterIndex()
        {
            dataIndex.RegisterIndex(this);
        }

        // 
        public void Insert(int key, T obj)
        {
            var wtf = this as IDataIndexBase<T>;

            dataIndex.InsertToFreeMulty(wtf.MemoryKey, key, obj, trigger, recordsComparers, locker);
        }

        // 
        public void Delete(int key, T obj)
        {
            var wtf = this as IDataIndexBase<T>;

            dataIndex.DeleteFromFreeMulty(wtf.MemoryKey, key, obj, trigger, recordsComparers, DontRemoveKeys, locker);
        }

        // Очистка индекса
        public void Clear()
        {
            var wtf = this as IDataIndexBase<T>;

            dataIndex.ClearMulty(wtf.MemoryKey, recordsComparers.Length, locker);
        }

        #endregion

        //
        public int GetMemoryKey(object arg)
        {
            var args = arg as object[];
            var key = (TKey)args[0];
            var multy = (int)args[1];

            var wtf = this as IDataIndexBase<T>;
            var mem = dataIndex.GetFreeMultyTree(wtf.MemoryKey, key, trigger, multy, locker);

            return mem;
        }

        //
        public bool Check(StringBuilder stb)
        {
            var wtf = this as IDataIndexBase<T>;

            stb.Append("HashedMultyListIndex index (");
            stb.Append(wtf.Name);
            stb.Append(") started check: \r\n");

            var flug = false; //dataIndex.CheckGroup(wtf.MemoryKey, this, this, trigger, table, stb);

            if (flug)
            {
                stb.Append("Fail!\r\n");
            }
            else
            {
                stb.Append("Done\r\n");
            }

            return flug;
        }
    }
}
