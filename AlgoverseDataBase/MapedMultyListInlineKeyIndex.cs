using System;
using System.Text;
using Algoverse.Threading;

namespace Algoverse.DataBase
{
    public class MapedMultyListInlineKeyIndex<TKey,T> : IDataIndexBase<T>, IMemoryValidator 
        where T : Record, new() where TKey : IInlineKey
    {
        Index                   dataIndex;
        Table<T>                table;
        ValueLockRW locker = new ValueLockRW();
        DataIndexBase<T> owner;
        readonly IRecordsComparer<T>[] recordsComparers;

        public MapedMultyListInlineKeyIndex(string uniqueName, Table<T> table, IRecordsComparer<T>[] recordsComparers, DataIndexBase<T> owner)
        {
            Name           = uniqueName;
            dataIndex      = table.DataBase.Index;

            this.table          = table;
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
                var mem = dataIndex.InlineKeyMulty_GetTree(wtf.MemoryKey, key, multy, locker);

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
            var mem = dataIndex.InlineKeyMulty_GetTree(wtf.MemoryKey, key, 0, locker);

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
            var mem = dataIndex.InlineKeyMulty_GetTree(wtf.MemoryKey, key, 0, locker);

            return mem != 0;
        }

        // Return data object by key, multy and index
        public int GetCount(TKey key, int multy)
        {
            var wtf = this as IDataIndexBase<T>;
            var mem = dataIndex.InlineKeyMulty_GetTree(wtf.MemoryKey, key, multy, locker);

            if (mem != 0)
            {
                return dataIndex.Count(mem, locker);
            }

            return 0;
        }

        #region ' IDataIndexBase<T> members '

        // Memory address of the root element
        int IDataIndexBase<T>.MemoryKey { get; set; }

        public void RegisterIndex()
        {
            dataIndex.RegisterIndex(this);
        }

        // 
        public void Insert(TKey key, T obj)
        {
            var wtf = this as IDataIndexBase<T>;

            dataIndex.InlineKeyMulty_Insert(wtf.MemoryKey, key, obj, recordsComparers, locker);
        }

        // 
        public void Delete(TKey key, T obj)
        {
            var wtf = this as IDataIndexBase<T>;

            dataIndex.InlineKeyMulty_Delete(wtf.MemoryKey, key, obj, recordsComparers, DontRemoveKeys, locker);
        }

        // Очистка индекса
        public void Clear()
        {
            var wtf = this as IDataIndexBase<T>;

            dataIndex.InlineKeyMulty_Clear(wtf.MemoryKey, recordsComparers.Length, locker);
        }

        #endregion

        //
        public int GetMemoryKey(object arg)
        {
            var args = arg as object[];
            var key = (TKey)args[0];
            var multy = (int)args[1];

            var wtf = this as IDataIndexBase<T>;
            var mem = dataIndex.InlineKeyMulty_GetTree(wtf.MemoryKey, key, multy, locker);

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
