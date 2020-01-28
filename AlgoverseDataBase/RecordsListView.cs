using System;
using Algoverse.Threading;

namespace Algoverse.DataBase
{
    public class RecordsListView<T> : IDataListExt<T> where T : Record
    {
        int                         memoryKey;
        object                      groupKey;
        Index                       dataIndex;
        protected Table<T>          table;
        IRecordsComparer<T>         comparer;
        IMemoryValidator            validator;
        DataIndexBase<T>            owner;
        ValueLockRW                 locker;

        internal RecordsListView(object groupKey, Index dataIndex, Table<T> table, IRecordsComparer<T> comparer, IMemoryValidator validator, DataIndexBase<T> owner, ValueLockRW locker)
        {
            this.dataIndex = dataIndex;
            this.table     = table;
            this.comparer  = comparer;
            this.validator = validator;
            this.owner = owner;
            this.locker = locker;
            this.groupKey  = groupKey;
        }

        public virtual T this[int index]
        {
            get
            {
                memoryKey = validator.GetMemoryKey(groupKey);

                if (memoryKey == 0)
                {
                    return table[0];
                }

                var code = dataIndex.GetByIndex(index, memoryKey, locker);

                return table[code];
            }
        }

        // Count data objects in index
        public virtual int Count
        {
            get
            {
                memoryKey = validator.GetMemoryKey(groupKey);

                if (memoryKey == 0)
                {
                    return 0;
                }

                return dataIndex.Count(memoryKey, locker);
            }
        }

        //
        public bool Contains(int code)
        {
            return Contains(table.GetOriginal(code, owner));
        }

        //
        public bool Contains(T obj)
        {
            memoryKey = validator.GetMemoryKey(groupKey);

            if (memoryKey == 0)
            {
                return false;
            }

            return dataIndex.Contains(obj, memoryKey, comparer, locker);
        }

        // 
        public T GetByCode(int code)
        {
            return table[code];
        }

        // 
        public T[] ToArray()
        {
            throw new NotImplementedException();
        }

#if DEBUG
        public void DebugPrint()
        {
            if (memoryKey == 0)
            {
                memoryKey = validator.GetMemoryKey(groupKey);
            }

            dataIndex.PrintToDebug(memoryKey);
        }

        public void DebugPrint(int mem)
        {
            dataIndex.PrintToDebug(mem);
        }

#endif

        public override string ToString()
        {
            return "Count: " + Count;
        }
    }
}
