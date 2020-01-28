using Algoverse.DataBase.Collections;
using Algoverse.Threading;
using System.Collections.Generic;
using System;

namespace Algoverse.DataBase.Threading
{
    public class SortedDataListSafe<T> : IDataList<T> where T : Record
    {
        Table<T> table;
        SortedList<T> list;
        ValueLockRW locker = new ValueLockRW();

        public SortedDataListSafe(ISortedListComparer<T> comp, Table<T> table)
        {
            this.table = table;
            list = new SortedList<T>(comp);
        }

        public T this[int index]
        {
            get
            {
                try
                {
                    locker.ReadLock();
                    
                    return table[list[index]];
                }
                finally
                {
                    locker.Unlock();
                }
            }
        }

        public int Count
        {
            get
            {
                return list.Count;
            }
        }

        public bool Add(T obj)
        {
            try
            {
                locker.WriteLock();

                list.Add(obj);
            }
            finally
            {
                locker.Unlock();
            }

            return false;
        }

        public bool Remove(T obj)
        {
            try
            {
                locker.WriteLock();

                //if (ht.ContainsKey(key))
                //{
                    list.Remove(obj);

                //    return true;
                //}
            }
            finally
            {
                locker.Unlock();
            }

            return false;
        }
    }
}


