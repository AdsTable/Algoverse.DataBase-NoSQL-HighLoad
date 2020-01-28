using Algoverse.DataBase.Collections;
using Algoverse.Threading;
using System.Collections.Generic;

namespace Algoverse.DataBase.Threading
{
    public class SortedListSafe<T>
    {
        SortedList<T> list;
        ValueLockRW locker = new ValueLockRW();

        public SortedListSafe(ISortedListComparer<T> comp)
        {
            list = new SortedList<T>(comp);
        }

        public int this[int index]
        {
            get
            {
                try
                {
                    locker.ReadLock();
                    
                    return list[index];
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


