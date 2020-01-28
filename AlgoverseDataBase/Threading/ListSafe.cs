using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Algoverse.Threading;

namespace Algoverse.DataBase.Threading
{
    public class ListSafe<T>
    {
        List<T> list;
        ValueLockRW locker;

        public ListSafe()
        {
            list = new List<T>();
            locker = new ValueLockRW();
        }

        public int Count => list.Count;

        public T this[int index]
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
            set
            {
                try
                {
                    locker.WriteLock();

                    list[index] = value;
                }
                finally
                {
                    locker.Unlock();
                }
            }
        }

        public void Add(T obj)
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
        }

        public void Remove(T obj)
        {
            try
            {
                locker.WriteLock();

                list.Remove(obj);
            }
            finally
            {
                locker.Unlock();
            }
        }

        public void RemoveAt(int index)
        {
            try
            {
                locker.WriteLock();

                list.RemoveAt(index);
            }
            finally
            {
                locker.Unlock();
            }
        }
    }
}
