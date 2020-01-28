using System;
using System.Collections.Generic;
using System.Threading;
using Algoverse.DataBase.Threading;

namespace Algoverse.DataBase
{
    public class Pool<T> where T : class
    {
        #region ' Static '

        static Dictionary<int, Dictionary<Type, Queue<T>>> ht_glob;
        static ValueLock locker_glob;

        static Pool()
        {
            ht_glob = new Dictionary<int, Dictionary<Type, Queue<T>>>(256);
            locker_glob = new ValueLock();
        }

        public static T Get()
        {
            var idt = Thread.CurrentThread.ManagedThreadId;

            Dictionary<Type, Queue<T>> dic;

            if (!ht_glob.ContainsKey(idt))
            {
                dic = new Dictionary<Type, Queue<T>>();

                locker_glob.Lock();

                ht_glob.Add(idt, dic);

                locker_glob.Unlock();
            }
            else
            {
                dic = ht_glob[idt];
            }

            T obj = null;

            if (dic.ContainsKey(typeof (T)))
            {
                var q = dic[typeof (T)];

                if (q.Count > 0)
                {
                    obj = q.Dequeue();
                }
            }

            return obj;
        }

        public static void Release(T obj)
        {
            var idt = Thread.CurrentThread.ManagedThreadId;

            Dictionary<Type, Queue<T>> dic;

            if (!ht_glob.ContainsKey(idt))
            {
                dic = new Dictionary<Type, Queue<T>>();

                locker_glob.Lock();

                ht_glob.Add(idt, dic);

                locker_glob.Unlock();
            }
            else
            {
                dic = ht_glob[idt];
            }

            Queue<T> q;

            if (dic.ContainsKey(typeof (T)))
            {
                q = dic[typeof (T)];
            }
            else
            {
                q = new Queue<T>();

                dic.Add(typeof (T), q);
            }

            q.Enqueue(obj);
        }

        #endregion

        #region ' Instance '

        Dictionary<int, Queue<T>> ht;
        ValueLock locker;

        public Pool()
        {
            ht = new Dictionary<int, Queue<T>>(256);
            locker = new ValueLock();
        }

        public T GetInstance()
        {
            var idt = Thread.CurrentThread.ManagedThreadId;

            Queue<T> q;

            if (!ht.ContainsKey(idt))
            {
                q = new Queue<T>();

                locker.Lock();

                ht.Add(idt, q);

                locker.Unlock();
            }
            else
            {
                q = ht[idt];
            }

            T obj = null;

            if (q.Count > 0)
            {
                obj = q.Dequeue();
            }

            return obj;
        }

        public void ReleaseInstance(T obj)
        {
            var idt = Thread.CurrentThread.ManagedThreadId;

            Queue<T> q;

            if (!ht.ContainsKey(idt))
            {
                q = new Queue<T>();

                locker.Lock();

                ht.Add(idt, q);

                locker.Unlock();
            }
            else
            {
                q = ht[idt];
            }

            q.Enqueue(obj);
        }

        public List<T> GetInstanceList()
        {
            var list = new List<T>();

            foreach (var itm0 in ht.Values)
            {
                list.AddRange(itm0);
            }

            return list;
        }
        #endregion
    }
}
