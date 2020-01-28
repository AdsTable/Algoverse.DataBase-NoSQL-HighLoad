using System;
using System.Collections.Generic;

namespace Algoverse.DataBase
{
    public abstract class HashedListTrigger<TKey, T> : Comparer<T>, IHashed<TKey, T> where T : Record where TKey : IComparable<TKey>
    {
        public abstract bool Filter(T obj);
        public abstract TKey GetHashKey(T obj);

        public Field[] Fields { get; }

        protected HashedListTrigger(Field[] fields)
        {
            Fields = Helper.Concat(fields); ;
        }
    }
}