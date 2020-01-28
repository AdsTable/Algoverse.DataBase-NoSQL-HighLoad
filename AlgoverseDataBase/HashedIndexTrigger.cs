
using System;

namespace Algoverse.DataBase
{
    public abstract class HashedIndexTrigger<TKey, T> : IHashed<TKey, T> where T : Record  where TKey : IComparable<TKey>
    {
        public abstract bool Filter(T obj);
        public abstract TKey GetHashKey(T obj);

        public Field[] Fields { get; }

        protected HashedIndexTrigger(Field[] fields)
        {
            Fields = fields;
        }
    }
}
