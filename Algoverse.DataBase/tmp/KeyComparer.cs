using System;

namespace Algoverse.DataBase.tmp
{
    class KeyComparer<TKey, T> : IKeyComparer<TKey> where T : Record, new() where TKey : IComparable<TKey>
    {
        readonly Table<T> table;
        readonly IHashed<TKey, T> trigger;

        public KeyComparer(Table<T> table, IHashed<TKey, T> trigger)
        {
            this.table = table;
            this.trigger = trigger;
        }

        public int Compare(TKey x, int y)
        {
            if (y == 0)
            {
                return 1;
            }

            var yo = table[y];
            var yk = trigger.GetHashKey(yo);

            return x.CompareTo(yk);            
        }
    }
}