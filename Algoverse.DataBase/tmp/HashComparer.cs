using System;
using System.Collections.Generic;

namespace Algoverse.DataBase.tmp
{
    class HashComparer<TKey, T> : Comparer<int>, IRecordsComparer<T> where T : Record, new() where TKey : IComparable<TKey>
    {
        readonly Table<T> table;
        readonly HashedIndexTrigger<TKey, T> trigger;
        
        public HashComparer(Table<T> table, HashedIndexTrigger<TKey, T> trigger)
        {
            this.table = table;
            this.trigger = trigger;
        }

        public int CompareRecords(T xObj, int yCode)
        {
            if (yCode == 0)
            {
                return 1;
            }

            var yo = table[yCode];

            var xk = trigger.GetHashKey(xObj);
            var yk = trigger.GetHashKey(yo);

            return xk.CompareTo(yk);
        }

        public override int Compare(int x, int y)
        {
            if (x == 0)
            {
                return -1;
            }

            if (y == 0)
            {
                return 1;
            }

            var xo = table[x];
            var yo = table[y];

            var xk = trigger.GetHashKey(xo);
            var yk = trigger.GetHashKey(yo);

            return xk.CompareTo(yk);
        }
    }
}