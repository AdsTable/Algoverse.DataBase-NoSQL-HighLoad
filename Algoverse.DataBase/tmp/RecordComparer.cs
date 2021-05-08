using System.Collections.Generic;

namespace Algoverse.DataBase.tmp
{
    class RecordsComparer<T> : IRecordsComparer<T> where T : Record, new()
    {
        readonly Table<T> table;
        readonly Comparer<T> comparer;

        public RecordsComparer(Table<T> table, Comparer<T> comparer)
        {
            this.table = table;
            this.comparer = comparer;
        }

        public int CompareRecords(T xObj, int yCode)
        {
            if (yCode == 0)
            {
                return 1;
            }

            var yo = table[yCode];

            return comparer.Compare(xObj, yo);
        }
    }
}
