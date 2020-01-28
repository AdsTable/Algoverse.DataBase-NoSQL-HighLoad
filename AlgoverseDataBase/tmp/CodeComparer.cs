using System.Collections.Generic;

namespace Algoverse.DataBase.tmp
{
    class CodeComparer<T> : Comparer<int> where T : Record, new()
    {
        readonly Table<T> table;
        readonly Comparer<T> comparer;

        public CodeComparer(Table<T> table, Comparer<T> comparer)
        {
            this.table = table;
            this.comparer = comparer;
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

            return comparer.Compare(xo, yo);
        }
    }
}