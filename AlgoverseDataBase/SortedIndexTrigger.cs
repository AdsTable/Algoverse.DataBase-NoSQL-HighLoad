using System.Collections.Generic;

namespace Algoverse.DataBase
{
    public abstract class SortedIndexTrigger<T> : Comparer<T> where T : Record
    {
        public abstract bool Filter(T obj);
        public Field[] Fields { get; }

        protected SortedIndexTrigger(Field[] fields)
        {
            Fields = fields;
        }
    }
}
