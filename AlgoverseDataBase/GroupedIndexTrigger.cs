using System.Collections.Generic;

namespace Algoverse.DataBase
{
    public abstract class GroupedIndexTrigger<T> : Comparer<T> where T : Record
    {
        public abstract bool    Filter(T obj);
        public abstract int     GetGroupCode(T obj);

        public Field[] Fields { get; }

        protected GroupedIndexTrigger(Field[] fields)
        {
            Fields = fields;
        }
    }
}
