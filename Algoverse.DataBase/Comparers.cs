using System;

namespace Algoverse.DataBase
{
    public interface IInlineKey
    {
        byte[] KeyData { get; }
    }


    public interface IByteKeyComparer<in TKey> where TKey : IComparable<TKey>
    {
        
    }

    public interface IKeyComparer<in TKey> where TKey : IComparable<TKey>
    {
        int Compare(TKey x, int y);
    }

    interface IHashed<TKey, T> where T : Record where TKey : IComparable<TKey>
    {
        TKey GetHashKey(T obj);
    }

    interface IMaped<TKey> where TKey : IComparable<TKey>
    {
        TKey MapKey(int code);
    }

    interface IGroupComparer<TKey> where TKey : IComparable<TKey>
    {
        int CompareGroups(TKey grp, int rec);
    }

    public interface IRecordsComparer<T> where T : Record
    {
        DataIndexBase<T> Owner { get; set; }
        int CompareRecords(T xObj, int yCode);
        Field[] Fields { get; }
    }

    interface IMemoryValidator
    {
        int GetMemoryKey(object grp);
    }

    interface IRecordsGetter<T> where T : Record
    {
        T GetRecord(int code);
    }

    public interface IMapedComparer<TKey> : IKeyComparer<TKey> 
        where TKey : IComparable<TKey>
    {
        //int Compare(TKey key1, int key2);
        int Compare(int key1, int key2);
    }

    public abstract class MultyMapTrigger<TKey, TValue> : IMapedComparer<TKey> 
        where TValue : Record 
        where TKey : IComparable<TKey>
    {
        public abstract bool Filter(TValue obj);
        public abstract TKey[] GetKeys(TValue obj);
        public abstract bool KeyFilter(TKey key, TValue obj);
        public abstract int GetKeyCode(TKey key);
        public abstract int Compare(TKey x, int y);
        public abstract int Compare(int key1, int key2);
        public DataIndexBase<TValue> Owner { get; set; }
        public Field[] Fields { get; }

        protected MultyMapTrigger(Field[] fields)
        {
            Fields = fields;
        }
    }

    public abstract class MultyMapByteKeyTrigger<TKey, TValue> 
        where TValue : Record
        where TKey : IInlineKey
    {
        public abstract bool Filter(TValue obj);
        public abstract TKey[] GetKeys(TValue obj);
        public abstract bool KeyFilter(TKey key, TValue obj);

        public DataIndexBase<TValue> Owner { get; set; }
        public Field[] Fields { get; }

        protected MultyMapByteKeyTrigger(Field[] fields)
        {
            Fields = fields;
        }
    }
}
