namespace Algoverse.DataBase
{
    public abstract class DataIndexBase<T> : IVerifiableIndex, IDataIndexBase<T> where T : Record
    {
        public abstract void Insert(T obj);
        public abstract void Delete(T obj);
        public abstract void Update(T oldObj, T newObj);
        public abstract void Clear();
        public abstract Field[] Fields { get; }
        public abstract int Id { get; internal set; }
        public abstract bool UsedAnotherTable { get; set; }
        public abstract string Name { get; protected set; }
        public abstract int MemoryKey { get; set; }
        public abstract bool Check(Log log);
        public abstract void RegisterIndex();
    }

    public interface IDataIndexBase<T> where T : Record
    {
        string  Name                { get; }
        int     MemoryKey           { get; set; }
        void    RegisterIndex();
    }

    public interface IVerifiableIndex
    {
        bool Check(Log log);
    }

    public interface IVerifiableTable
    {
        bool CheckIndex(Log log);
        bool CheckTable(Log log);
    }
}
