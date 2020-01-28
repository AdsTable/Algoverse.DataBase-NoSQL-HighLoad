namespace Algoverse.DataBase
{
    public interface IDataListExt<T> : IDataList<T> where T : Record
    {
        T       GetByCode(int code);
        bool    Contains(int code);
        T[]     ToArray();
    }

    public interface IDataList<T> where T : Record
    {
        int Count           { get; }
        T   this[int index] { get; }
    }
}
