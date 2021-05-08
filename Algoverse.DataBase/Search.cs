namespace Algoverse.DataBase
{
    public class SearchStatistic<T> where T : Record
    {
        readonly IDataList<T> list;
        public string Key { get; set; }

        public IDataList<T> List
        {
            get
            {
                return list;
            }
        }


        public SearchStatistic(string key, IDataList<T> list)
        {
            this.list = list;
            Key = key;
        }
    }
}
