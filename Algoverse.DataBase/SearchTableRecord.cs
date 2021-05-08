namespace Algoverse.DataBase
{
    public class SearchTableRecord : Record
    {
        //public SearchTableRecord(string word)
        //{
        //    Word    = word;
        //}

        //public string   Word        { get; set; }

        //public string Value
        //{
        //    get
        //    {
        //        return GetString(Market.DataBase.Atom.Value);
        //    }
        //    private set
        //    {
        //        Set(Market.DataBase.Atom.Value, value);
        //    }
        //}

        public string Word
        {
            get
            {
                return GetString(SearchTable.Word);
            }
            set
            {
                Set(SearchTable.Word, value);
            }
        }
    }
}
