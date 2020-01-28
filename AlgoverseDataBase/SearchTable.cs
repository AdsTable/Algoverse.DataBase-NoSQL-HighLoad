using System;

namespace Algoverse.DataBase
{
    public class SearchTable : Table<SearchTableRecord>
    {
        static TableVersion curent;
        public static Field Word { get; }
        public static Field Counter { get; }

        static SearchTable()
        {
            Word = new Field("Word", typeof (string));
            Counter = new Field("Word", typeof(int));

            curent = new TableVersion(1, new[] { Word, Counter }, PageFileIOMode.CRC32);
        }

        public SearchTable(DataBase Base, string key) : base(Base, curent, key)
        {
            
        }


        //public override Type[] Structure()
        //{
        //    return new []{ typeof(string), typeof(int) };
        //}

        //public override SearchTableRecord Read(IReader device)
        //{
        //    return new SearchTableRecord (device.ReadString());
        //}

        //public override void Write(IWriter device, SearchTableRecord record)
        //{
        //    device.Write(record.Word);
        //}
        public override SearchTableRecord Create()
        {
            return new SearchTableRecord();
        }
    }
}
