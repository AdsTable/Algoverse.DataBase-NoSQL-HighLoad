using System.Collections.Generic;
using System.Diagnostics;
using Algoverse.Threading;

namespace Algoverse.DataBase
{
    public class SearchIndex<TValue> : DataIndexBase<TValue> where TValue : Record, new()
    {
        Table<TValue> table;
        SearchTable searchTable;
        MapedMultyListIndex<string, TValue> hash;
        ISearchIndexTrigger<TValue> triggerSeach;

        // Конструктор
        public SearchIndex(string name, Table<TValue> table, ISearchIndexTrigger<TValue> trigger, IRecordsComparer<TValue>[] recordsComparers)
        {
            this.Name               = name;
            this.table              = table;
            this.triggerSeach       = trigger;
            this.searchTable        = new SearchTable(table.DataBase, name + "srh");
            this.hash               = new MapedMultyListIndex<string, TValue>(name, table, new MaperByKey(searchTable), recordsComparers, this) {DontRemoveKeys = true};
            this.WordSeparators     = new []{' ', ',', '.', '\\', '/',  ':', ';', '!', '?', '#', '@', '$', '%', '^', '&', '*', '(', ')', '+', '-', '|', '_', '"', '\'', '[', ']', '{', '}', '\t', '\r', '\n'};
            this.WordMinLength      = 3;
            this.WordSuffixLength   = 2;
            this.WordPrefixLength   = 3;

            Fields = Helper.Concat(trigger.Fields);

            for (var i = 0; i < recordsComparers.Length; ++i)
            {
                recordsComparers[i].Owner = this;

                Fields = Helper.Concat(Fields, recordsComparers[i].Fields);
            }

            table.AddIndex(this);
        }

        // Символы разделители слов
        public char[]   WordSeparators      { get; set; }
        // Минимальная длинна слова
        public int      WordMinLength       { get; set; }
        // Максимальная длинна суффикса
        public int      WordSuffixLength    { get; set; }
        // Максимальная длинна окончания
        public int      WordPrefixLength    { get; set; }

        // Количество поддеревьев
        //public int MultyCount
        //{
        //    get
        //    {
        //        return (int)hash.MultyCount;
        //    } 
        //}

        // Индексатор
        public IDataList<TValue> this[string key, int listIndex]
        {
            get
            {
                return hash.GetList(key, listIndex);
            }
        }

        // Индексатор
        public IDataList<TValue> this[string key, int listIndex, List<SearchStatistic<TValue>> statistics]
        {
            get
            {
                var str = Words(key);

                for (var i = 0; i < str.Count; ++i)
                {
                    var w = str[i];

                    if (w == key)
                    {
                        continue;
                    }

                    var tmp = hash.GetList(w, listIndex);

                    if (tmp.Count == 0)
                    {
                        continue;
                    }

                    statistics.Add(new SearchStatistic<TValue>(w, tmp));
                }

                Pool<List<string>>.Release(str);

                return hash.GetList(key, listIndex);
            }
        }

        // Добавление записи
        public override void Insert(TValue obj)
        {
            if (triggerSeach.Filter(obj))
            {
                return;
            }

            var str = Words(triggerSeach.Text(obj));

            for (var i = 0; i < str.Count; ++i)
            {
                var w = str[i];
                var key = hash.KeyToCode(w);

                if (key == 0)
                {
                    var tmp = new SearchTableRecord();

                    tmp.Word = w;

                    searchTable.Insert(tmp);

                    key = tmp.Code;
                }

                hash.Insert(key, obj);

                //var key2 = hash.KeyToCode(w);

                //if (key2 != key)
                //{
                //    Debug.WriteLine("Insert key_must: " + key + ", key_fact: " + key2);
                //}
            }

            Pool<List<string>>.Release(str);
        }

        // Удаление записи
        public override void Delete(TValue obj)
        {
            var str = Words(triggerSeach.Text(obj));

            for (var i = 0; i < str.Count; ++i)
            {
                var w = str[i];
                var key = hash.KeyToCode(w);

                //if (w == "222")
                //{
                //    int bp = 0;
                //}

                if (key != 0)
                {
                    hash.Delete(key, obj);

                    //var key2 = hash.KeyToCode(w);

                    //Debug.WriteLine("Insert key: " + key + ", key_after: " + key2);
                }
            }

            Pool<List<string>>.Release(str);
        }

        // TODO Может быть стоит пересмотреть механизм обновления. С целью повышения производительности.
        public override void Update(TValue old_obj, TValue new_obj)
        {
            var wtf = this as DataIndexBase<TValue>;

            wtf.Delete(old_obj);

            new_obj.SetIndexCurrent(Id);

            wtf.Insert(new_obj);
        }

        // Очистка индекса
        public override void Clear()
        {
            searchTable.ClearTable();

            hash.Clear();
        }

        public sealed override Field[] Fields { get; }

        public override int Id { get; internal set; }

        public override bool UsedAnotherTable
        {
            get
            {
                return true;
            }
            set { throw new System.NotImplementedException(); }
        }

        public override string Name { get; protected set; }
        public override int MemoryKey { get; set; }
        public override void RegisterIndex()
        {
            hash.RegisterIndex();
        }

        // Функция возвращает слова из текста
        // TODO этот метод можно оптимизировать, так что бы не было дополнительно созданных строк
        List<string> Words(string str)
        {
            var ar = str.ToLower().Split(WordSeparators);

            #region ' Pool '

            var list = Pool<List<string>>.Get();

            if (list == null)
            {
                list = new List<string>(ar.Length);
            }
            else
            {
                list.Clear();
            }

            var ht = Pool<HashSet<string>>.Get();

            if (ht == null)
            {
                ht = new HashSet<string>();
            }
            else
            {
                ht.Clear();
            }

            #endregion
            
            for (int i = 0; i < ar.Length; ++i)
            {
                var it = ar[i];

                if (it.Length > WordMinLength && !ht.Contains(it))
                {
                    ht.Add(it);
                    list.Add(it);
                }
            }

            var c = list.Count;

            for (int i = 0; i < c; ++i)
            {
                var it = list[i];
                var sp = 0;

                // Добавляем префиксы
                for (int n = 0; n < WordPrefixLength; ++n)
                {
                    var pre = it.Substring(n + 1);

                    if (pre.Length < WordMinLength)
                    {
                        break;
                    }

                    if (!ht.Contains(pre))
                    {
                        ht.Add(pre);
                        list.Add(pre);
                    }
                }

                // Добавляем суффиксы
                for (int n = 0; n < WordSuffixLength; ++n)
                {
                    var pre = it.Substring(0, it.Length - n - 1);

                    if (pre.Length < WordMinLength)
                    {
                        break;
                    }

                    if (!ht.Contains(pre))
                    {
                        ht.Add(pre);
                        list.Add(pre);
                    }

                    sp++;
                }

                var cc = list.Count - sp;

                // Добавляем префиксы для суффиксов
                for (int j = list.Count - 1; j > cc; j--)
                {
                    it = list[j];

                    for (int n = 0; n < WordPrefixLength; ++n)
                    {
                        var pre = it.Substring(n + 1);

                        if (pre.Length < WordMinLength)
                        {
                            break;
                        }

                        if (!ht.Contains(pre))
                        {
                            ht.Add(pre);
                            list.Add(pre);
                        }
                    }
                }
            }

            Pool<HashSet<string>>.Release(ht);

            return list;
        }
        
        // Освобождение ресурсов
        public void Dispose()
        {
        }

        public class MaperByKey : IMapedComparer<string>
        {
            SearchTable table;

            public MaperByKey(SearchTable table)
            {
                this.table = table;
            }

            // WARNING Важно что GetOriginal можно не использовать пока в SearchTable нет индексов
            public int Compare(string key1, int key2)
            {
                return key1.CompareTo(table[key2].Word);
            }

            public int Compare(int key1, int key2)
            {
                return table[key1].Word.CompareTo(table[key2].Word);
            }
        }

        public override bool Check(Log log)
        {
            var flug = false;
            //var wtf = this as IDataIndex<T>;

            log.Append("Search index (");
            log.Append(this.Name);
            log.Append(") check is not implementet \r\n");

            return flug;
        }
    }

    public abstract class ISearchIndexTrigger<T> where T : Record
    {
        public abstract string Text(T obj);
        public abstract bool Filter(T obj);

        public Field[] Fields { get; }

        protected ISearchIndexTrigger(Field[] fields)
        {
            Fields = fields;
        }
    }
}