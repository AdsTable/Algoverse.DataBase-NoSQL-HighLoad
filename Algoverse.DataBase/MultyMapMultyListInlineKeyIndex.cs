using System;

namespace Algoverse.DataBase
{
    // Потокобезопасный класс  Multy Map Multy List Inline Key Index
    public class MultyMapMultyListInlineKeyIndex<TKey, TValue> : DataIndexBase<TValue> where TValue : Record, new() where TKey : IInlineKey
    { 
        // Триггер
        MultyMapByteKeyTrigger<TKey, TValue> trigger;
        // Хеш таблица
        MapedMultyListInlineKeyIndex<TKey, TValue> hash;
        // Ключ памяти
        int memoryKey;

        // Конструктор
        public MultyMapMultyListInlineKeyIndex(string key, Table<TValue> table, MultyMapByteKeyTrigger<TKey, TValue> trigger, IRecordsComparer<TValue>[] valuesTriggers)
        {
            this.trigger = trigger;
            this.Name = key;
            this.Table = table;
            this.hash = new MapedMultyListInlineKeyIndex<TKey, TValue>(key, table, valuesTriggers, this);

            trigger.Owner = this;

            Fields = Helper.Concat(trigger.Fields);

            for (var i = 0; i < valuesTriggers.Length; ++i)
            {
                valuesTriggers[i].Owner = this;

                Fields = Helper.Concat(Fields, valuesTriggers[i].Fields);
            }

            table.AddIndex(this);
        }

        // Индексатор
        public IDataList<TValue> this[TKey key, int listIndex] => hash.GetList(key, listIndex);

        // Таблица в которой состоит данный индекс
        public Table<TValue> Table { get; }

        // Добавление записи
        public override void Insert(TValue obj)
        {
            if (trigger.Filter(obj)) return;

            var keys = trigger.GetKeys(obj);

            for (var i = 0; i < keys.Length; ++i)
            {
                var key = keys[i];

                if (!trigger.KeyFilter(key, obj))
                {
                    hash.Insert(key, obj);
                }
            }
        }

        // Удаление записи
        public override void Delete(TValue obj)
        {
            var keys = trigger.GetKeys(obj);

            for (var i = 0; i < keys.Length; ++i)
            {
                // TODO Нельзя так оптимизировать эту ситуация просто потому, что фильтр может зависить от данных в других записях, которые меняются
                // независимо. Поэтому даже объект который не прошел фильтр, может присутствовать в индексе. Так же как и объект который не проходит
                // фильтр может присутствовать в фильтре. Вобще это мысль которую надо еще обдумать.
                //if (!trigger.KeyFilter(keys[i], obj))
                //{
                //    hash.Delete(trigger.GetKeyCode(keys[i]), obj);
                //}

                var key = keys[i];

                hash.Delete(key, obj);
            }
        }

        // Возможно это можно оптимизировать, проведя анализ изменения ключей и сортировки. Если это конечно имеет смысл.
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
            hash.Clear();
        }

        // Return data object by key, multy and index
        public int GetCount(TKey key, int multy)
        {
            return hash.GetCount(key, multy);
        }

        public sealed override Field[] Fields { get; }

        public override int Id { get; internal set; }

        public override bool UsedAnotherTable
        {
            get
            {
                return true;
            }
            set { throw new NotImplementedException(); }
        }

        public sealed override string Name { get; protected set; }

        public override int MemoryKey
        {
            get
            {
                return memoryKey;
            }
            set
            {
                memoryKey = value;
            }
        }

        public override void RegisterIndex()
        {
            hash.RegisterIndex();
        }

        // Полное удаление группы из индекса
        //public void RemoveGroup(TKey key)
        //{
        //    hash.RemoveGroup(trigger.GetKeyCode(key));
        //}

        // Освобождение ресурсов
        public void Dispose()
        {
        }

        public override bool Check(Log log)
        {
            var flug = false;

            log.Append("MultyKeyedDictionaryMultyList index (");
            log.Append(this.Name);
            log.Append(") check is not implementet \r\n");

            return flug;
        }
    }
}
