using System;

namespace Algoverse.DataBase
{
    // Потокобезопасный класс
    public class MultyMapMultyListIndex<TKey, TValue> : DataIndexBase<TValue> 
        where TValue : Record, new() where TKey : IComparable<TKey>
    {
        // Триггер
        readonly MultyMapTrigger<TKey, TValue> trigger;
        // Хеш таблица
        MapedMultyListIndex<TKey, TValue> hash;
        int memoryKey;

        // Конструктор
        public MultyMapMultyListIndex(string key, Table<TValue> table, MultyMapTrigger<TKey, TValue> trigger, IRecordsComparer<TValue>[] valuesTriggers)
        {
            this.trigger = trigger;
            this.Name = key;
            this.Table = table;
            this.trigger = trigger;
            this.hash = new MapedMultyListIndex<TKey, TValue>(key, table, trigger, valuesTriggers, this);

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
        public IDataList<TValue> this[TKey key, int listIndex]
        {
            get
            {
                return hash.GetList(key, listIndex);
            }
        }

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

                var code = trigger.GetKeyCode(key);
                if (code == 1150)
                {
                    int bp = 0;
                }

                if (!trigger.KeyFilter(key, obj))
                {
                    //var code = trigger.GetKeyCode(key);

                    hash.Insert(code, obj);
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

                var key = trigger.GetKeyCode(keys[i]);

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
