using System;
using System.Diagnostics;
using System.Threading;
using Algoverse.Threading;

namespace Algoverse.DataBase
{
    public abstract unsafe class Record
    {
        protected Record()
        {
            Code = 0;
            WriteTime = 0;
        }

        // Уникальный код объекта, который соответствует странице в файле базы данных.
        public int Code { get; internal set; }

        // Время последней последней записи на носитель
        public long WriteTime { get; internal set; }

        // Все данные записи
        internal byte[] Data
        {
            get => data;
            set
            {
                data = value;

                Loaded();
            }
        }

        // Время последнего доступа. По этому полю таблицы строят кеш в памяти. При очистке кеша используется это поле.
        internal int RatingTime { get; set; }

        // Флаг содержит некоторую информацию о состоянии записи
        public RecordFlag Flag { get; internal set; }

        // Функция вызывается каждый раз после того, как были загруженны данные этой записи
        protected virtual void Loaded()
        {
        }

        #region ' Original '

        // Здесь хранятся оригинальные данные если запись изменяется. Идея такова что массив Data никогда не меняется. В случае
        // если произошли изменения создается объект оригинала и массив Data перемещается туда. На его место создается новый массив, он то и меняется.
        OriginalReord original;
        byte[] data;

        // Данный метод вызывается перед изменением записей
        void RegisterChange(Field field)
        {
            if (Code < 1 || (Flag & RecordFlag.Reserved) == RecordFlag.Reserved)
            {
                if (Data == null)
                {
                    //Data = new byte[TableVersion.ht[this.GetType()]];
                    Data = field.Table.CurrentVersion.CreateEmptyData();
                }

                return;
            }

            var cache = original;
            var flag = false;
            
            if (cache == null)
            {
                var tmp = new OriginalReord(Code, Data, new bool[field.Table.CurrentVersion.Structure.Length]);

                while (cache == null)
                {
                    if (Interlocked.CompareExchange(ref original, tmp, cache) == cache)
                    {
                        cache = tmp;
                        flag = true;

                        break;
                    }

                    cache = original;
                }
            }

            ValueLock.Lock(ref cache.Lock);

            if (cache != original)
            {
                RegisterChange(field);

                cache.Lock = 0;
            }
            else
            {
                if (flag)
                {
                    var tmp = field.Table.CurrentVersion.CreateEmptyData();

                    Array.Copy(Data, tmp, tmp.Length);

                    Data = tmp;
                }

                original.Fields[field.Id] = true;
            }
        }

        // Оригинальные данные зануляются после каждого обновления
        internal void RemoveUpdatedOriginal()
        {
            var tmp = original;

            if (tmp == null)
            {
                return;
            }

            // Оригинал заблокирован, поэтому удаление потокобезопастно
            original = null;
            // Разблокировали
            tmp.Lock = 0;
        }

        // Получения оригинальных данных для обновления индексов.
        internal OriginalReord GetOrignForUpdate()
        {
            var cache = original;

            if (cache == null)
            {
                return null;
            }

            ValueLock.Lock(ref cache.Lock);

            if (cache != original)
            {
                cache.Lock = 0;

                cache = GetOrignForUpdate();
            }

            return cache;
        }

        // Метод возвращает данные для чтения индекса. 
        internal byte[] GetOrignalForRead(int id)
        {
            var cache = original;
            var data = Data;

            // IndexIsCurrent выставляется в true только если запись заблокированна, поэтому метод потокобезопасный.
            if (cache == null || cache.IndexIsCurrent != null && cache.IndexIsCurrent[id])
            {
                return data;
            }

            return cache.Data;
        }

        // Метод
        internal void SetIndexCurrent(int id)
        {
            var cache = original;

            if (cache != null)
            {
                cache.IndexIsCurrent[id] = true;
            }
        }

        #endregion
        
        #region ' Get '

        protected bool GetBoolean(Field field)
        {
            return Data[field.Offset] == 1;
        }

        protected  byte GetByte(Field field)
        {
            return Data[field.Offset];
        }

        protected sbyte GetSByte(Field field)
        {
            return (sbyte)Data[field.Offset];
        }

        protected short GetInt16(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (short*)ptr;

                return *p;
            }
        }
        
        protected ushort GetUInt16(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (ushort*)ptr;

                return *p;
            }
        }

        protected ushort GetChar(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (char*)ptr;

                return *p;
            }
        }

        protected  int GetInt32(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (int*)ptr;

                return *p;
            }
        }

        protected  uint GetUInt32(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (uint*)ptr;

                return *p;
            }
        }

        protected  long GetInt64(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                return *p;
            }
        }

        protected  ulong GetUInt64(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (ulong*)ptr;

                return *p;
            }
        }

        protected float GetSingle(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (float*)ptr;

                return *p;
            }
        }

        protected double GetDouble(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (double*)ptr;

                return *p;
            }
        }

        protected decimal GetDecimal(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (decimal*)ptr;

                return *p;
            }
        }

        protected DateTime GetDateTime(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                var ret = new DateTime(*p);

                return ret;
            }
        }

        protected long GetDateTimeAsTicks(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                return *p;
            }
        }

        protected TimeSpan GetTimeSpan(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                var ret = new TimeSpan(*p);

                return ret;
            }
        }

        protected long GetTimeSpanAsTicks(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                return *p;
            }
        }

        protected string GetString(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;
                
                var ret = field.Storage.ReadString(*p);

                return ret;
            }
        }

        protected byte[] GetStringAsByteArray(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                var ret = field.Storage.ReadBytes(*p);

                return ret;
            }
        }

        protected byte[] GetByteArray(Field field)
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                var ret = field.Storage.ReadBytes(*p);

                return ret;
            }
        }

        protected T[] GetByteArray<T>(Field field) where T : struct
        {
            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                var ret = field.Storage.Read<T>(*p);

                return ret;
            }
        }

        #endregion
        
        #region ' Set '

        protected void Set(Field field, bool val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (bool*)ptr;

                *p = val;
            }

            if (original != null) original.Lock = 0;
        }

        protected void Set(Field field, byte val)
        {
            RegisterChange(field);

            Data[field.Offset] = val;

            if (original != null) original.Lock = 0;
        }

        protected void Set(Field field, sbyte val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (sbyte*)ptr;

                *p = val;
            }

            if (original != null) original.Lock = 0;
        }

        protected void Set(Field field, short val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (short*)ptr;

                *p = val;
            }

            if (original != null) original.Lock = 0;
        }

        protected void Set(Field field, ushort val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (ushort*)ptr;

                *p = val;
            }

            if (original != null) original.Lock = 0;
        }

        protected void Set(Field field, char val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (char*)ptr;

                *p = val;
            }

            if (original != null) original.Lock = 0;
        }

        protected void Set(Field field, int val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (int*)ptr;

                *p = val;
            }

            if (original != null) original.Lock = 0;
        }

        protected void Set(Field field, uint val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (uint*)ptr;

                *p = val;
            }

            if (original != null) original.Lock = 0;
        }
        
        protected void Set(Field field, long val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                *p = val;
            }

            if (original != null) original.Lock = 0;
        }
        
        protected void Set(Field field, ulong val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (ulong*)ptr;

                *p = val;
            }

            if (original != null) original.Lock = 0;
        }

        protected void Set(Field field, float val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (float*)ptr;

                *p = val;
            }

            if (original != null) original.Lock = 0;
        }
        
        protected void Set(Field field, double val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (double*)ptr;

                *p = val;
            }

            if (original != null) original.Lock = 0;
        }

        protected void Set(Field field, decimal val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (decimal*)ptr;

                *p = val;
            }

            if (original != null) original.Lock = 0;
        }

        protected void Set(Field field, DateTime val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                *p = val.Ticks;
            }

            if (original != null) original.Lock = 0;
        }

        protected void SetDateTimeAsTicks(Field field, long val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                *p = val;
            }

            if (original != null) original.Lock = 0;
        }

        protected void Set(Field field, TimeSpan val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                *p = val.Ticks;
            }

            if (original != null) original.Lock = 0;
        }

        protected void SetTimeSpanAsTicks(Field field, long val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                *p = val;
            }

            if (original != null) original.Lock = 0;
        }

        protected void Set(Field field, string val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                var v = field.Storage.Write(*p, val);

                *p = v;
            }

            if (original != null)
            {
                original.Lock = 0;
            }
        }
        
        protected void Set(Field field, byte[] val)
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                var v = field.Storage.Write(*p, val);

                *p = v;
            }

            if (original != null) original.Lock = 0;
        }

        protected void Set<T>(Field field, T[] val) where T : struct
        {
            RegisterChange(field);

            fixed (byte* ptr = &Data[field.Offset])
            {
                var p = (long*)ptr;

                var v = field.Storage.Write(*p, val);

                *p = v;
            }

            if (original != null) original.Lock = 0;
        }

        #endregion
    }

    public enum RecordFlag : byte
    {
        Normal = 0,
        Reserved = 1
    }
}

/* 
 
 
 Чтение запись на диск
 
 
 Многопоточное апдейт
 Многопоточное изменение поля
  
 Метаданные бекапов + версия данных
 
 Инит функция структуры
 Удаление старых строк
 Кеширование строк
 * 
 */