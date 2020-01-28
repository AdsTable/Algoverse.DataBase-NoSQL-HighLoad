using System;
using Algoverse.Threading.Collections;

namespace Algoverse.DataBase
{
    // todo Сделеть строки и массивы переменной длинны. Так же параметр указывающий что данное поле должно храниться в отдельном файле.
    public class TableVersion
    {
        public static DictionarySafe<Type, int>  ht = new DictionarySafe<Type, int>();

        public TableVersion(int version, Field[] structure, PageFileIOMode mode)
        {
            Version = version;
            Structure = structure;
            Mode = mode;

            for (int i = 0; i < Structure.Length; ++i)
            {
                var str = Structure[i];

                str.Id = i;
                str.Offset = RecordSize;

                HasStorage |= str.IsStorage;

                RecordSize += str.Size;
            }

            // crc32
            if ((Mode & PageFileIOMode.CRC32) == PageFileIOMode.CRC32)
            {
                RecordSize += 4 /*crc32*/ + 8 /*last write date-time*/;
            }
            else
            {
                RecordSize += 8 /*last write date-time*/;
            }
        }

        // Числовой номер версии структуры данных таблицы
        public int Version { get; }

        // список типов, в той последовательности, в которой 
        // происходит чтение или запись данных методами Read \ Write.
        // На основе этих данных расчитывается значение RecordSize
        public Field[] Structure { get; }

        // Параметры хранения записи
        public PageFileIOMode Mode { get; }

        // Указывает, что хотябы одно поле обращается к хранилищу данных переменной длинны
        public bool HasStorage { get; internal set; }

        // Размер записи на диске
        public int RecordSize { get; internal set; }

        // Создает пустой массив данных в зависимости от режима записи
        internal byte[] CreateEmptyData()
        {
            // Дата последней записи на диск
            var size = RecordSize - 8;

            // Поле crc32
            if (Mode == PageFileIOMode.CRC32) //|| Mode == PageFileIOMode.
            {
                size -= 4;
            }

            return new byte[size];
        }
    }
}
