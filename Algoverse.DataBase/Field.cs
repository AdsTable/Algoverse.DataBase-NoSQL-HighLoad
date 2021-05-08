using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Algoverse.DataBase
{
    public class Field
    {
        public Field(string name, Type type)
        {
            if (name == null || name.Length == 0)
            {
                throw new Exception("Name is null or empty");
            }

            Name = name;
            Type = type;
            IsStorage = type == typeof(string) || type.IsArray;

            if (type == typeof (bool) || type == typeof (byte) || type == typeof (sbyte))
            {
                Size = 1;
            }
            else if (type == typeof (short) || type == typeof (ushort) || type == typeof (char))
            {
                Size = 2;
            }
            else if (type == typeof (int) || type == typeof (uint) || type == typeof (float))
            {
                Size = 4;
            }
            else if (type == typeof(long) || type == typeof(ulong) || type == typeof(double) || type == typeof(string) || type == typeof(DateTime) || type == typeof(TimeSpan) || type.IsArray)
            {
                Size = 8;
            }
            else if (type == typeof (decimal))
            {
                Size = 16;
            }
            else
            {
                Size = Marshal.SizeOf(type);
            }
        }

        public int Id { get; internal set; }
        public bool IsStorage { get; }
        public string Name { get; }
        public Type Type { get; set; }
        public ArrayStorage Storage { get; internal set; }
        public int Size { get; }
        public int Offset { get; internal set; }
        internal ITable Table { get; set; }
    }
}