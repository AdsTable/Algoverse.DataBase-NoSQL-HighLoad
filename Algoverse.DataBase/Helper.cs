using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Algoverse.DataBase
{
    public static unsafe class Helper
    {
        public static SYSTEM_INFO Info;

        static Helper()
        {
            GetSystemInfo(ref Info);
        }

        public static MemoryMappedFile CreateMMF(this FileStream fs, string key)
        {
            var obj = null as MemoryMappedFile;

            //Debug.Write(key);
            //Debug.Write(" : ");
            //Debug.WriteLine(fs.Name);
            
            //Console.Write(key);
            //Console.Write(" : ");
            //Console.WriteLine(fs.Name);

            int step = 0;

            while (true)
            {
                step++;

                //Debug.WriteLine("Step: " + step);
                //Console.WriteLine("Step: " + step);

                try
                {
                    // Это мегахак, при повторном создании ММФ происходит ошибка.
                    key = Guid.NewGuid().ToString("N");

                    //Debug.Write("[" + key + "] Try create: ");
                    //Console.Write("[" + key + "] Try create: ");

                    //obj = MemoryMappedFile.CreateOrOpen()

                    obj = MemoryMappedFile.CreateFromFile
                        (
                            fs,
                            key,
                            fs.Length,
                            MemoryMappedFileAccess.ReadWrite,
                            null,
                            HandleInheritability.Inheritable,
                            true
                        );

                    //Debug.WriteLine("Done");
                    //Console.WriteLine("Done");

                    break;
                }
                catch (Exception e0)
                {
                    Debug.Write("Fail: ");
                    Debug.WriteLine(e0.Message);

                    System.Threading.Thread.CurrentThread.Join(500);

                    //Console.Write("Fail: ");
                    //Console.WriteLine(e0.Message);
                    
                    //try
                    //{
                    //    Debug.Write("[" + key + "] Try open: ");
                    //    Console.Write("[" + key + "] Try open: ");

                        

                    //    obj = MemoryMappedFile.OpenExisting
                    //        (
                    //            key,
                    //            MemoryMappedFileRights.ReadWrite,
                    //            HandleInheritability.Inheritable
                    //        );

                    //    Debug.WriteLine("Done");
                    //    Console.WriteLine("Done");

                    //    break;
                    //}
                    //catch (Exception e1)
                    //{
                    //    Debug.WriteLine("Fail: ");
                    //    Debug.WriteLine(e1.Message);

                    //    Console.Write("Fail: ");
                    //    Console.WriteLine(e1.Message);
                    //}
                }

                System.Threading.Thread.Yield();
        }

            //Debug.WriteLine("");
            //Console.WriteLine("");

            return obj;
        }

        public static byte* Pointer(this MemoryMappedViewAccessor acc, long offset)
        {
            var num = offset % Info.dwAllocationGranularity;

            byte* tmp_ptr = null;

            RuntimeHelpers.PrepareConstrainedRegions();

            acc.SafeMemoryMappedViewHandle.AcquirePointer(ref tmp_ptr);

            tmp_ptr += num;

            return tmp_ptr;
        }

        public static string ReadString(this MemoryMappedViewAccessor obj, int pos)
        {
            var len = obj.ReadInt32(pos);
            var chr = new char[len];

            obj.ReadArray<char>(pos + 4, chr, 0, len);

            return new string(chr);
        }

        public static void Write(this MemoryMappedViewAccessor obj, int pos, string str, int len)
        {
            var chr = str.ToCharArray();
            var l = chr.Length < len ? chr.Length : len;

            obj.Write(pos, l);
            obj.WriteArray<char>(pos + 4, chr, 0, l);
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern void GetSystemInfo(ref SYSTEM_INFO lpSystemInfo);

        public struct SYSTEM_INFO
        {
            internal int dwOemId;
            internal int dwPageSize;
            internal IntPtr lpMinimumApplicationAddress;
            internal IntPtr lpMaximumApplicationAddress;
            internal IntPtr dwActiveProcessorMask;
            internal int dwNumberOfProcessors;
            internal int dwProcessorType;
            internal int dwAllocationGranularity;
            internal short wProcessorLevel;
            internal short wProcessorRevision;
        }

        public static FileInfo GetSafeFileInfo(string path)
        {
            var dir = Path.GetDirectoryName(path);
            var file = Path.GetFileName(path);
            var d = new DirectoryInfo(dir);
            var arr = d.GetFiles(file);

            if (arr.Length == 0)
            {
                return null;
            }

            return arr[0];
        }

        public static string ReadString(this FileStream obj)
        {
            var lb = new byte[4];

            obj.Read(lb, 0, 4);

            var len = ((lb[0] | (lb[1] << 8)) | (lb[2] << 0x10)) | (lb[3] << 0x18);
            var cb = new byte[len * 2];
            var chr = new char[len];

            obj.Read(cb, 0, len * 2);

            for (int i = 0, j = 0; i < cb.Length; i += 2, ++j)
            {
                var b0 = cb[i + 0];
                var b1 = cb[i + 1];

                char c = (char)(b0 | (b1 << 8));

                chr[j] = c;
            }

            return new string(chr);
        }

        public static void CheckPath(this string path)
        {
            var str = path.Split("\\".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);

            for (int i = 1; i < str.Length; ++i)
            {
                try
                {
                    var dd = new DirectoryInfo(string.Join("\\", str, 0, i + 1));
                    
                    if (!dd.Exists)
                    {
                        dd.Create();
                    }

                    //var d = new DirectoryInfo(path);

                    //if (d.Attributes.HasFlag(FileAttributes.ReadOnly))
                    //{
                    //    d.Attributes = d.Attributes & ~FileAttributes.ReadOnly;
                    //}
                }
                catch
                {}
            }
        }

        public static void DirectoryDelete(this string path)
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }
        }

        public static ulong CalculateHash(this string str)
        {
            var hashedValue = 3074457345618258791ul;

            for (int i = 0; i < str.Length; i++)
            {
                hashedValue += str[i];
                hashedValue *= 3074457345618258799ul;
            }
            
            return hashedValue;
        }

        public static string CalculateHashString(this string str)
        {
            var res = str.CalculateHash();

            return res.ToString("x8");
        }

        public static Field[] Concat(params Field[][] arr)
        {
            var ht = new Dictionary<int, Field>();

            for (var i = 0; i < arr.Length; ++i)
            {
                var itm = arr[i];

                for (var j = 0; j < itm.Length; ++j)
                {
                    var fld = itm[j];

                    if (!ht.ContainsKey(fld.Id))
                    {
                        ht.Add(fld.Id, fld);
                    }
                }
            }

            var ret = new Field[ht.Count];

            ht.Values.CopyTo(ret, 0);

            return ret;
        }
    }
}