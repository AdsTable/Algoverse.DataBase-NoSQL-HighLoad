using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Algoverse.DataBase
{
    public unsafe class Vars : IDisposable
    {
        #region ' Core '

        MemoryMappedFile file;
        MemoryMappedViewAccessor header_mma;
        protected byte* ptr;
        FileStream fs;
        long length;
        bool disposed;

        public Vars(string fullPath, int size)
        {
            Key = fullPath.CalculateHashString();

            var inf = new FileInfo(fullPath);

            // Создаем заголовок файла.
            if (!inf.Exists)
            {
                fs = new FileStream(fullPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                fs.SetLength(size);

                InitFile();
            }
            else
            {
                fs = new FileStream(fullPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite); //

                InitFile();
            }
        }

        ~Vars()
        {
            Dispose();
        }

        #endregion
        
        // Ключ загруженного в память файла
        public string Key { get; }

        // Инициализая файла
        void InitFile()
        {
            if (header_mma != null)
            {
                header_mma.SafeMemoryMappedViewHandle.ReleasePointer();
                header_mma.Dispose();
            }

            file?.Dispose();

            file = fs.CreateMMF(Key);

            header_mma = file.CreateViewAccessor(0, fs.Length, MemoryMappedFileAccess.ReadWrite);
            ptr = header_mma.Pointer(0);
        }

        #region ' Dispose '

        // Освобождение ресурсов
        public void Dispose()
        {
            if (!disposed)
            {
                if (header_mma != null)
                {
                    header_mma.SafeMemoryMappedViewHandle.ReleasePointer();
                    header_mma.Dispose();
                }

                file?.Dispose();
                fs?.Close();

                disposed = true;
            }
        }

        #endregion
    }
}
