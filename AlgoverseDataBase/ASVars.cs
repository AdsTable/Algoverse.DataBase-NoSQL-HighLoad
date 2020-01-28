using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Algoverse.Threading;

namespace Algoverse.DataBase
{
    // Переменные класса ArrayStorage
    public class AsVars : Vars
    {
        ValueLockRW locker;

        public AsVars(string fullPath) : base(fullPath, 8)
        {
            locker = new ValueLockRW();
        }

        // Количество
        public unsafe long Length
        {
            get
            {
                try
                {
                    locker.ReadLock();

                    return *(long*) ptr;
                }
                finally
                {
                    locker.Unlock();
                }
            }
            set
            {
                try
                {
                    locker.WriteLock();

                    *(long*)ptr = value;
                }
                finally
                {
                    locker.Unlock();
                }
            }
        }
    }
}
