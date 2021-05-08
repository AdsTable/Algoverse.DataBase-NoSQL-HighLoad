using System;
using System.Collections.Generic;
using System.IO;

namespace Algoverse.DataBase
{
    internal interface ITable : IDisposable
    {
        int Count { get; }
        string Key { get; }
        PageFile File { get; }
        TableVersion CurrentVersion { get; }

        List<object> Indexes { get; }

        bool Check(Log log);
        void InitIndexes(Log log);
        int RebuildIndex(Log log);

        int RebuildIndex();

        void RegisterIndexes();

        void ResetMemoryCash();

        //CacheItem GetCurrentDataForWrite(int code, Field field);
        //byte[] GetCurrentDataForRead(int code);
    }
}