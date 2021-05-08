using System;
using System.Threading;

namespace Algoverse.DataBase
{
    public class Job
    {
        public static void Run(Action a)
        {
            var ts = new ThreadStart(a);
            var t = new Thread(ts);

            t.Start();
        }

        public static void Run<T>(Action<T> a, T arg0) 
        {
            var ts = new ThreadStart(() => { a(arg0); });
            var t = new Thread(ts);

            t.Start();
        }

        public static void Run<T0, T1>(Action<T0, T1> a, T0 arg0, T1 arg1)
        {
            var ts = new ThreadStart(() => { a(arg0, arg1); });
            var t = new Thread(ts);

            t.Start();
        }
    }
}
