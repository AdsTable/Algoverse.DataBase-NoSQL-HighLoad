using System.Collections.Generic;

namespace Algoverse.DataBase
{
    public abstract class TaskAction
    {
        static Dictionary<string, TaskAction> ht;

        static TaskAction()
        {
            ht = new Dictionary<string, TaskAction>();
        }

        public static void Run(string key, params object[] args)
        {
            if (ht.ContainsKey(key))
            {
                ht[key].Run(args);
            }
        }

        protected TaskAction()
        {
            if (!ht.ContainsKey(Key))
            {
                ht.Add(Key, this);
            }
        }

        public abstract string Key { get; }

        public abstract void Run(object[] args);

    }
}
