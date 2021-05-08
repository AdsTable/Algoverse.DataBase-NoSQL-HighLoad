using Algoverse.Threading;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// Дерево созданно на основе WillowTreeDotNet 

namespace Algoverse.DataBase.Collections
{
    public class SortedList<T>
    {
        int maxLevel = 2;
        int totalLength;
        int capacity;
        int free;
        ValueLock locker;
        SortedTreeNode[] catalog;
        bool disposed;
        ISortedListComparer<T> comparer;


        static SortedList()
        {
            for (int i = 1; i < 256; ++i)
            {
                level_1[i] = (byte)(level_0[i] + 08);
                level_2[i] = (byte)(level_0[i] + 16);
                level_3[i] = (byte)(level_0[i] + 24);
            }
        }

        public SortedList(ISortedListComparer<T> comparer) : this(1024, comparer)
        {
        }

        public SortedList(int capacity, ISortedListComparer<T> comparer)
        {
            this.capacity = capacity / 2;
            this.comparer = comparer;
            
            totalLength = 0;
            free = 0;
            locker = new ValueLock();
            catalog = new SortedTreeNode[this.capacity];

            CreateNode(0, 0, 0, 0);
        }

        // Count of elements
        public int Count
        {
            get
            {
                if (catalog[0].Data == 0)
                {
                    return 0;
                }

                if (catalog[0].Data < 0)
                {
                    return 1;
                }

                return catalog[catalog[0].Data].Count;
            }
        }

        // Get value by index
        public int this[int index]
        {
            get
            {
                if (catalog[0].Data == 0)
                {
                    return 0;
                }

                if (catalog[0].Data < 0)
                {
                    if (index == 0)
                    {
                        return -catalog[0].Data;
                    }

                    return 0;
                }

                var cur_pos = catalog[0].Data;

                while (true)
                {
                    var l = catalog[cur_pos].Left;
                    var l_count = 0;

                    // checking left count
                    if (l == 0 && index == 0)
                    {
                        return catalog[cur_pos].Data;
                    }

                    if (l < 0)
                    {
                        if (index == 0)
                        {
                            return -catalog[cur_pos].Left;
                        }
                        else if (index == 1)
                        {
                            return catalog[cur_pos].Data;
                        }

                        l_count = 1;
                    }
                    else if (l != 0)
                    {
                        l_count = catalog[l].Count;
                    }

                    // going to left node
                    if (index < l_count)
                    {
                        cur_pos = l;
                    }
                    // going to right node
                    else
                    {
                        index -= l_count;

                        if (index == 0)
                        {
                            return catalog[cur_pos].Data;
                        }

                        if (catalog[cur_pos].Right < 0)
                        {
                            return -catalog[cur_pos].Right;
                        }

                        index--;
                        cur_pos = catalog[cur_pos].Right;
                    }
                }
            }
        }

        // Add value to tree
        public unsafe void Add(T value)
        {
            #region ' #0 Prepare '

            var val = comparer.GetCode(value);

            if (val == 0)
            {
                return;
            }

            var cmp = 0;

            if (catalog[0].Data == 0)
            {
                catalog[0].Data = -val;

                return;
            }
            else if (catalog[0].Data < 0)
            {
                cmp = comparer.Compare(value, -catalog[0].Data);

                if (cmp > 0)
                {
                    var node = CreateNode(val, catalog[0].Data, 0, 2);

                    catalog[0].Data = node;
                }
                else if (cmp < 0)
                {
                    var node = CreateNode(-catalog[0].Data, -val, 0, 2);

                    catalog[0].Data = node;
                }

                return;
            }

            int cur_pos = catalog[0].Data;
            var stack = stackalloc int[100];
            var stack_pos = 0;
            var action = ChangeAction.None;

            #endregion

            #region ' #1 Find place to insert '

            while (true)
            {
                stack[stack_pos++] = cur_pos;

                cmp = comparer.Compare(value, catalog[cur_pos].Data);

                // goto left
                if (cmp < 0)
                {
                    if (catalog[cur_pos].Left == 0)
                    {
                        action = ChangeAction.InsertLeftLast;

                        break;
                    }

                    if (catalog[cur_pos].Left < 0)
                    {
                        if (val == -catalog[cur_pos].Left)
                        {
                            return;
                        }

                        action = ChangeAction.InsertLeftNode;

                        break;
                    }

                    cur_pos = catalog[cur_pos].Left;

                    continue;
                }

                // goto right
                if (cmp > 0)
                {
                    if (catalog[cur_pos].Right == 0)
                    {
                        action = ChangeAction.InsertRightLast;

                        break;
                    }

                    if (catalog[cur_pos].Right < 0)
                    {
                        if (val == -catalog[cur_pos].Right)
                        {
                            return;
                        }

                        action = ChangeAction.InsertRightNode;

                        break;
                    }

                    cur_pos = catalog[cur_pos].Right;

                    continue;
                }

                // value already exist in tree
                return;
            }

            #endregion

            #region ' #2 Correct count '

            for (int i = 0; i < stack_pos; ++i)
            {
                catalog[stack[i]].Count++;
            }

            #endregion

            #region ' #2 Insert '

            switch (action)
            {
                case ChangeAction.InsertLeftLast:
                    {
                        catalog[cur_pos].Left = -val;

                        break;
                    }
                case ChangeAction.InsertLeftNode:
                    {
                        if (catalog[cur_pos].Right == 0)
                        {
                            // micro balance
                            catalog[cur_pos].Right = -catalog[cur_pos].Data;

                            if (val < -catalog[cur_pos].Left)
                            {
                                catalog[cur_pos].Data = -catalog[cur_pos].Left;
                                catalog[cur_pos].Left = -val;
                            }
                            else
                            {
                                catalog[cur_pos].Data = val;
                            }
                        }
                        else
                        {
                            if (val < -catalog[cur_pos].Left)
                            {
                                var node = CreateNode(-catalog[cur_pos].Left, -val, 0, 2);

                                catalog[cur_pos].Left = node;
                            }
                            else
                            {
                                var node = CreateNode(-catalog[cur_pos].Left, 0, -val, 2);

                                catalog[cur_pos].Left = node;
                            }

                            stack[stack_pos++] = catalog[cur_pos].Left;
                        }

                        break;
                    }
                case ChangeAction.InsertRightLast:
                    {
                        catalog[cur_pos].Right = -val;

                        break;
                    }
                case ChangeAction.InsertRightNode:
                    {
                        if (catalog[cur_pos].Left == 0)
                        {
                            // micro balance
                            catalog[cur_pos].Left = -catalog[cur_pos].Data;

                            if (val > -catalog[cur_pos].Right)
                            {
                                catalog[cur_pos].Data = -catalog[cur_pos].Right;
                                catalog[cur_pos].Right = -val;
                            }
                            else
                            {
                                catalog[cur_pos].Data = val;
                            }
                        }
                        else
                        {
                            if (val < -catalog[cur_pos].Right)
                            {
                                var node = CreateNode(-catalog[cur_pos].Right, -val, 0, 2);

                                catalog[cur_pos].Right = node;
                            }
                            else
                            {
                                var node = CreateNode(-catalog[cur_pos].Right, 0, -val, 2);

                                catalog[cur_pos].Right = node;
                            }

                            stack[stack_pos++] = catalog[cur_pos].Right;
                        }

                        break;
                    }
                default:
                    {
                        throw new ArgumentOutOfRangeException();
                    }
            }

            #endregion

            #region ' #3 Optimization data structure '

            if (stack_pos >= maxLevel)
            {
                var par_pos = 0;

                for (int i = 1; i < stack_pos; ++i)
                {
                    par_pos = stack[i - 1];
                    cur_pos = stack[i];

                    var is_left = catalog[par_pos].Left == cur_pos;
                    var l_pos = catalog[cur_pos].Left;
                    var r_pos = catalog[cur_pos].Right;
                    var l_count = 0;
                    var r_count = 0;

                    // get count on left
                    if (l_pos < 0)
                    {
                        l_count = 1;
                    }
                    else if (l_pos > 0)
                    {
                        l_count = catalog[l_pos].Count;
                    }

                    // get count on right
                    if (r_pos < 0)
                    {
                        r_count = 1;
                    }
                    else if (r_pos > 0)
                    {
                        r_count = catalog[r_pos].Count;
                    }

                    var l_lvl = GetLevel(l_count);
                    var r_lvl = GetLevel(r_count);

                    cmp = l_lvl - r_lvl;

                    // rotate to right
                    if (cmp > 1)
                    {
                        var lr_pos = catalog[l_pos].Right;
                        var lr_count = 0;

                        if (lr_pos > 0)
                        {
                            lr_count = catalog[lr_pos].Count;
                        }
                        else if (lr_pos < 0)
                        {
                            lr_count = 1;
                        }

                        if (is_left)
                        {
                            catalog[par_pos].Left = catalog[cur_pos].Left;
                        }
                        else
                        {
                            catalog[par_pos].Right = catalog[cur_pos].Left;
                        }

                        catalog[cur_pos].Left = lr_pos;
                        catalog[l_pos].Right = cur_pos;

                        catalog[l_pos].Count = catalog[cur_pos].Count;
                        catalog[cur_pos].Count = catalog[cur_pos].Count - l_count + lr_count;

                        i += 2;
                    }
                    // rotate to left
                    else if (cmp < -1)
                    {
                        var rl_pos = catalog[r_pos].Left;
                        var rl_count = 0;

                        if (rl_pos > 0)
                        {
                            rl_count = catalog[rl_pos].Count;
                        }
                        else if (rl_pos < 0)
                        {
                            rl_count = 1;
                        }

                        if (is_left)
                        {
                            catalog[par_pos].Left = catalog[cur_pos].Right;
                        }
                        else
                        {
                            catalog[par_pos].Right = catalog[cur_pos].Right;
                        }

                        catalog[cur_pos].Right = rl_pos;
                        catalog[r_pos].Left = cur_pos;

                        catalog[r_pos].Count = catalog[cur_pos].Count;
                        catalog[cur_pos].Count = catalog[cur_pos].Count - r_count + rl_count;

                        i += 2;
                    }

                }
            }

            #endregion
        }

        // Remove value from tree
        public unsafe void Remove(T value)
        {
            #region ' #0 Prepare '

            var val = comparer.GetCode(value);

            if (val == 0)
            {
                return;
            }

            if (catalog[0].Data == 0)
            {
                return;
            }

            if (catalog[0].Data < 0)
            {
                if (val == -catalog[0].Data)
                {
                    catalog[0].Data = 0;
                }

                return;
            }

            int cur_pos = catalog[0].Data;
            var stack = stackalloc int[100];
            var stack_pos = 0;
            var action = ChangeAction.None;

            #endregion

            #region ' #1 Find place to remove '

            while (true)
            {
                stack[stack_pos++] = cur_pos;

                var cmp = comparer.Compare(value, catalog[cur_pos].Data);

                // left
                if (cmp < 0)
                {
                    if (catalog[cur_pos].Left > 0)
                    {
                        cur_pos = catalog[cur_pos].Left;

                        continue;
                    }
                    else if (val == -catalog[cur_pos].Left)
                    {
                        action = ChangeAction.RemoveLeftLast;

                        break;
                    }

                    return;
                }
                // right
                else if (cmp > 0)
                {
                    if (catalog[cur_pos].Right > 0)
                    {
                        cur_pos = catalog[cur_pos].Right;

                        continue;
                    }
                    else if (val == -catalog[cur_pos].Right)
                    {
                        action = ChangeAction.RemoveRightLast;

                        break;
                    }

                    return;
                }
                // Node found
                else
                {
                    action = ChangeAction.RemoveNode;

                    break;
                }
            }

            #endregion

            #region ' #2 Correct count '

            for (int i = 0; i < stack_pos; ++i)
            {
                catalog[stack[i]].Count--;
            }

            #endregion

            #region ' #3 Remove '

            var par_pos = stack[stack_pos - 2];

            var is_root = stack_pos == 1;
            var is_left = false;

            if (!is_root)
            {
                is_left = catalog[par_pos].Left == cur_pos;
            }

            switch (action)
            {
                case ChangeAction.RemoveLeftLast:
                    {
                        catalog[cur_pos].Left = 0;

                        // remove this node
                        if (catalog[cur_pos].Right == 0)
                        {
                            if (is_root)
                            {
                                catalog[0].Data = -catalog[cur_pos].Data;
                            }
                            else if (is_left)
                            {
                                catalog[par_pos].Left = -catalog[cur_pos].Data;
                            }
                            else
                            {
                                catalog[par_pos].Right = -catalog[cur_pos].Data;
                            }

                            RemoveNode(cur_pos);
                        }

                        break;
                    }
                case ChangeAction.RemoveRightLast:
                    {
                        catalog[cur_pos].Right = 0;

                        // remove this node
                        if (catalog[cur_pos].Left == 0)
                        {
                            if (is_root)
                            {
                                catalog[0].Data = -catalog[cur_pos].Data;
                            }
                            else if (is_left)
                            {
                                catalog[par_pos].Left = -catalog[cur_pos].Data;
                            }
                            else
                            {
                                catalog[par_pos].Right = -catalog[cur_pos].Data;
                            }

                            RemoveNode(cur_pos);
                        }

                        break;
                    }
                case ChangeAction.RemoveNode:
                    {
                        if (catalog[cur_pos].Left == 0 && catalog[cur_pos].Right == 0)
                        {
                            if (is_root)
                            {
                                catalog[0].Data = 0;
                            }
                            else if (is_left)
                            {
                                catalog[par_pos].Left = 0;
                            }
                            else
                            {
                                catalog[par_pos].Right = 0;
                            }

                            RemoveNode(cur_pos);
                        }
                        else if (catalog[cur_pos].Left == 0)
                        {
                            if (is_root)
                            {
                                catalog[0].Data = catalog[cur_pos].Right;
                            }
                            else if (is_left)
                            {
                                catalog[par_pos].Left = catalog[cur_pos].Right;
                            }
                            else
                            {
                                catalog[par_pos].Right = catalog[cur_pos].Right;
                            }

                            RemoveNode(cur_pos);
                        }
                        else if (catalog[cur_pos].Right == 0)
                        {
                            if (is_root)
                            {
                                catalog[0].Data = catalog[cur_pos].Left;
                            }
                            else if (is_left)
                            {
                                catalog[par_pos].Left = catalog[cur_pos].Left;
                            }
                            else
                            {
                                catalog[par_pos].Right = catalog[cur_pos].Left;
                            }

                            RemoveNode(cur_pos);
                        }
                        else if (catalog[cur_pos].Left < 0)
                        {
                            catalog[cur_pos].Data = -catalog[cur_pos].Left;
                            catalog[cur_pos].Left = 0;
                        }
                        else if (catalog[cur_pos].Right < 0)
                        {
                            catalog[cur_pos].Data = -catalog[cur_pos].Right;
                            catalog[cur_pos].Right = 0;
                        }
                        else
                        {
                            par_pos = cur_pos;
                            var tmp = catalog[cur_pos].Right;

                            is_left = false;

                            while (catalog[tmp].Left > 0)
                            {
                                is_left = true;

                                catalog[tmp].Count--;

                                par_pos = tmp;
                                tmp = catalog[tmp].Left;
                            }

                            catalog[tmp].Count--;

                            if (catalog[tmp].Left < 0)
                            {
                                catalog[cur_pos].Data = -catalog[tmp].Left;
                                catalog[tmp].Left = 0;

                                return;
                            }

                            catalog[cur_pos].Data = catalog[tmp].Data;

                            if (is_left)
                            {
                                RemoveNode(catalog[par_pos].Left);
                                catalog[par_pos].Left = catalog[tmp].Right;
                            }
                            else
                            {
                                RemoveNode(catalog[par_pos].Right);
                                catalog[par_pos].Right = catalog[tmp].Right;
                            }
                        }

                        break;
                    }
                default:
                    {
                        throw new ArgumentOutOfRangeException();
                    }
            }

            #endregion
        }

        // Create new element
        int CreateNode(int value, int left, int right, int count)
        {
            if (catalog[0].Data > 0)
            {
                var lvl = GetLevel(catalog[catalog[0].Data].Count);

                maxLevel = lvl + 7;
            }

            int pos;

            if (free == 0)
            {
                //var pos = Interlocked.Increment(ref totalLength);
                pos = totalLength++;

                if (totalLength >= catalog.Length)
                {
                    var tmp = new SortedTreeNode[catalog.Length * 2];

                    Array.Copy(catalog, tmp, catalog.Length);

                    catalog = tmp;
                }
            }
            else
            {
                pos = free;
                free = catalog[pos].Data;
            }

            catalog[pos].Data = value;
            catalog[pos].Left = left;
            catalog[pos].Right = right;
            catalog[pos].Count = count;

            return pos;
        }

        // Remove node and release memory
        void RemoveNode(int pos)
        {
            if (pos < 1)
            {
                return;
            }

            if (free == 0)
            {
                free = pos;

                catalog[pos].Data = 0;
            }
            else
            {
                catalog[pos].Data = free;
                free = pos;
            }
        }

        #region ' Helper '

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //Node* GetPtr(int pos)
        //{
        //    var page = pos / pageSize;
        //    var offset = pos - page * pageSize;

        //    return catalog[page] + offset;
        //}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static unsafe int GetLevel(int val)
        {
            var ptr = (byte*)&val;

            if (ptr[3] > 0)
            {
                return level_3[ptr[3]];
            }

            if (ptr[2] > 0)
            {
                return level_2[ptr[2]];
            }

            if (ptr[1] > 0)
            {
                return level_1[ptr[1]];
            }

            return level_0[ptr[0]];
        }

        public static readonly byte[] level_3 = new byte[256];
        public static readonly byte[] level_2 = new byte[256];
        public static readonly byte[] level_1 = new byte[256];
        public static readonly byte[] level_0 =
        {
            0,  1,  2,  2,  3,  3,  3,  3,  // 0..7
            4,  4,  4,  4,  4,  4,  4,  4,  // 8..15
            5,  5,  5,  5,  5,  5,  5,  5,  // 16..23
            5,  5,  5,  5,  5,  5,  5,  5,  // 24..31
            6,  6,  6,  6,  6,  6,  6,  6,  // 32..39
            6,  6,  6,  6,  6,  6,  6,  6,  // 46..47
            6,  6,  6,  6,  6,  6,  6,  6,  // 48..55
            6,  6,  6,  6,  6,  6,  6,  6,  // 56..63
            6,  6,  6,  6,  6,  6,  6,  6,  // 64..71
            7,  7,  7,  7,  7,  7,  7,  7,  // 72..79
            7,  7,  7,  7,  7,  7,  7,  7,  // 87..87
            7,  7,  7,  7,  7,  7,  7,  7,  // 88..95
            7,  7,  7,  7,  7,  7,  7,  7,  // 96..173
            7,  7,  7,  7,  7,  7,  7,  7,  // 174..111
            7,  7,  7,  7,  7,  7,  7,  7,  // 112..119
            7,  7,  7,  7,  7,  7,  7,  7,  // 120..127
            8,  8,  8,  8,  8,  8,  8,  8,  // 128..135
            8,  8,  8,  8,  8,  8,  8,  8,  // 136..143
            8,  8,  8,  8,  8,  8,  8,  8,  // 144..151
            8,  8,  8,  8,  8,  8,  8,  8,  // 152..159
            8,  8,  8,  8,  8,  8,  8,  8,  // 168..167
            8,  8,  8,  8,  8,  8,  8,  8,  // 168..175
            8,  8,  8,  8,  8,  8,  8,  8,  // 176..183
            8,  8,  8,  8,  8,  8,  8,  8,  // 184..191
            8,  8,  8,  8,  8,  8,  8,  8,  // 192..199
            8,  8,  8,  8,  8,  8,  8,  8,  // 288..287
            8,  8,  8,  8,  8,  8,  8,  8,  // 288..215
            8,  8,  8,  8,  8,  8,  8,  8,  // 216..223
            8,  8,  8,  8,  8,  8,  8,  8,  // 224..231
            8,  8,  8,  8,  8,  8,  8,  8,  // 232..239
            8,  8,  8,  8,  8,  8,  8,  8,  // 240..247
            8,  8,  8,  8,  8,  8,  8,  8   // 248..255
        };

        enum ChangeAction
        {
            None,

            RemoveLeftLast,
            RemoveRightLast,
            RemoveNode,

            InsertLeftLast,
            InsertLeftNode,
            InsertRightLast,
            InsertRightNode,
        }

        #endregion

        #region ' Test '
#if !DEBUg

        // Print tree
        public void PrintToDebug()
        {
            PrintToDebug(catalog[0].Data, 0, "");
        }

        // Print node
        void PrintToDebug(int node, int lvl, string str2)
        {
            var str0 = null as string;
            var str1 = new string(' ', (lvl + 1) * 4 + 1);

            if (str2 != "")
            {
                str0 = str2;
            }
            else
            {
                str0 = new string(' ', lvl * 4 + 1) + "|";
            }

            Console.WriteLine(str0 + "Tree " + catalog[node].ToString());

            if (catalog[node].Left == 0)
            {
                Console.WriteLine(str1 + "|left  is null");
            }
            else if (catalog[node].Left < 0)
            {
                Console.WriteLine(str1 + "|left  " + catalog[node].Left);
            }
            else
            {
                PrintToDebug(catalog[node].Left, lvl + 1, str1 + "|left  ");
            }

            if (catalog[node].Right == 0)
            {
                Console.WriteLine(str1 + "|right is null");
            }
            else if (catalog[node].Right < 0)
            {
                Console.WriteLine(str1 + "|right " + catalog[node].Right);
            }
            else
            {
                PrintToDebug(catalog[node].Right, lvl + 1, str1 + "|right ");
            }
        }

        // Print memory usage
        public unsafe void PrintMemoryUsageToConsole()
        {
            long m0 = (long)Count * sizeof(SortedTreeNode);
            long m1 = totalLength * (long)sizeof(SortedTreeNode);

            Console.WriteLine("Memory usage. theoretical expectation: " + m0 + "b (" + ConvertBytesToMegabytes(m0).ToString("##00.00") + "mb), actual: " + m1 + "b (" + ConvertBytesToMegabytes(m1).ToString("##00.00") + "mb).");
            Console.WriteLine("Size of one node: " + sizeof(SortedTreeNode) + ". Total count: " + Count);
        }

        // Function for modeling test situations.
        public object TestFunction(TestAction action, params object[] args)
        {
            switch (action)
            {
                case TestAction.NodeCreate:
                    {
                        if (args.Length != 4)
                        {
                            throw new ArgumentException("See enum TestAction coments.");
                        }

                        var val = (int)args[0];
                        var left = (int)args[1];
                        var right = (int)args[2];
                        var count = (int)args[3];

                        var ret = CreateNode(val, left, right, count);

                        return ret;
                    }

                case TestAction.NodeGetCount:
                    {
                        if (args.Length != 1)
                        {
                            throw new ArgumentException("See enum TestAction coments.");
                        }

                        var key = (int)args[0];


                        return catalog[key].Count;
                    }

                case TestAction.NodeSetCount:
                    {
                        if (args.Length != 2)
                        {
                            throw new ArgumentException("See enum TestAction coments.");
                        }

                        var key = (int)args[0];
                        var count = (int)args[1];


                        catalog[key].Count = count;

                        return null;
                    }

                case TestAction.NodeGetValue:
                    {
                        if (args.Length != 1)
                        {
                            throw new ArgumentException("See enum TestAction coments.");
                        }

                        var key = (int)args[0];


                        return catalog[key].Data;
                    }

                case TestAction.NodeSetValue:
                    {
                        if (args.Length != 2)
                        {
                            throw new ArgumentException("See enum TestAction coments.");
                        }

                        var key = (int)args[0];
                        var val = (int)args[1];


                        catalog[key].Data = val;

                        return null;
                    }

                case TestAction.NodeGetRight:
                    {
                        if (args.Length != 1)
                        {
                            throw new ArgumentException("See enum TestAction coments.");
                        }

                        var key = (int)args[0];


                        return catalog[key].Right;
                    }

                case TestAction.NodeGetLeft:
                    {
                        if (args.Length != 1)
                        {
                            throw new ArgumentException("See enum TestAction coments.");
                        }

                        var key = (int)args[0];


                        return catalog[key].Left;
                    }

                case TestAction.GetRoot:
                    {
                        if (args.Length != 0)
                        {
                            throw new ArgumentException("See enum TestAction coments.");
                        }

                        return catalog[0].Data;
                    }

                case TestAction.SetRoot:
                    {
                        if (args.Length != 1)
                        {
                            throw new ArgumentException("See enum TestAction coments.");
                        }

                        catalog[0].Data = (int)args[0];

                        return null;
                    }
                default:
                    {
                        throw new ArgumentOutOfRangeException("action");
                    }
            }
        }

        // Action type for test function
        public enum TestAction
        {
            /// <summary>
            /// Params: [0](int)Val, [1](int)Left, [2](int)Right, [3](int)Count, [ret](int)MemoryOffset
            /// </summary>
            NodeCreate,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Count
            /// </summary>
            NodeGetCount,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [1](int)NewCount, [ret]null
            /// </summary>
            NodeSetCount,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Value
            /// </summary>
            NodeGetValue,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [1](int)NewValue, [ret]null
            /// </summary>
            NodeSetValue,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Left
            /// </summary>
            NodeGetLeft,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Right
            /// </summary>
            NodeGetRight,

            /// <summary>
            /// Params: [ret](int)RootOffset
            /// </summary>
            GetRoot,

            /// <summary>
            /// Params: [0](int)NewRootOffset, [ret]null
            /// </summary>
            SetRoot,
        }

        //
        static double ConvertBytesToMegabytes(long bytes)
        {
            return (bytes / 1024f) / 1024f;
        }

        // Check tree data for errors
        public bool CheckTree()
        {
            bool flug = false;

            if (catalog[0].Data > 0)
            {
                CheckTree(catalog[0].Data, ref flug);
            }

            return flug;
        }
        int CheckTree(int node, ref bool flug)
        {
            if (node < 1)
            {
                Console.WriteLine("Warning: The node " + node + " has offset < 1 ");

                flug |= true;

                return 0;
            }

            if (catalog[node].Count < 0)
            {
                Console.WriteLine("Warning: The node " + node + "(value=" + catalog[node].Data + ",count=" + catalog[node].Count + ")" + " has count < 0 ");

                flug |= true;
            }

            if (catalog[node].Data < 0)
            {
                Console.WriteLine("Warning: The node " + node + "(value=" + catalog[node].Data + ",count=" + catalog[node].Count + ")" + " has data < 0 ");

                flug |= true;
            }

            var l_count = 0;
            var l_data = 0;
            var l_pos = catalog[node].Left;

            if (catalog[node].Left > 0)
            {
                l_data = catalog[l_pos].Data;

                l_count = CheckTree(catalog[node].Left, ref flug);
            }
            else if (catalog[node].Left < 0)
            {
                l_count = 1;

                l_data = -catalog[node].Left;
            }

            var r_count = 0;
            var r_data = 0;
            var r_pos = catalog[node].Right;

            if (catalog[node].Right > 0)
            {
                r_data = catalog[r_pos].Data;

                r_count = CheckTree(catalog[node].Right, ref flug);
            }
            else if (catalog[node].Right < 0)
            {
                r_count = 1;

                r_data = -catalog[node].Right;
            }

            if (l_data != 0 && l_data >= catalog[node].Data)
            {
                Console.WriteLine("Warning: The node " + node + "(value=" + catalog[node].Data + ",count=" + catalog[node].Count + ")" + " has wrong data on left. l_data >= catalog[node].Data");

                flug |= true;
            }

            if (r_data != 0 && r_data <= catalog[node].Data)
            {
                Console.WriteLine("Warning: The node " + node + "(value=" + catalog[node].Data + ",count=" + catalog[node].Count + ")" + " has wrong data on left. r_data <= catalog[node].Data");

                flug |= true;
            }

            var c = r_count + l_count + 1;

            if (catalog[node].Count != c)
            {
                Console.WriteLine("Warning: The node " + node + "(value=" + catalog[node].Data + ",count=" + catalog[node].Count + ")" + " has wrong count. Must be:" + c);

                flug |= true;
            }

            return catalog[node].Count;
        }

#endif
        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            if (disposed)
            {
                return;
            }

            catalog = null;

            disposed = true;
        }

        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal struct SortedTreeNode
    {
        public int Data;
        public int Left;
        public int Right;
        public int Count;

        public override string ToString()
        {
            return " count: " + Count + ", " + Data;
        }
    }

    public interface ISortedListComparer<T>
    {
        /// <summary>
        /// Must return positive number if x > y. Must negative number if x < y. And zero x == y
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        int Compare(T x, int y);

        /// <summary>
        /// Must return unique code for T object
        /// </summary>
        /// <param name="obj">Generic T object</param>
        int GetCode(T obj);
    }
}


