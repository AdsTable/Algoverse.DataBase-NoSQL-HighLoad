using System;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using Algoverse.Threading;

namespace Algoverse.DataBase
{
    internal unsafe class Index : IDisposable
    {
        #region ' Core '

        const int headerSize = 100;
        const int maxLevel = 30;

        bool disposed;
        long length;
        long capacity;
        FileStream fs;
        MemoryMappedFile file;
        MemoryMappedViewAccessor mapping;

        // TODO блокировки происходят по типам дерева. Возможно стоит подумать о блокировках на уровне каждого индекса, т.к. они разделют разную память и объекты.
        ValueLockRW lockInd = new ValueLockRW();
        //ValueLockRW lockerGroup = new ValueLockRW();
        string key;
        byte* header;
        byte* mem;

        static Index()
        {
            for (int i = 1; i < 256; ++i)
            {
                level_1[i] = (byte)(level_0[i] + 08);
                level_2[i] = (byte)(level_0[i] + 16);
                level_3[i] = (byte)(level_0[i] + 24);
            }
        }
        
        // Constructor
        public Index(string fullPath)
        {
            capacity = 1024 * 1024 * 20;

            key = fullPath.CalculateHashString();

            // Create file
            if (!File.Exists(fullPath))
            {
                fs = new FileStream(fullPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                fs.SetLength(headerSize + capacity);

                InitFile();

                var count = (int*)header;

                *count = 10;
            }
            // Open file
            else
            {
                fs = new FileStream(fullPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

                InitFile();
            }

            // Header format
            // 00 - Lenght (Max used offset in the file)
            // 04 - Count of registred indexes
            // 08 - Root catalog 
            // 12 - Linked list (04 bytes catalog)
            // 16 - Linked list (05 bytes catalog)
            // 20 - Linked list (06 bytes catalog)
            // 24 - Linked list (07 bytes catalog)
            // 28 - Linked list (08 bytes catalog)
            // 32 - Linked list (09 bytes catalog)
            // 36 - Linked list (10 bytes catalog)
            // 40 - Linked list (11 bytes catalog)
            // 44 - Linked list (12 bytes catalog)
            // 48 - Linked list (13 bytes catalog)
            // 52 - Linked list (14 bytes catalog)
            // 56 - Linked list (15 bytes catalog)
            // 60 - Linked list (16 bytes catalog)
            // 64 - Linked list (17 bytes catalog)
            // 68 - Linked list (18 bytes catalog)
            // 72 - Linked list (19 bytes catalog)
            // 76 - Linked list (20 bytes catalog)
            // 80 - Linked list (other bytes catalog)
            // 00
            // 00
        }

        // Register index
        [HandleProcessCorruptedStateExceptions]
        public void RegisterIndex<T>(IDataIndexBase<T> index) where T : Record
        {
            try
            {
                //lockInd?.ReadLock();
                lockInd.WriteLock();

                var count = (int*)(header + 4);
                var root = (int*)(header + 8);

                IndexLink* ptr;

                if (*count == 0)
                {
                    *root = CreateIndexLink(index);

                    ptr = (IndexLink*)(mem + *root);

                    index.MemoryKey = ptr->Root;

                    *count += 1;

                    return;
                }

                var cur = (IndexLink*)(mem + *root);

                for (int i = 0; i < *count; ++i)
                {
                    if (cur->Length == index.Name.Length)
                    {
                        var str = (char*)((byte*)cur + sizeof (IndexLink));

                        for (int j = 0; j < cur->Length; ++j)
                        {
                            if (str[j] != index.Name[j])
                            {
                                goto exit;
                            }
                        }

                        index.MemoryKey = cur->Root;

                        return;
                    }

                    exit:

                    if (cur->Next == 0)
                    {
                        break;
                    }

                    cur = (IndexLink*)(mem + cur->Next);
                }

                cur->Next = CreateIndexLink(index);

                ptr = (IndexLink*)(mem + cur->Next);

                index.MemoryKey = ptr->Root;

                *count += 1;
            }
            finally
            {
                lockInd.Unlock();
            }
        }

        #endregion

        #region ' Tree '

        // Count
        public int Count(int memoryKey, ValueLockRW locker)
        {
            try
            {
                if (locker != null)
                {
                    locker.ReadLock();
                }

                var ptr = (int*)(mem + memoryKey);

                if (*ptr == 0)
                {
                    return 0;
                }
                else if (*ptr < 0)
                {
                    return 1;
                }

                return ((CountNode*)(mem + *ptr))->Count;
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Return element by index
        public int GetByIndex(int index, int memoryKey, ValueLockRW locker)
        {
            try
            {
                if (locker != null)
                {
                    locker.ReadLock();
                }

                var ptr = (int*)(mem + memoryKey);

                if (*ptr == 0)
                {
                    return 0;
                }
                else if (*ptr < 0)
                {
                    return -*ptr;
                }

                var flug = (Flugs*)(mem + *ptr);

                if (index >= ((CountNode*)flug)->Count)
                {
                    throw new IndexOutOfRangeException();
                }

                var ind = index;

                while (true)
                {
                    switch (*flug)
                    {
                            #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (TreeNode*)flug;
                            var l_count = 0;

                            // checking left count
                            if (cur->Left == 0)
                            {
                                if (ind == 0)
                                {
                                    return cur->Data;
                                }
                            }
                            else if (cur->Left < 0)
                            {
                                if (ind == 0)
                                {
                                    return -cur->Left;
                                }
                                else if (ind == 1)
                                {
                                    return cur->Data;
                                }

                                l_count = 1;
                            }
                            else
                            {
                                l_count = ((CountNode*)(mem + cur->Left))->Count;
                            }

                            // going to left node
                            if (ind < l_count)
                            {
                                // exception
                                if (cur->Left < 1)
                                {
                                    ErrorReport(5);

                                    return 0;
                                }

                                flug = (Flugs*)(mem + cur->Left);
                            }
                                // going to right node
                            else
                            {
                                ind -= l_count;

                                if (ind == 0)
                                {
                                    return cur->Data;
                                }

                                if (cur->Right < 0)
                                {
                                    return -cur->Right;
                                }

                                ind--;
                                flug = (Flugs*)(mem + cur->Right);
                            }
                        }

                            break;

                            #endregion

                            #region ' Data '

                        case Flugs.Data:
                        {
                            var cur = (DataNode*)flug;

                            if (ind < cur->Count)
                            {
                                var dat = (int*)(mem + cur->Data);

                                return dat[ind];
                            }
                            else
                            {
                                ErrorReport(9);

                                return 0;
                            }

                            break;
                        }

                            #endregion

                            #region ' Other '

                        default:
                        {
                            ErrorReport(111);

                            return 0;
                        }

                            #endregion
                    }
                }
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Inserting new element to index
        public void Insert<T>(T obj, int memoryKey, IRecordsComparer<T> comparer, bool isUnique, ValueLockRW locker) where T : Record
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }
                
                MakeSpace();

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    *ptr = -obj.Code;

                    return;
                }
                else if (*ptr < 0)
                {
                    cmp = comparer.CompareRecords(obj, -*ptr);

                    if (cmp < 0)
                    {
                        *ptr = CreateTreeNode(-*ptr, -obj.Code, 0, 2);
                    }
                    else if (cmp > 0)
                    {
                        *ptr = CreateTreeNode(obj.Code, *ptr, 0, 2);
                    }

                    return;
                }

                var action = ChangeAction.None;
                var stack = stackalloc int[100];
                var lvl = 0;
                var ext_data = 0;
                Flugs* flug;

                pos = *ptr;

                #endregion

                #region ' Find place to insert '

                while (true)
                {
                    stack[lvl++] = pos;
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                            #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (TreeNode*)flug;
                            cmp = comparer.CompareRecords(obj, cur->Data);

                            // already exist
                            if (cmp == 0 && isUnique)
                            {
                                return;
                            }
                                // left
                            else if (cmp <= 0)
                            {
                                if (cur->Left == 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertTreeNodeLeftLast;

                                    goto insert;
                                }
                                else if (cur->Left < 0)
                                {
                                    cmp = comparer.CompareRecords(obj, -cur->Left);

                                    if (cmp == 0)
                                    {
                                        return;
                                    }

                                    // Lock
                                    action = ChangeAction.InsertTreeNodeLeftTree;

                                    goto insert;
                                }
                                else
                                {
                                    //flug = (Flugs*) (mem + cur->left);
                                    pos = cur->Left;

                                    continue;
                                }
                            }
                                // right
                            else
                            {
                                if (cur->Right == 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertTreeNodeRightLast;

                                    goto insert;
                                }
                                else if (cur->Right < 0)
                                {
                                    cmp = comparer.CompareRecords(obj, -cur->Right);

                                    if (cmp == 0)
                                    {
                                        return;
                                    }

                                    // Lock
                                    action = ChangeAction.InsertTreeNodeRightTree;

                                    goto insert;
                                }
                                else
                                {
                                    //flug = (Flugs*) (mem + cur->right);
                                    pos = cur->Right;

                                    continue;
                                }
                            }
                        }

                            #endregion

                            #region ' Data '

                        case Flugs.Data:
                        {
                            var cur = (DataNode*)flug;
                            var dat = (int*)(mem + cur->Data);

                            int first = 0;
                            int last = cur->Count - 1;

                            // position is before 0
                            if (comparer.CompareRecords(obj, dat[first]) < 0)
                            {
                                // Lock
                                action = ChangeAction.InsertDataNodeFirst;

                                goto insert;
                            }
                                // position is after last
                            else if (comparer.CompareRecords(obj, dat[last]) > 0)
                            {
                                // Lock
                                action = ChangeAction.InsertDataNodeLast;

                                goto insert;
                            }
                                // position middle
                            else
                            {
                                // search for position
                                while (first < last)
                                {
                                    int mid = (first + last) >> 1;

                                    if (comparer.CompareRecords(obj, dat[mid]) <= 0)
                                    {
                                        last = mid;
                                    }
                                    else
                                    {
                                        first = mid + 1;
                                    }
                                }

                                // already exist
                                if (comparer.CompareRecords(obj, dat[last]) == 0 && isUnique)
                                {
                                    return;
                                }

                                // Insertion index found. last - desired position
                                ext_data = last;

                                // Lock
                                action = ChangeAction.InsertDataNodeMiddle;

                                goto insert;
                            }
                        }

                            #endregion

                            #region ' Other '

                        default:
                        {
                            return;
                            Debugger.Break();

                            
                            break;
                        }

                            #endregion
                    }
                }

                insert:
                ;

                #endregion

                #region ' Correct count '

                for (int i = 0; i < lvl; ++i)
                {
                    flug = (Flugs*)(mem + stack[i]);

                    switch (*flug)
                    {
                            #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (TreeNode*)flug;

                            cur->Count++;

                            break;
                        }

                            #endregion

                            #region ' Data '

                        case Flugs.Data:
                        {
                            continue;
                        }

                            #endregion

                            #region ' Other '

                        default:
                        {
                            return;
                        }

                            #endregion
                    }
                }

                #endregion

                #region ' Insert '

                switch (action)
                {
                    case ChangeAction.InsertTreeNodeLeftLast:
                    {
                        var cur = (TreeNode*)flug;

                        cur->Left = -obj.Code;

                        return;
                    }
                    case ChangeAction.InsertTreeNodeLeftTree:
                    {
                        var cur = (TreeNode*)flug;

                        // micro rotate
                        if (cur->Right == 0)
                        {
                            cur->Right = -cur->Data;

                            if (cmp < 0)
                            {
                                cur->Data = -cur->Left;
                                cur->Left = -obj.Code;
                            }
                            else if (cmp > 0)
                            {
                                cur->Data = obj.Code;
                            }
                        }
                        else
                        {
                            if (cmp < 0)
                            {
                                cur->Left = CreateTreeNode(-cur->Left, -obj.Code, 0, 2);
                            }
                            else
                            {
                                cur->Left = CreateTreeNode(-cur->Left, 0, -obj.Code, 2);
                            }
                        }

                        goto exit;
                    }
                    case ChangeAction.InsertTreeNodeRightLast:
                    {
                        var cur = (TreeNode*)flug;
                        cur->Right = -obj.Code;

                        return;
                    }
                    case ChangeAction.InsertTreeNodeRightTree:
                    {
                        var cur = (TreeNode*)flug;
                        //var cmp = comparer.CompareRecords(obj, -cur->Right);

                        // micro rotate
                        if (cur->Left == 0)
                        {
                            cur->Left = -cur->Data;

                            if (cmp < 0)
                            {
                                cur->Data = obj.Code;
                            }
                            else if (cmp > 0)
                            {
                                cur->Data = -cur->Right;
                                cur->Right = -obj.Code;
                            }
                        }
                        else
                        {
                            if (cmp < 0)
                            {
                                cur->Right = CreateTreeNode(-cur->Right, -obj.Code, 0, 2);
                            }
                            else
                            {
                                cur->Right = CreateTreeNode(-cur->Right, 0, -obj.Code, 2);
                            }
                        }

                        goto exit;
                    }
                    case ChangeAction.InsertDataNodeFirst:
                    {
                        var cur = (DataNode*)flug;
                        var par = (LinkNode*)(mem + stack[lvl - 2]);
                        int* par_ptr;

                        // get parent link
                        if (par->Left == pos)
                        {
                            par_ptr = &par->Left;
                        }
                        else if (par->Right == pos)
                        {
                            par_ptr = &par->Right;
                        }
                        else
                        {
                            ErrorReport(90);

                            return;
                        }

                        var adr = CreateTreeNode(obj.Code, 0, *par_ptr, cur->Count + 1);

                        *par_ptr = adr;

                        goto exit;
                    }
                    case ChangeAction.InsertDataNodeLast:
                    {
                        var cur = (DataNode*)flug;
                        var par = (LinkNode*)(mem + stack[lvl - 2]);
                        int* par_ptr;

                        // get parent link
                        if (par->Left == pos)
                        {
                            par_ptr = &par->Left;
                        }
                        else if (par->Right == pos)
                        {
                            par_ptr = &par->Right;
                        }
                        else
                        {
                            ErrorReport(90);

                            return;
                        }

                        var adr = CreateTreeNode(obj.Code, *par_ptr, 0, cur->Count + 1);

                        *par_ptr = adr;

                        goto exit;
                    }
                    case ChangeAction.InsertDataNodeMiddle:
                    {
                        var cur = (DataNode*)flug;
                        var dat = (int*)(mem + cur->Data);
                        var par = (LinkNode*)(mem + stack[lvl - 2]);
                        int* par_ptr;

                        // get parent link
                        if (par->Left == pos)
                        {
                            par_ptr = &par->Left;
                        }
                        else if (par->Right == pos)
                        {
                            par_ptr = &par->Right;
                        }
                        else
                        {
                            ErrorReport(90);

                            return;
                        }

                        var last = ext_data;

                        // create right data node
                        var right_pos = CreateDataNode();
                        var right_node = (DataNode*)(mem + right_pos);

                        right_node->Count = cur->Count - last;
                        right_node->Min = dat[last];
                        right_node->Max = cur->Max;
                        right_node->Data = cur->Data + last * sizeof (int);

                        var c = cur->Count;

                        // correct cur data node
                        cur->Count = last;
                        cur->Max = dat[last - 1];

                        // create tree node
                        *par_ptr = CreateTreeNode(obj.Code, pos, right_pos, c + 1);


                        //var adr_left = CreateTreeNode(*par_ptr, -obj.Code, 0);
                        //var adr_right = CreateDataNode();
                        //var adr_root = CreateBlockNode(adr_left, adr_right);

                        //*par_ptr = adr_root;

                        //var block_root = (BlockNode*)(mem + adr_root);

                        //block_root->Count = cur->Count + 1;
                        //block_root->MaxOnLeft = obj.Code;

                        //var block_left = (BlockNode*)(mem + adr_left);

                        //block_left->Count = last + 1;
                        //block_left->MaxOnLeft = dat[last - 1];

                        //var data_new = (DataNode*)(mem + adr_right);

                        //data_new->Count = cur->Count - last;
                        //data_new->Min = dat[last];
                        //data_new->Max = cur->Max;
                        //data_new->Data = cur->Data + last * sizeof(int);

                        //cur->Count = last;
                        //cur->Max = dat[last - 1];

                        goto exit;
                    }
                    default:
                    {
                        return;
                    }
                }

                #endregion

                #region ' Optimization data structure '

                exit:

                if (lvl >= maxLevel)
                {
                    //if (lvl >= maxLevel * 2)
                    //{
                    //    SmartOptimization(stack[9], stack[10]);
                    //}
                    //else
                    //{
                    var c = 0;

                    //for (int i = lvl - 1; i > 0; i--)

                    for (int i = 1; i < lvl; ++i)
                    {
                        var par = (TreeNode*)(mem + stack[i - 1]);
                        var itm = (Flugs*)(mem + stack[i]);

                        int* par_ptr;

                        if (par->Left == stack[i])
                        {
                            par_ptr = &par->Left;
                        }
                        else if (par->Right == stack[i])
                        {
                            par_ptr = &par->Right;
                        }
                        else
                        {
                            ErrorReport(65);

                            return;
                        }

                        if (*itm == Flugs.Tree)
                        {
                            var cur = (TreeNode*)itm;
                            var l_count = 0;
                            var r_count = 0;

                            if (cur->Left < 0)
                            {
                                l_count = 1;
                            }
                            else if (cur->Left > 0)
                            {
                                var f = (Flugs*)(mem + cur->Left);

                                if (*f == Flugs.Tree)
                                {
                                    l_count = ((CountNode*)f)->Count;
                                }
                                else
                                {
                                    l_count = 1;
                                }
                            }

                            if (cur->Right < 0)
                            {
                                r_count = 1;
                            }
                            else if (cur->Right > 0)
                            {
                                var f = (Flugs*)(mem + cur->Right);

                                if (*f == Flugs.Tree)
                                {
                                    r_count = ((CountNode*)f)->Count;
                                }
                                else
                                {
                                    r_count = 1;
                                }
                            }

                            var l_lvl = GetLevel(l_count);
                            var r_lvl = GetLevel(r_count);

                            cmp = l_lvl - r_lvl;

                            // To right
                            if (cmp > 1)
                            {
                                var l_flug = (Flugs*)(mem + cur->Left);

                                if (*l_flug != Flugs.Tree)
                                {
                                    continue;
                                }

                                var l_node = (TreeNode*)l_flug;
                                var lr_count = 0;

                                if (l_node->Right > 0)
                                {
                                    lr_count = ((CountNode*)(mem + l_node->Right))->Count;
                                }
                                else if (l_node->Right < 0)
                                {
                                    lr_count = 1;
                                }

                                *par_ptr = cur->Left;

                                cur->Left = l_node->Right;
                                l_node->Right = stack[i];

                                l_node->Count = cur->Count;
                                cur->Count = cur->Count - l_count + lr_count;

                                i += 2;
                            }
                                // to left
                            else if (cmp < -1)
                            {
                                var r_flug = (Flugs*)(mem + cur->Right);

                                if (*r_flug != Flugs.Tree)
                                {
                                    continue;
                                }

                                var r_node = (TreeNode*)r_flug;
                                var rl_count = 0;

                                if (r_node->Left > 0)
                                {
                                    rl_count = ((CountNode*)(mem + r_node->Left))->Count;
                                }
                                else if (r_node->Left < 0)
                                {
                                    rl_count = 1;
                                }

                                *par_ptr = cur->Right;

                                cur->Right = r_node->Left;
                                r_node->Left = stack[i];

                                r_node->Count = cur->Count;
                                cur->Count = cur->Count - r_count + rl_count;

                                i += 2;
                            }
                        }
                    }
                }
                //}

                //if (lvl >= maxLevel)
                //{
                //    var ind = 0;

                //    for (int i = lvl - 1; i > 0; i--)
                //    {
                //        var itm = (Flugs*)(mem + stack[i]);

                //        ind = i;

                //        if ((*itm & Flugs.Data) != Flugs.Data)
                //        {
                //            var cur = (DataNode*)itm;

                //            if (cur->Count > 1000)
                //            {
                //                ind++;

                //                break;
                //            }
                //        }
                //    }

                //    SmartOptimization(stack[ind - 1], stack[ind]);
                //}

                //if (lvl >= maxLevel)
                //{
                //    SmartOptimization(stack[5], stack[6]);
                //}

                //if (lvl >= maxLevel)
                //{
                //    if (lvl >= maxLevel * 2)
                //    {
                //        SmartOptimization(stack[9], stack[10]);
                //    }
                //    else
                //    {
                //        //

                //        for (int i = lvl - 1; i > 0; i--)
                //        {
                //            var itm = (Flugs*)(mem + stack[i]);

                //            if ((*itm & Flugs.Data) != Flugs.Data)
                //            {
                //                var cur = (DataNode*)itm;

                //                if (cur->Count > 1000)
                //                {
                //                    SmartOptimization(stack[i], stack[i-1]);

                //                    break;
                //                }
                //            }
                //        }
                //    }
                //}

                //if (lvl >= maxLevel)
                //{
                //    if (lvl >= maxLevel * 2)
                //    {
                //        SmartOptimization(stack[9], stack[10]);
                //    }
                //    else
                //    {
                //        var c = 0;

                //        for (int i = lvl - 1; i > 0; i--)
                //        {
                //            var itm = (CountNode*)(mem + stack[i]);

                //            c += itm->Count;

                //            if (c >= 1000)
                //            {
                //                SmartOptimization(stack[i], stack[i - 1]);

                //                break;
                //            }
                //        }
                //    }
                //}

                #endregion
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Remove element by code from index
        public void Delete<T>(T obj, int memoryKey, IRecordsComparer<T> comparer, ValueLockRW locker) where T : Record
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }
                
                #region ' Prepare '

                //var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    return;
                }
                else if (*ptr < 0)
                {
                    var cmp = comparer.CompareRecords(obj, -*ptr);

                    if (cmp == 0)
                    {
                        *ptr = 0;
                    }

                    return;
                }

                Flugs* flug;
                var stack = stackalloc int[100];
                var lvl = 0;
                var action = ChangeAction.None;
                var ext_data = 0;

                pos = *ptr;

                #endregion

                #region ' Find place to remove '

                while (true)
                {
                    stack[lvl++] = pos;
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                            #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (TreeNode*)flug;
                            var cmp = comparer.CompareRecords(obj, cur->Data);

                            // left
                            if (cmp < 0)
                            {
                                if (cur->Left > 0)
                                {
                                    pos = cur->Left;

                                    continue;
                                }
                                else if (cur->Left < 0)
                                {
                                    cmp = comparer.CompareRecords(obj, -cur->Left);

                                    if (cmp == 0)
                                    {
                                        action = ChangeAction.RemoveTreeNodeOnLeftLast;

                                        goto exit_find;
                                    }
                                }

                                return;
                            }
                                // right
                            else if (cmp > 0)
                            {
                                if (cur->Right > 0)
                                {
                                    pos = cur->Right;

                                    continue;
                                }
                                else if (cur->Right < 0)
                                {
                                    cmp = comparer.CompareRecords(obj, -cur->Right);

                                    if (cmp == 0)
                                    {
                                        action = ChangeAction.RemoveTreeNodeOnRightLast;

                                        goto exit_find;
                                    }
                                }

                                return;
                            }
                                // remove
                            else
                            {
                                action = ChangeAction.RemoveTreeNode;

                                goto exit_find;
                            }
                        }

                            #endregion

                            #region ' Data '

                        case Flugs.Data:
                        {
                            var cur = (DataNode*)flug;
                            var dat = (int*)(mem + cur->Data);

                            var first = 0;
                            var last = cur->Count - 1;
                            var cmp = comparer.CompareRecords(obj, dat[first]);
                            var cmp_last = comparer.CompareRecords(obj, dat[last]);

                            // not found
                            if (cmp < 0 || cmp_last > 0)
                            {
                                return;
                            }
                                // first
                            else if (cmp == 0)
                            {
                                // remove data node
                                if (cur->Count == 1)
                                {
                                    // Lock
                                    action = ChangeAction.RemoveDataNode;

                                    goto exit_find;
                                }
                                    // 
                                else if (cur->Count == 2)
                                {
                                    action = ChangeAction.RemoveDataNodeTwoFirst;

                                    goto exit_find;
                                }
                                    // remove first node
                                else
                                {
                                    // Lock
                                    action = ChangeAction.RemoveDataNodeFirst;

                                    goto exit_find;
                                }
                            }
                                // last
                            else if (cmp_last == 0)
                            {
                                if (cur->Count == 2)
                                {
                                    action = ChangeAction.RemoveDataNodeTwoLast;

                                    goto exit_find;
                                }
                                else
                                {
                                    action = ChangeAction.RemoveDataNodeLast;

                                    goto exit_find;
                                }
                            }
                            else if (cur->Count == 3)
                            {
                                if (comparer.CompareRecords(obj, dat[1]) == 0)
                                {
                                    action = ChangeAction.RemoveDataNodeThree;

                                    goto exit_find;
                                }

                                return;
                            }
                            else if (cur->Count == 4)
                            {
                                if (comparer.CompareRecords(obj, dat[1]) == 0)
                                {
                                    action = ChangeAction.RemoveDataNodeFour1;

                                    goto exit_find;
                                }
                                else if (comparer.CompareRecords(obj, dat[2]) == 0)
                                {
                                    action = ChangeAction.RemoveDataNodeFour2;

                                    goto exit_find;
                                }

                                return;
                            }

                            // search for position
                            while (first < last)
                            {
                                int mid = (first + last) >> 1;

                                if (comparer.CompareRecords(obj, dat[mid]) <= 0)
                                {
                                    last = mid;
                                }
                                else
                                {
                                    first = mid + 1;
                                }
                            }

                            cmp = comparer.CompareRecords(obj, dat[last]);

                            // not found
                            if (cmp != 0)
                            {
                                return;
                            }

                            action = ChangeAction.RemoveDataNodeMiddle;

                            ext_data = last;

                            goto exit_find;
                        }

                            #endregion

                            #region ' Other '

                        default:
                        {
                            return;
                        }

                            #endregion
                    }
                }

                exit_find:
                ;

                #endregion

                #region ' Correct count '

                for (int i = 0; i < lvl; ++i)
                {
                    flug = (Flugs*)(mem + stack[i]);

                    switch (*flug)
                    {
                            #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (TreeNode*)flug;

                            cur->Count--;

                            break;
                        }

                            #endregion

                            #region ' Data '

                        case Flugs.Data:
                        {
                            var cur = (DataNode*)flug;

                            cur->Count--;

                            break;
                        }

                            #endregion

                            #region ' Other '

                        default:
                        {
                            return;
                        }

                            #endregion
                    }
                }

                exit:

                #endregion

                #region ' Remove '

                int* par_ptr;
                var par = (LinkNode*)(mem + stack[lvl - 2]);
                var cur_pos = stack[lvl - 1];

                // cur node is root
                if (lvl == 1)
                {
                    par_ptr = (int*)(mem + memoryKey);
                }
                else
                {
                    if (par->Right == cur_pos)
                    {
                        par_ptr = &par->Right;
                    }
                        // cur node is left
                    else if (par->Left == cur_pos)
                    {
                        par_ptr = &par->Left;
                    }
                        // bug
                    else
                    {
                        ErrorReport(90);

                        return;
                    }
                }

                switch (action)
                {
                    case ChangeAction.RemoveTreeNodeOnLeftLast:
                    {
                        var cur = (TreeNode*)(mem + cur_pos);

                        cur->Left = 0;

                        if (cur->Right == 0)
                        {
                            *par_ptr = -cur->Data;

                            ReleaseMemory(cur_pos, sizeof(TreeNode));
                        }

                        break;
                    }
                    case ChangeAction.RemoveTreeNodeOnRightLast:
                    {
                        var cur = (TreeNode*)(mem + cur_pos);

                        cur->Right = 0;

                        if (cur->Left == 0)
                        {
                            *par_ptr = -cur->Data;

                            ReleaseMemory(cur_pos, sizeof (TreeNode));
                        }

                        break;
                    }
                    case ChangeAction.RemoveTreeNode:
                    {
                        RemoveTreeNode(par_ptr, cur_pos);

                        break;
                    }
                    case ChangeAction.RemoveDataNode:
                    {
                        *par_ptr = 0;

                        RemoveDataNode(cur_pos);

                        break;
                    }
                    case ChangeAction.RemoveDataNodeTwoFirst:
                    {
                        var cur = (DataNode*)(mem + cur_pos);
                        var dat = (int*)(mem + cur->Data);

                        *par_ptr = -dat[1];

                        RemoveDataNode(cur_pos);

                        break;
                    }
                    case ChangeAction.RemoveDataNodeTwoLast:
                    {
                        var cur = (DataNode*)(mem + cur_pos);
                        var dat = (int*)(mem + cur->Data);

                        *par_ptr = -dat[0];

                        RemoveDataNode(cur_pos);

                        break;
                    }
                    case ChangeAction.RemoveDataNodeFirst:
                    {
                        var cur = (DataNode*)(mem + cur_pos);
                        var dat = (int*)(mem + cur->Data);

                        var tmp = cur->Data;

                        cur->Min = dat[1];
                        cur->Data += 4;

                        ReleaseMemory(tmp, 4);

                        break;
                    }
                    case ChangeAction.RemoveDataNodeLast:
                    {
                        var cur = (DataNode*)(mem + cur_pos);
                        var dat = (int*)(mem + cur->Data);

                        cur->Max = dat[cur->Count - 1];

                        ReleaseMemory(cur->Data + 4 * cur->Count, 4);

                        break;
                    }
                    case ChangeAction.RemoveDataNodeThree:
                    {
                        var cur = (DataNode*)(mem + cur_pos);
                        var dat = (int*)(mem + cur->Data);

                        *par_ptr = CreateTreeNode(dat[2], -dat[0], 0, 2);

                        RemoveDataNode(cur_pos);

                        break;
                    }
                    case ChangeAction.RemoveDataNodeFour1:
                    {
                        var cur = (DataNode*)(mem + cur_pos);
                        var dat = (int*)(mem + cur->Data);

                        *par_ptr = CreateTreeNode(dat[2], -dat[0], -dat[3], 3);

                        RemoveDataNode(cur_pos);

                        break;
                    }
                    case ChangeAction.RemoveDataNodeFour2:
                    {
                        var cur = (DataNode*)(mem + cur_pos);
                        var dat = (int*)(mem + cur->Data);

                        *par_ptr = CreateTreeNode(dat[1], -dat[0], -dat[3], 3);

                        RemoveDataNode(cur_pos);

                        break;
                    }
                    case ChangeAction.RemoveDataNodeMiddle:
                    {
                        var cur = (DataNode*)(mem + cur_pos);
                        var dat = (int*)(mem + cur->Data);
                        var last = ext_data;

                        // left shift
                        if (cur->Count - last <= 500)
                        {
                            for (int n = last; n < cur->Count; ++n)
                            {
                                dat[n] = dat[n + 1];
                            }

                            ReleaseMemory(cur->Data + cur->Count, sizeof (int));

                            break;
                        }

                        // right shift
                        if (last <= 500)
                        {
                            for (int n = last; n > 0; --n)
                            {
                                dat[n] = dat[n - 1];
                            }

                            cur->Data += 4;

                            ReleaseMemory(cur->Data, sizeof (int));

                            break;
                        }

                        var adr_right = CreateDataNode();
                        var data_new = (DataNode*)(mem + adr_right);

                        data_new->Count = cur->Count - last - 1;
                        data_new->Min = dat[last + 2];
                        data_new->Max = cur->Max;
                        data_new->Data = cur->Data + (last + 2) * sizeof (int);

                        *par_ptr = CreateTreeNode(dat[last + 1], cur_pos, adr_right, cur->Count);

                        cur->Count = last;
                        cur->Max = dat[last - 1];

                        ReleaseMemory(cur->Data + last, sizeof (int));

                        break;
                    }
                    default:
                    {
                        return;
                    }
                }

                #endregion
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Check containce element by code
        public bool Contains<T>(T obj, int memoryKey, IRecordsComparer<T> comparer, ValueLockRW locker) where T : Record
        {
            try
            {
                if (locker != null)
                {
                    locker.ReadLock();
                }
                
                var ptr = (int*)(mem + memoryKey);

                if (*ptr == 0)
                {
                    return false;
                }
                else if (*ptr < 0)
                {
                    var cmp = comparer.CompareRecords(obj, -*ptr);

                    if (cmp == 0)
                    {
                        return true;
                    }

                    return false;
                }

                var flug = (Flugs*)(mem + *ptr);

                while (true)
                {
                    #region ' Tree '

                    if (*flug == Flugs.Tree)
                    {
                        var cur = (TreeNode*)flug;
                        var cmp = comparer.CompareRecords(obj, cur->Data);

                        if (cmp > 0)
                        {
                            if (cur->Right == 0)
                            {
                                return false;
                            }
                            else if (cur->Right < 0)
                            {
                                cmp = comparer.CompareRecords(obj, -cur->Right);

                                if (cmp == 0)
                                {
                                    return true;
                                }
                                else
                                {
                                    return false;
                                }
                            }
                            else
                            {
                                flug = (Flugs*)(mem + cur->Right);
                            }
                        }
                        else if (cmp < 0)
                        {
                            if (cur->Left == 0)
                            {
                                return false;
                            }
                            else if (cur->Left < 0)
                            {
                                cmp = comparer.CompareRecords(obj, -cur->Left);

                                if (cmp == 0)
                                {
                                    return true;
                                }
                                else
                                {
                                    return false;
                                }
                            }
                            else
                            {
                                flug = (Flugs*)(mem + cur->Left);
                            }
                        }
                        else
                        {
                            return true;
                        }
                    }

                        #endregion

                    #region ' Data '

                    else
                    {
                        var cur = (DataNode*)flug;

                        if (comparer.CompareRecords(obj, cur->Max) > 0)
                        {
                            return false;
                        }

                        if (comparer.CompareRecords(obj, cur->Min) < 0)
                        {
                            return false;
                        }

                        return true;
                    }

                    #endregion
                }
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }
        
        // Find code
        public int Find<T>(T obj, int memoryKey, IKeyComparer<T> comparer, ValueLockRW locker) where T : IComparable<T>
        {
            try
            {
                if (locker != null)
                {
                    locker.ReadLock();
                }
                
                var ptr = (int*)(mem + memoryKey);

                if (*ptr == 0)
                {
                    return 0;
                }
                else if (*ptr < 0)
                {
                    var cmp = comparer.Compare(obj, -*ptr);

                    if (cmp == 0)
                    {
                        return -*ptr;
                    }

                    return 0;
                }

                var flug = (Flugs*)(mem + *ptr);

                while (true)
                {
                    #region ' Tree '

                    if ((*flug & Flugs.Tree) == Flugs.Tree)
                    {
                        var cur = (TreeNode*)flug;
                        var cmp = comparer.Compare(obj, cur->Data);

                        if (cmp == 0)
                        {
                            return cur->Data;
                        }
                            // left
                        else if (cmp < 0)
                        {
                            if (cur->Left < 0)
                            {
                                cmp = comparer.Compare(obj, -cur->Left);

                                if (cmp == 0)
                                {
                                    return -cur->Left;
                                }
                            }
                            else if (cur->Left > 0)
                            {
                                flug = (Flugs*)(mem + cur->Left);

                                continue;
                            }
                        }
                            // right
                        else
                        {
                            if (cur->Right < 0)
                            {
                                cmp = comparer.Compare(obj, -cur->Right);

                                if (cmp == 0)
                                {
                                    return -cur->Right;
                                }
                            }
                            else if (cur->Right > 0)
                            {
                                flug = (Flugs*)(mem + cur->Right);

                                continue;
                            }
                        }

                        return 0;
                    }

                        #endregion

                    #region ' Data '

                    else if ((*flug & Flugs.Data) == Flugs.Data)
                    {
                        var cur = (DataNode*)flug;
                        var dat = (int*)(mem + cur->Data);

                        int first = 0;
                        int last = cur->Count - 1;

                        // position is before 0
                        if (comparer.Compare(obj, dat[first]) < 0)
                        {
                            return dat[first];
                        }
                            // position is after last
                        else if (comparer.Compare(obj, dat[last]) > 0)
                        {
                            return dat[last];
                        }
                            // position middle
                        else
                        {
                            // search for position
                            while (first < last)
                            {
                                var mid = (first + last) >> 1;

                                if (comparer.Compare(obj, dat[mid]) <= 0)
                                {
                                    last = mid;
                                }
                                else
                                {
                                    first = mid + 1;
                                }
                            }

                            if (comparer.Compare(obj, dat[last]) == 0)
                            {
                                return dat[last];
                            }
                                // Здесь проверить другие алго.
                            else
                            {
                                return 0;
                            }
                        }
                    }

                        #endregion

                    #region ' Other '

                    else
                    {
                        return 0;
                    }

                    #endregion
                }

            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Очистить и освободить память
        public void ClearTree(int memoryKey, ValueLockRW locker)
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }

                var ptr = (int*)(mem + memoryKey);

                if (*ptr == 0)
                {
                    return;
                }
                else if (*ptr < 0)
                {
                    *ptr = 0;

                    return;
                }

                Flugs* flug;
                var lvl = 0;
                var pos = *ptr;
                var stack = stackalloc int[100];

                while (true)
                {
                    flug = (Flugs*)(mem + pos);
                    stack[lvl++] = pos;

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (TreeNode*)flug;
                            
                            // goto left
                            if (cur->Left > 0)
                            {
                                pos = cur->Left;
                            }
                            // goto left
                            else if (cur->Right > 0)
                            {
                                pos = cur->Right;
                            }
                            // remove
                            else
                            {
                                ReleaseMemory(pos, sizeof(TreeNode));

                                // root node
                                if (lvl == 1)
                                {
                                    *ptr = 0;

                                    return;
                                }

                                lvl -= 2;

                                var par = (TreeNode*)(mem + stack[lvl]);

                                if (par->Left == pos)
                                {
                                    par->Left = 0;
                                }
                                else
                                {
                                    par->Right = 0;
                                }
                            }

                            break;
                        }

                        #endregion

                        #region ' Other '

                        default:
                        {
                            ErrorReport(111);

                            return;
                        }

                        #endregion
                    }
                }
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        #endregion

        #region ' Group '

        // Inserting new element to index
        public void InsertToGroup<G,T>(int memoryKey, G grp, T obj, IGroupComparer<G> g_comparer, IRecordsComparer<T> t_comparer, ValueLockRW locker) where G : IComparable<G> where T : Record
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }

                MakeSpace();

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    *ptr = -obj.Code;

                    return;
                }
                else if (*ptr < 0)
                {
                    cmp = g_comparer.CompareGroups(grp, -*ptr);

                    if (cmp < 0)
                    {
                        *ptr = CreateGroupNode(*ptr, -obj.Code, 0, 2);
                    }
                    else if (cmp > 0)
                    {
                        *ptr = CreateGroupNode(-obj.Code, *ptr, 0, 2);
                    }
                    else
                    {
                        cmp = t_comparer.CompareRecords(obj, -*ptr);

                        if (cmp > 0)
                        {
                            var tmp = CreateTreeNode(obj.Code, *ptr, 0, 2);

                            *ptr = CreateGroupNode(tmp, 0, 0, 1);
                        }
                        else if (cmp < 0)
                        {
                            var tmp = CreateTreeNode(-*ptr, -obj.Code, 0, 2);

                            *ptr = CreateGroupNode(tmp, 0, 0, 1);
                        }
                    }

                    return;
                }

                var action = ChangeAction.None;
                var stack = stackalloc int[100];
                var lvl = 0;
                var ext_data = 0;
                Flugs* flug;

                var cur_data = 0;

                pos = *ptr;

                #endregion

                #region ' Find place to insert '
               
                while (true)
                {
                    stack[lvl++] = pos;
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                            {
                                var cur = (GroupNode*)flug;
                                
                                cur_data = -cur->Data;

                                if (cur->Data > 0)
                                {
                                    var tmp_tree_flug = (Flugs*)(mem + cur->Data);

                                    switch (*tmp_tree_flug)
                                    {
                                        case Flugs.Tree:
                                        {
                                            var tmp_tree = (TreeNode*)tmp_tree_flug;

                                            cur_data = tmp_tree->Data;

                                            break;
                                        }
                                        case Flugs.Data:
                                        {
                                            var tmp_tree = (DataNode*)tmp_tree_flug;
                                            var tmp_tree_dat = (int*)(mem + tmp_tree->Data);

                                            cur_data = tmp_tree_dat[0];

                                            break;
                                        }
                                    }
                                }

                                cmp = g_comparer.CompareGroups(grp, cur_data);

                                // already exist
                                if (cmp == 0)
                                {
                                    Insert(obj, pos + 1, t_comparer, false, null);

                                    cur = (GroupNode*)(mem + pos);

                                    return;
                                }
                                // left
                                else if (cmp < 0)
                                {
                                    if (cur->Left == 0)
                                    {
                                        // Lock
                                        action = ChangeAction.InsertTreeNodeLeftLast;

                                        goto insert;
                                    }
                                    else if (cur->Left < 0)
                                    {
                                        cmp = g_comparer.CompareGroups(grp, -cur->Left);

                                        if (cmp == 0)
                                        {
                                            cmp = t_comparer.CompareRecords(obj, -cur->Left);

                                            if (cmp > 0)
                                            {
                                                var tmp = CreateTreeNode(obj.Code, cur->Left, 0, 2);

                                                cur->Left = CreateGroupNode(tmp, 0, 0, 1);
                                            }
                                            else if (cmp < 0)
                                            {
                                                var tmp = CreateTreeNode(-cur->Left, -obj.Code, 0, 2);

                                                cur->Left = CreateGroupNode(tmp, 0, 0, 1);
                                            }

                                            goto exit; 
                                        }

                                        // Lock
                                        action = ChangeAction.InsertTreeNodeLeftTree;

                                        goto insert;
                                    }
                                    else
                                    {
                                        pos = cur->Left;

                                        continue;
                                    }
                                }
                                // right
                                else
                                {
                                    if (cur->Right == 0)
                                    {
                                        // Lock
                                        action = ChangeAction.InsertTreeNodeRightLast;

                                        goto insert;
                                    }
                                    else if (cur->Right < 0)
                                    {
                                        cmp = g_comparer.CompareGroups(grp, -cur->Right);

                                        if (cmp == 0)
                                        {
                                            cmp = t_comparer.CompareRecords(obj, -cur->Right);

                                            if (cmp > 0)
                                            {
                                                var tmp = CreateTreeNode(obj.Code, cur->Right, 0, 2);

                                                cur->Right = CreateGroupNode(tmp, 0, 0, 1);
                                            }
                                            else if (cmp < 0)
                                            {
                                                var tmp = CreateTreeNode(-cur->Right, -obj.Code, 0, 2);

                                                cur->Right = CreateGroupNode(tmp, 0, 0, 1);
                                            }

                                            goto exit;
                                        }

                                        // Lock
                                        action = ChangeAction.InsertTreeNodeRightTree;

                                        goto insert;
                                    }
                                    else
                                    {
                                        pos = cur->Right;

                                        continue;
                                    }
                                }
                            }

                        #endregion

                        #region ' Data '

                        case Flugs.Data:
                            {
                                var cur = (DataNode*)flug;
                                var dat = (int*)(mem + cur->Data);

                                int first = 0;
                                int last = cur->Count - 1;

                                // position is before 0
                                if (t_comparer.CompareRecords(obj, dat[first]) < 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertDataNodeFirst;

                                    goto insert;
                                }
                                // position is after last
                                else if (t_comparer.CompareRecords(obj, dat[last]) > 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertDataNodeLast;

                                    goto insert;
                                }
                                // position middle
                                else
                                {
                                    // search for position
                                    while (first < last)
                                    {
                                        int mid = (first + last) >> 1;

                                        if (t_comparer.CompareRecords(obj, dat[mid]) <= 0)
                                        {
                                            last = mid;
                                        }
                                        else
                                        {
                                            first = mid + 1;
                                        }
                                    }

                                    // already exist
                                    if (t_comparer.CompareRecords(obj, dat[last]) == 0 )//&& isUnique
                                    {
                                        return;
                                    }

                                    // Insertion index found. last - desired position
                                    ext_data = last;

                                    // Lock
                                    action = ChangeAction.InsertDataNodeMiddle;

                                    goto insert;
                                }
                            }

                        #endregion

                        #region ' Other '

                        default:
                            {
                                Debugger.Break();

                                break;
                            }

                        #endregion
                    }
                }

            insert: ;

                #endregion

                #region ' Correct count '

                for (int i = 0; i < lvl; ++i)
                {
                    flug = (Flugs*)(mem + stack[i]);

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                            {
                                var cur = (GroupNode*)flug;

                                cur->Count++;

                                break;
                            }

                        #endregion

                        #region ' Data '

                        case Flugs.Data:
                            {
                                continue;
                            }

                        #endregion

                        #region ' Other '

                        default:
                            {
                                return;
                            }

                        #endregion
                    }
                }

                #endregion

                #region ' Insert '

                switch (action)
                {
                    case ChangeAction.InsertTreeNodeLeftLast:
                    {
                        var cur = (GroupNode*)flug;

                        cur->Left = -obj.Code;

                        goto exit; //return;
                    }
                    case ChangeAction.InsertTreeNodeLeftTree:
                    {
                        var cur = (GroupNode*)flug;

                        // micro rotate
                        if (cur->Right == 0)
                        {
                            if (cur->Data < 0)
                            {
                                cur->Right = cur->Data;
                            }
                            else
                            {
                                cur->Right = CreateGroupNode(cur->Data, 0, 0, 1);
                            }

                            if (cmp < 0)
                            {
                                cur->Data = cur->Left;
                                cur->Left = -obj.Code;
                            }
                            else if (cmp > 0)
                            {
                                cur->Data = -obj.Code;
                            }
                        }
                        else
                        {
                            if (cmp < 0)
                            {
                                cur->Left = CreateGroupNode(cur->Left, -obj.Code, 0, 2);
                            }
                            else
                            {
                                cur->Left = CreateGroupNode(cur->Left, 0, -obj.Code, 2);
                            }
                        }

                        goto exit;
                    }
                    case ChangeAction.InsertTreeNodeRightLast:
                    {
                        var cur = (GroupNode*)flug;
                        cur->Right = -obj.Code;

                        goto exit; //return;
                    }
                    case ChangeAction.InsertTreeNodeRightTree:
                    {
                        var cur = (GroupNode*)flug;

                        // micro rotate
                        if (cur->Left == 0)
                        {
                            if (cur->Data < 0)
                            {
                                cur->Left = cur->Data;
                            }
                            else
                            {
                                cur->Left = CreateGroupNode(cur->Data, 0, 0, 1);
                            }

                            if (cmp < 0)
                            {
                                cur->Data = -obj.Code;
                            }
                            else if (cmp > 0)
                            {
                                cur->Data = cur->Right;
                                cur->Right = -obj.Code;
                            }
                        }
                        else
                        {
                            if (cmp < 0)
                            {
                                cur->Right = CreateGroupNode(cur->Right, -obj.Code, 0, 2);
                            }
                            else
                            {
                                cur->Right = CreateGroupNode(cur->Right, 0, -obj.Code, 2);
                            }
                        }

                        goto exit;
                    }
                    case ChangeAction.InsertDataNodeFirst:
                    {
                        var cur = (DataNode*)flug;
                        var par = (LinkNode*)(mem + stack[lvl - 2]); // correct link
                        int* par_ptr;

                        // get parent link
                        if (par->Left == pos)
                        {
                            par_ptr = &par->Left;
                        }
                        else if (par->Right == pos)
                        {
                            par_ptr = &par->Right;
                        }
                        else
                        {
                            ErrorReport(90);

                            return;
                        }

                        var adr = CreateTreeNode(obj.Code, 0, *par_ptr, cur->Count + 1);

                        *par_ptr = adr;

                        goto exit;
                    }
                    case ChangeAction.InsertDataNodeLast:
                    {
                        var cur = (DataNode*)flug;
                        var par = (LinkNode*)(mem + stack[lvl - 2]);
                        int* par_ptr;

                        // get parent link
                        if (par->Left == pos)
                        {
                            par_ptr = &par->Left;
                        }
                        else if (par->Right == pos)
                        {
                            par_ptr = &par->Right;
                        }
                        else
                        {
                            ErrorReport(90);

                            return;
                        }

                        var adr = CreateTreeNode(obj.Code, *par_ptr, 0, cur->Count + 1);

                        *par_ptr = adr;

                        goto exit;
                    }
                    case ChangeAction.InsertDataNodeMiddle:
                    {
                        var cur = (DataNode*)flug;
                        var dat = (int*)(mem + cur->Data);
                        var par = (LinkNode*)(mem + stack[lvl - 2]);
                        int* par_ptr;

                        // get parent link
                        if (par->Left == pos)
                        {
                            par_ptr = &par->Left;
                        }
                        else if (par->Right == pos)
                        {
                            par_ptr = &par->Right;
                        }
                        else
                        {
                            ErrorReport(90);

                            return;
                        }

                        var last = ext_data;

                        // create right data node
                        var right_pos = CreateDataNode();
                        var right_node = (DataNode*)(mem + right_pos);

                        right_node->Count = cur->Count - last;
                        right_node->Min = dat[last];
                        right_node->Max = cur->Max;
                        right_node->Data = cur->Data + last * sizeof(int);

                        var c = cur->Count;

                        // correct cur data node
                        cur->Count = last;
                        cur->Max = dat[last - 1];

                        // create tree node
                        *par_ptr = CreateTreeNode(obj.Code, pos, right_pos, c + 1);

                        goto exit;
                    }
                    default:
                    {
                        return;
                    }
                }

            exit:
                #endregion

                #region ' Optimization data structure '

                if (lvl >= maxLevel)
                {
                    //if (lvl >= maxLevel * 2)
                    //{
                    //    SmartOptimization(stack[9], stack[10]);
                    //}
                    //else
                    //{
                    //var c = 0;

                    //for (int i = lvl - 1; i > 0; i--)

                    for (int i = 1; i < lvl; ++i)
                    {
                        var par = (GroupNode*)(mem + stack[i - 1]);
                        var itm = (Flugs*)(mem + stack[i]);

                        int* par_ptr;

                        if (par->Left == stack[i])
                        {
                            par_ptr = &par->Left;
                        }
                        else if (par->Right == stack[i])
                        {
                            par_ptr = &par->Right;
                        }
                        else
                        {
                            ErrorReport(65);

                            return;
                        }

                        if (*itm == Flugs.Tree)
                        {
                            var cur = (GroupNode*)itm;
                            var l_count = 0;
                            var r_count = 0;

                            if (cur->Left < 0)
                            {
                                l_count = 1;
                            }
                            else if (cur->Left > 0)
                            {
                                var f = (Flugs*)(mem + cur->Left);

                                if (*f == Flugs.Tree)
                                {
                                    l_count = ((GroupNode*)f)->Count;
                                }
                                else
                                {
                                    l_count = 1;
                                }
                            }

                            if (cur->Right < 0)
                            {
                                r_count = 1;
                            }
                            else if (cur->Right > 0)
                            {
                                var f = (Flugs*)(mem + cur->Right);

                                if (*f == Flugs.Tree)
                                {
                                    r_count = ((GroupNode*)f)->Count;
                                }
                                else
                                {
                                    r_count = 1;
                                }
                            }

                            var l_lvl = GetLevel(l_count);
                            var r_lvl = GetLevel(r_count);

                            cmp = l_lvl - r_lvl;

                            // To right
                            if (cmp > 1)
                            {
                                var l_flug = (Flugs*)(mem + cur->Left);

                                if (*l_flug != Flugs.Tree)
                                {
                                    continue;
                                }

                                var l_node = (GroupNode*)l_flug;
                                var lr_count = 0;

                                if (l_node->Right > 0)
                                {
                                    lr_count = ((GroupNode*)(mem + l_node->Right))->Count;
                                }
                                else if (l_node->Right < 0)
                                {
                                    lr_count = 1;
                                }

                                *par_ptr = cur->Left;

                                cur->Left = l_node->Right;
                                l_node->Right = stack[i];

                                l_node->Count = cur->Count;
                                cur->Count = cur->Count - l_count + lr_count;

                                i += 2;
                            }
                            // to left
                            else if (cmp < -1)
                            {
                                var r_flug = (Flugs*)(mem + cur->Right);

                                if (*r_flug != Flugs.Tree)
                                {
                                    continue;
                                }

                                var r_node = (GroupNode*)r_flug;
                                var rl_count = 0;

                                if (r_node->Left > 0)
                                {
                                    rl_count = ((GroupNode*)(mem + r_node->Left))->Count;
                                }
                                else if (r_node->Left < 0)
                                {
                                    rl_count = 1;
                                }

                                *par_ptr = cur->Right;

                                cur->Right = r_node->Left;
                                r_node->Left = stack[i];

                                r_node->Count = cur->Count;
                                cur->Count = cur->Count - r_count + rl_count;

                                i += 2;
                            }
                        }
                    }
                }

                #endregion
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Receiving a code group. Actually it turns out the code of any member of this group characteristic.
        public int GetGroupTree<T>(int memoryKey, T grp, IGroupComparer<T> g_comparer, ValueLockRW locker) where T : IComparable<T>
        {
            try
            {
                if (locker != null)
                {
                    locker.ReadLock();
                }

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    return 0;
                }
                else if (*ptr < 0)
                {
                    cmp = g_comparer.CompareGroups(grp, -*ptr);

                    if (cmp == 0)
                    {
                        return pos;
                    }

                    return 0;
                }

                Flugs* flug;
                pos = *ptr;

                #endregion

                #region ' Find '

                while (true)
                {
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                        case Flugs.Tree:
                        {
                            var cur = (GroupNode*)(mem + pos);

                            var cur_data = -cur->Data;

                            if (cur->Data > 0)
                            {
                                var tmp_tree_flug = (Flugs*)(mem + cur->Data);

                                switch (*tmp_tree_flug)
                                {
                                    case Flugs.Tree:
                                    {
                                        var tmp_tree = (TreeNode*)tmp_tree_flug;

                                        cur_data = tmp_tree->Data;

                                        break;
                                    }
                                    case Flugs.Data:
                                    {
                                        var tmp_tree = (DataNode*)tmp_tree_flug;
                                        var tmp_tree_dat = (int*)(mem + tmp_tree->Data);

                                        cur_data = tmp_tree_dat[0];

                                        break;
                                    }
                                }
                            }

                            cmp = g_comparer.CompareGroups(grp, cur_data);

                            // already exist
                            if (cmp == 0)
                            {
                                return pos + 1;
                            }
                            // left
                            else if (cmp < 0)
                            {
                                if (cur->Left > 0)
                                {
                                    pos = cur->Left;

                                    continue;
                                }
                                else if (cur->Left < 0)
                                {
                                    cmp = g_comparer.CompareGroups(grp, -cur->Left);

                                    if (cmp == 0)
                                    {
                                        var off = (byte*)&cur->Left - (byte*)cur;

                                        pos += (int)off;

                                        return pos;
                                    }
                                }

                                return 0;
                            }
                            // right
                            else
                            {
                                if (cur->Right > 0)
                                {
                                    pos = cur->Right;

                                    continue;
                                }
                                else if (cur->Right < 0)
                                {
                                    cmp = g_comparer.CompareGroups(grp, -cur->Right);

                                    if (cmp == 0)
                                    {
                                        var off = (byte*)&cur->Right - (byte*)cur;
                                        
                                        pos += (int)off;

                                        return pos;
                                    }
                                }

                                return 0;
                            }
                        }
                        case Flugs.Data:
                        {

                            break;
                        }
                    }

                }

                #endregion

            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Delete element from group
        public void DeleteFromGroup<G, T>(int memoryKey, G grp, T obj, IGroupComparer<G> g_comparer, IRecordsComparer<T> t_comparer, ValueLockRW locker) where G : IComparable<G> where T : Record
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    return;
                }
                else if (*ptr < 0)
                {
                    cmp = g_comparer.CompareGroups(grp, -*ptr);

                    if (cmp == 0 && t_comparer.CompareRecords(obj, -*ptr) == 0)
                    {
                        *ptr = 0;
                    }

                    return;
                }

                var action = ChangeAction.None;
                var stack = stackalloc int[100];
                var lvl = 0;
                var ext_data = 0;
                Flugs* flug;

                var cur_data = 0;

                pos = *ptr;

                #endregion

                #region ' Find '

                while (true)
                {
                    stack[lvl++] = pos;
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                        case Flugs.Tree:
                        {
                            var cur = (GroupNode*)flug;

                            cur_data = -cur->Data;

                            if (cur->Data > 0)
                            {
                                var tmp_tree_flug = (Flugs*)(mem + cur->Data);

                                switch (*tmp_tree_flug)
                                {
                                    case Flugs.Tree:
                                    {
                                        var tmp_tree = (TreeNode*)tmp_tree_flug;

                                        cur_data = tmp_tree->Data;

                                        break;
                                    }
                                    case Flugs.Data:
                                    {
                                        var tmp_tree = (DataNode*)tmp_tree_flug;
                                        var tmp_tree_dat = (int*)(mem + tmp_tree->Data);

                                        cur_data = tmp_tree_dat[0];

                                        pos = 1; // todo найти позицию элемента в массиве. если остается 0 элементов удалить массив и все остальные действия связанные с массивами...

                                        break;
                                    }
                                }
                            }

                            cmp = g_comparer.CompareGroups(grp, cur_data);

                            // already exist
                            if (cmp == 0)
                            {
                                Delete(obj, pos + 1, t_comparer, null);

                                var pos_ptr = (int*)(mem + pos + 1);

                                if (*pos_ptr == 0)
                                {
                                    action = ChangeAction.RemoveTreeNode;

                                    goto remove;
                                }

                                return;
                            }
                            // left
                            else if (cmp < 0)
                            {
                                if (cur->Left > 0)
                                {
                                    pos = cur->Left;

                                    continue;
                                }
                                
                                if (cur->Left < 0)
                                {
                                    cmp = g_comparer.CompareGroups(grp, -cur->Left);

                                    if (cmp == 0 && t_comparer.CompareRecords(obj, -cur->Left) == 0)
                                    {
                                        cur->Left = 0;
                                        
                                        goto remove;
                                    }
                                }

                                return;
                            }
                            // right
                            else
                            {
                                if (cur->Right > 0)
                                {
                                    pos = cur->Right;

                                    continue;
                                }
                                
                                if (cur->Right < 0)
                                {
                                    cmp = g_comparer.CompareGroups(grp, -cur->Right);

                                    if (cmp == 0 && t_comparer.CompareRecords(obj, -cur->Right) == 0)
                                    {
                                        cur->Right = 0;
                                        
                                        goto remove;
                                    }
                                }

                                return;
                            }
                        }
                    }
                }
                 
            remove: 

                #endregion

                #region ' Correct count '

                for (int i = 0; i < lvl; ++i)
                {
                    flug = (Flugs*)(mem + stack[i]);

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                            {
                                var cur = (GroupNode*)flug;

                                cur->Count--;

                                break;
                            }

                        #endregion

                        #region ' Data '

                        case Flugs.Data:
                            {
                                var cur = (DataNode*)flug;

                                cur->Count--;

                                break;
                            }

                        #endregion

                        #region ' Other '

                        default:
                            {
                                return;
                            }

                        #endregion
                    }
                }

                #endregion

                #region ' Remove '

                int* par_ptr;
                //var isLeft = false;
                var par = (GroupNode*)(mem + stack[lvl - 2]);

                var cur_pos = stack[lvl - 1];

                // cur node is root
                if (lvl == 1)
                {
                    par_ptr = (int*)(mem + memoryKey);
                }
                else
                {
                    if (par->Right == cur_pos)
                    {
                        par_ptr = &par->Right;
                    }
                    // cur node is left
                    else if (par->Left == cur_pos)
                    {
                        par_ptr = &par->Left;
                    }
                    // bug
                    else
                    {
                        ErrorReport(90);

                        return;
                    }
                }

                switch (action)
                {
                    case ChangeAction.RemoveTreeNode:
                    {
                        var cur = (GroupNode*)(mem + cur_pos);

                        if (cur->Left == 0 && cur->Right == 0)
                        {
                            *par_ptr = 0;

                            ReleaseMemory(cur_pos, sizeof(GroupNode));
                        }
                        else if (cur->Left == 0)
                        {
                            *par_ptr = cur->Right;

                            ReleaseMemory(cur_pos, sizeof(GroupNode));
                        }
                        else if (cur->Right == 0)
                        {
                            *par_ptr = cur->Left;

                            ReleaseMemory(cur_pos, sizeof(GroupNode));
                        }
                        else if (cur->Left < 0)
                        {
                            cur->Data = cur->Left;
                            cur->Left = 0;
                        }
                        else if (cur->Right < 0)
                        {
                            cur->Data = cur->Right;
                            cur->Right = 0;
                        }
                        else
                        {
                            par_ptr = &cur->Right;

                            var tmp_pos = cur->Right;
                            var tmp = (GroupNode*)(mem + tmp_pos);

                            while (tmp->Left > 0)
                            {
                                tmp->Count--;

                                par_ptr = &tmp->Left;
                                tmp_pos = tmp->Left;
                                tmp = (GroupNode*)(mem + tmp_pos);
                            }

                            tmp->Count--;

                            if (tmp->Left < 0)
                            {
                                cur->Data = tmp->Left;
                                tmp->Left = 0;

                                //if (tmp->Right == 0)
                                //{
                                //    *par_ptr = -tmp->Data;
                                //    ReleaseMemory(tmp_pos, sizeof(TreeNode));
                                //}

                                return;
                            }

                            cur->Data = tmp->Data; // todo необходимо применить политику не изменения адреса

                            ReleaseMemory(tmp_pos, sizeof(GroupNode));
                            *par_ptr = tmp->Right;
                        }

                        break;
                    }
                    //default:
                    //{
                    //    return;
                    //}
                }

                #endregion
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Очистить и освободить память
        public void ClearGroup(int memoryKey, ValueLockRW locker)
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }

                var ptr = (int*)(mem + memoryKey);

                if (*ptr == 0)
                {
                    return;
                }
                else if (*ptr < 0)
                {
                    *ptr = 0;

                    return;
                }

                Flugs* flug;
                var lvl = 0;
                var pos = *ptr;
                var stack = stackalloc int[100];

                while (true)
                {
                    flug = (Flugs*)(mem + pos);
                    stack[lvl++] = pos;

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (GroupNode*)flug;

                            // goto left
                            if (cur->Left > 0)
                            {
                                pos = cur->Left;
                            }
                            // goto left
                            else if (cur->Right > 0)
                            {
                                pos = cur->Right;
                            }
                            // remove
                            else
                            {
                                // Очищаем дерево элементов этой группы
                                ClearTree(cur->Data, null);

                                ReleaseMemory(pos, sizeof(GroupNode));

                                // root node
                                if (lvl == 1)
                                {
                                    *ptr = 0;

                                    return;
                                }

                                lvl -= 2;

                                var par = (GroupNode*)(mem + stack[lvl]);

                                if (par->Left == pos)
                                {
                                    par->Left = 0;
                                }
                                else
                                {
                                    par->Right = 0;
                                }
                            }

                            break;
                        }

                        #endregion

                        #region ' Other '

                        default:
                        {
                            ErrorReport(111);

                            return;
                        }

                        #endregion
                    }
                }
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Количество групп
        public int GetGroupsCount(int memoryKey, ValueLockRW locker)
        {
            try
            {
                if (locker != null)
                {
                    locker.ReadLock();
                }

                #region ' Prepare '

                var pos = memoryKey;
                var ptr = (int*)(mem + memoryKey);

                if (*ptr == 0)
                {
                    return 0;
                }
                else if (*ptr < 0)
                {
                    return 1;
                }

                var node = (GroupNode*)*ptr;

                return node->Count;

                #endregion

            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        #endregion

        #region ' Free Group '

        // In development
        // Inserting new element to index
        public void InsertToFreeGroup<T>(int memoryKey, int grp, T obj, IRecordsComparer<T> t_comparer, ValueLockRW locker) where T : Record
        {
            try
            {
                locker?.WriteLock();

                MakeSpace();

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    *ptr = CreateMultyNode(grp, 0, 0, 1, - obj.Code);

                    return;
                }

                var action = ChangeAction.None;
                var stack = stackalloc int[100];
                var lvl = 0;
                var ext_data = 0;
                Flugs* flug;

                var cur_data = 0;

                pos = *ptr;

                #endregion

                #region ' Find place to insert '

                while (true)
                {
                    stack[lvl++] = pos;
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (MultyNode*)flug;

                            cmp = grp.CompareTo(cur->Data);

                            // already exist
                            if (cmp == 0)
                            {
                                Insert(obj, pos + 9, t_comparer, false, null);

                                cur = (MultyNode*)(mem + pos);

                                return;
                            }
                           // left
                            else if (cmp < 0)
                            {
                                if (cur->Left == 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertTreeNodeLeftLast;

                                    goto insert;
                                }
                                else
                                {
                                    pos = cur->Left;

                                    continue;
                                }
                            }
                            // right
                            else
                            {
                                if (cur->Right == 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertTreeNodeRightLast;

                                    goto insert;
                                }
                                else
                                {
                                    pos = cur->Right;

                                    continue;
                                }
                            }
                        }

                        #endregion

                        #region ' Data '

                        case Flugs.Data:
                            {
                                var cur = (DataNode*)flug;
                                var dat = (int*)(mem + cur->Data);

                                int first = 0;
                                int last = cur->Count - 1;

                                // position is before 0
                                if (t_comparer.CompareRecords(obj, dat[first]) < 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertDataNodeFirst;

                                    goto insert;
                                }
                                // position is after last
                                else if (t_comparer.CompareRecords(obj, dat[last]) > 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertDataNodeLast;

                                    goto insert;
                                }
                                // position middle
                                else
                                {
                                    // search for position
                                    while (first < last)
                                    {
                                        int mid = (first + last) >> 1;

                                        if (t_comparer.CompareRecords(obj, dat[mid]) <= 0)
                                        {
                                            last = mid;
                                        }
                                        else
                                        {
                                            first = mid + 1;
                                        }
                                    }

                                    // already exist
                                    if (t_comparer.CompareRecords(obj, dat[last]) == 0)//&& isUnique
                                    {
                                        return;
                                    }

                                    // Insertion index found. last - desired position
                                    ext_data = last;

                                    // Lock
                                    action = ChangeAction.InsertDataNodeMiddle;

                                    goto insert;
                                }
                            }

                        #endregion

                        #region ' Other '

                        default:
                            {
                                Debugger.Break();

                                break;
                            }

                        #endregion
                    }
                }

            insert: ;

                #endregion

                #region ' Correct count '

                for (int i = 0; i < lvl; ++i)
                {
                    flug = (Flugs*)(mem + stack[i]);

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                            {
                                var cur = (MultyNode*)flug;

                                cur->Count++;

                                break;
                            }

                        #endregion

                        #region ' Data '

                        case Flugs.Data:
                            {
                                continue;
                            }

                        #endregion

                        #region ' Other '

                        default:
                            {
                                return;
                            }

                        #endregion
                    }
                }

                #endregion

                #region ' Insert '

                switch (action)
                {
                    case ChangeAction.InsertTreeNodeLeftLast:
                    {
                        var cur = (MultyNode*)flug;

                        cur->Left = CreateMultyNode(grp, 0, 0, 1, -obj.Code);

                        goto exit; //return;
                    }
                    case ChangeAction.InsertTreeNodeRightLast:
                    {
                        var cur = (MultyNode*)flug;

                        cur->Right = CreateMultyNode(grp, 0, 0, 1, -obj.Code);

                        goto exit; //return;
                    }
                }

                exit:
                #endregion

                #region ' Optimization data structure '

                if (lvl >= maxLevel)
                {
                    for (int i = 1; i < lvl; ++i)
                    {
                        var par = (MultyNode*)(mem + stack[i - 1]);
                        var itm = (Flugs*)(mem + stack[i]);

                        int* par_ptr;

                        if (par->Left == stack[i])
                        {
                            par_ptr = &par->Left;
                        }
                        else if (par->Right == stack[i])
                        {
                            par_ptr = &par->Right;
                        }
                        else
                        {
                            ErrorReport(65);

                            return;
                        }

                        if (*itm == Flugs.Tree)
                        {
                            var cur = (MultyNode*)itm;
                            var l_count = 0;
                            var r_count = 0;

                            if (cur->Left < 0)
                            {
                                l_count = 1;
                            }
                            else if (cur->Left > 0)
                            {
                                var f = (Flugs*)(mem + cur->Left);

                                if (*f == Flugs.Tree)
                                {
                                    l_count = ((MultyNode*)f)->Count;
                                }
                                else
                                {
                                    l_count = 1;
                                }
                            }

                            if (cur->Right < 0)
                            {
                                r_count = 1;
                            }
                            else if (cur->Right > 0)
                            {
                                var f = (Flugs*)(mem + cur->Right);

                                if (*f == Flugs.Tree)
                                {
                                    r_count = ((MultyNode*)f)->Count;
                                }
                                else
                                {
                                    r_count = 1;
                                }
                            }

                            var l_lvl = GetLevel(l_count);
                            var r_lvl = GetLevel(r_count);

                            cmp = l_lvl - r_lvl;

                            // To right
                            if (cmp > 1)
                            {
                                var l_flug = (Flugs*)(mem + cur->Left);

                                if (*l_flug != Flugs.Tree)
                                {
                                    continue;
                                }

                                var l_node = (MultyNode*)l_flug;
                                var lr_count = 0;

                                if (l_node->Right > 0)
                                {
                                    lr_count = ((MultyNode*)(mem + l_node->Right))->Count;
                                }
                                else if (l_node->Right < 0)
                                {
                                    lr_count = 1;
                                }

                                *par_ptr = cur->Left;

                                cur->Left = l_node->Right;
                                l_node->Right = stack[i];

                                l_node->Count = cur->Count;
                                cur->Count = cur->Count - l_count + lr_count;

                                i += 2;
                            }
                            // to left
                            else if (cmp < -1)
                            {
                                var r_flug = (Flugs*)(mem + cur->Right);

                                if (*r_flug != Flugs.Tree)
                                {
                                    continue;
                                }

                                var r_node = (MultyNode*)r_flug;
                                var rl_count = 0;

                                if (r_node->Left > 0)
                                {
                                    rl_count = ((MultyNode*)(mem + r_node->Left))->Count;
                                }
                                else if (r_node->Left < 0)
                                {
                                    rl_count = 1;
                                }

                                *par_ptr = cur->Right;

                                cur->Right = r_node->Left;
                                r_node->Left = stack[i];

                                r_node->Count = cur->Count;
                                cur->Count = cur->Count - r_count + rl_count;

                                i += 2;
                            }
                        }
                    }
                }

                #endregion
            }
            finally
            {
                locker?.Unlock();
            }
        }

        // Receiving a code group. Actually it turns out the code of any member of this group characteristic.
        public int GetFreeGroupTree<T>(int memoryKey, int grp, ValueLockRW locker) where T : IComparable<T>
        {
            try
            {
                if (locker != null)
                {
                    locker.ReadLock();
                }

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    return 0;
                }

                Flugs* flug;
                pos = *ptr;

                #endregion

                #region ' Find '

                while (true)
                {
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                        case Flugs.Tree:
                        {
                            var cur = (MultyNode*)(mem + pos);

                            cmp = grp.CompareTo(cur->Data);

                            // already exist
                            if (cmp == 0)
                            {
                                return pos + 9;
                            }
                            // left
                            else if (cmp < 0)
                            {
                                if (cur->Left > 0)
                                {
                                    pos = cur->Left;

                                    continue;
                                }

                                return 0;
                            }
                            // right
                            else
                            {
                                if (cur->Right > 0)
                                {
                                    pos = cur->Right;

                                    continue;
                                }

                                return 0;
                            }
                        }
                        case Flugs.Data:
                        {
                            break;
                        }
                    }

                }

                #endregion

            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Delete element from group
        public void DeleteFromFreeGroup<G, T>(int memoryKey, int grp, T obj, IRecordsComparer<T> t_comparer, ValueLockRW locker) where T : Record
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    return;
                }

                var action = ChangeAction.None;
                var stack = stackalloc int[100];
                var lvl = 0;
                var ext_data = 0;
                Flugs* flug;

                var cur_data = 0;

                pos = *ptr;

                #endregion

                #region ' Find '

                while (true)
                {
                    stack[lvl++] = pos;
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                        case Flugs.Tree:
                        {
                            var cur = (MultyNode*)flug;

                            cmp = grp.CompareTo(cur->Data);

                            // already exist
                            if (cmp == 0)
                            {
                                Delete(obj, pos + 9, t_comparer, null);

                                if (cur->Tree == 0)
                                {
                                    action = ChangeAction.RemoveTreeNode;

                                    goto remove;
                                }

                                return;
                            }
                            // left
                            else if (cmp < 0)
                            {
                                if (cur->Left > 0)
                                {
                                    pos = cur->Left;

                                    continue;
                                }

                                return;
                            }
                            // right
                            else
                            {
                                if (cur->Right > 0)
                                {
                                    pos = cur->Right;

                                    continue;
                                }

                                return;
                            }
                        }
                    }
                }

                remove:

                #endregion

                #region ' Correct count '

                for (int i = 0; i < lvl; ++i)
                {
                    flug = (Flugs*)(mem + stack[i]);

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                            {
                                var cur = (MultyNode*)flug;

                                cur->Count--;

                                break;
                            }

                        #endregion

                        #region ' Data '

                        case Flugs.Data:
                            {
                                var cur = (DataNode*)flug;

                                cur->Count--;

                                break;
                            }

                        #endregion

                        #region ' Other '

                        default:
                            {
                                return;
                            }

                        #endregion
                    }
                }

                #endregion

                #region ' Remove '

                int* par_ptr;
                //var isLeft = false;
                var par = (MultyNode*)(mem + stack[lvl - 2]);

                var cur_pos = stack[lvl - 1];

                // cur node is root
                if (lvl == 1)
                {
                    par_ptr = (int*)(mem + memoryKey);
                }
                else
                {
                    if (par->Right == cur_pos)
                    {
                        par_ptr = &par->Right;
                    }
                    // cur node is left
                    else if (par->Left == cur_pos)
                    {
                        par_ptr = &par->Left;
                    }
                    // bug
                    else
                    {
                        ErrorReport(90);

                        return;
                    }
                }

                switch (action)
                {
                    case ChangeAction.RemoveTreeNode:
                    {
                        var cur = (MultyNode*)(mem + cur_pos);

                        if (cur->Left == 0 && cur->Right == 0)
                        {
                            *par_ptr = 0;

                            ReleaseMemory(cur_pos, sizeof(MultyNode));
                        }
                        else if (cur->Left == 0)
                        {
                            *par_ptr = cur->Right;

                            ReleaseMemory(cur_pos, sizeof(MultyNode));
                        }
                        else if (cur->Right == 0)
                        {
                            *par_ptr = cur->Left;

                            ReleaseMemory(cur_pos, sizeof(MultyNode));
                        }
                        else
                        {
                            par_ptr = &cur->Right;

                            var tmp_pos = cur->Right;
                            var tmp = (MultyNode*)(mem + tmp_pos);

                            while (tmp->Left > 0)
                            {
                                tmp->Count--;

                                par_ptr = &tmp->Left;
                                tmp_pos = tmp->Left;
                                tmp = (MultyNode*)(mem + tmp_pos);
                            }

                            tmp->Count--;

                            if (tmp->Left < 0)
                            {
                                cur->Data = tmp->Left;
                                tmp->Left = 0;

                                return;
                            }

                            cur->Data = tmp->Data; // todo необходимо применить политику не изменения адреса

                            ReleaseMemory(tmp_pos, sizeof(MultyNode));
                            *par_ptr = tmp->Right;
                        }

                        break;
                    }
                }

                #endregion
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        #endregion

        #region ' Multy '

        // Inserting new element to multy tree
        public void InsertToMulty<G, T>(int memoryKey, G grp, T obj, IGroupComparer<G> g_comparer, IRecordsComparer<T>[] r_comparers, ValueLockRW locker) where G : IComparable<G> where T : Record
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }

                MakeSpace();

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    *ptr = -obj.Code;

                    return;
                }
                else if (*ptr < 0)
                {
                    cmp = g_comparer.CompareGroups(grp, -*ptr);

                    if (cmp < 0)
                    {
                        *ptr = CreateGroupNode(*ptr, -obj.Code, 0, 2);
                    }
                    else if (cmp > 0)
                    {
                        *ptr = CreateGroupNode(-obj.Code, *ptr, 0, 2);
                    }
                    else
                    {
                        cmp = r_comparers[0].CompareRecords(obj, -*ptr);

                        //if (cmp > 0)
                        //{
                        //    var tmp = CreateTreeNode(obj.Code, *ptr, 0, 2);

                        //    *ptr = CreateGroupNode(tmp, 0, 0, 1);
                        //}
                        //else if (cmp < 0)
                        //{
                        //    var tmp = CreateTreeNode(-*ptr, -obj.Code, 0, 2);

                        //    *ptr = CreateGroupNode(tmp, 0, 0, 1);
                        //}

                        if (cmp != 0)
                        {
                            // insert first
                            var tmp = AllocMemory(r_comparers.Length * 4);
                            var ptr_tmp = (int*)(mem + tmp);

                            for (int i = 0; i < r_comparers.Length; ++i)
                            {
                                cmp = r_comparers[i].CompareRecords(obj, -*ptr);

                                if (cmp > 0)
                                {
                                    ptr_tmp[i] = CreateTreeNode(obj.Code, *ptr, 0, 2);
                                }
                                else
                                {
                                    ptr_tmp[i] = CreateTreeNode(-*ptr, -obj.Code, 0, 2);
                                }
                            }

                            *ptr = CreateGroupNode(tmp, 0, 0, 1);
                        }
                    }

                    return;
                }

                var action = ChangeAction.None;
                var stack = stackalloc int[100];
                var lvl = 0;
                var ext_data = 0;
                Flugs* flug;

                var cur_data = 0;

                pos = *ptr;

                #endregion

                #region ' Find place to insert '

                while (true)
                {
                    stack[lvl++] = pos;
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (GroupNode*)flug;

                            cur_data = -cur->Data;

                            if (cur->Data > 0)
                            {
                                var ptr_tmp = (int*)(mem + cur->Data);

                                // sub trees each has only one element
                                if (*ptr_tmp > 0)
                                {
                                    var tmp_tree_flug = (Flugs*)(mem + *ptr_tmp);

                                    switch (*tmp_tree_flug)
                                    {
                                        case Flugs.Tree:
                                        {
                                            var tmp_tree = (TreeNode*)tmp_tree_flug;

                                            cur_data = tmp_tree->Data;

                                            break;
                                        }
                                        case Flugs.Data:
                                        {
                                            var tmp_tree = (DataNode*)tmp_tree_flug;
                                            var tmp_tree_dat = (int*)(mem + tmp_tree->Data);

                                            cur_data = tmp_tree_dat[0];

                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    cur_data = -*ptr_tmp;
                                }

                                // TODO can be *ptr_tmp == 0 ?
                            }

                            cmp = g_comparer.CompareGroups(grp, cur_data);

                            // already exist
                            if (cmp == 0)
                            {
                                // Вложенные деревья еще не созданы
                                if (cur->Data < 0)
                                {
                                    cur->Data = AllocMemory(r_comparers.Length * 4);

                                    for (int i = 0; i < r_comparers.Length; ++i)
                                    {
                                        var ptr_tmp = (int*)(mem + cur->Data);

                                        ptr_tmp[i] = -cur_data;
                                    }
                                }

                                // Вложенные деревья уже созданы
                                for (int i = 0; i < r_comparers.Length; ++i)
                                {
                                    Insert(obj, cur->Data + i * 4, r_comparers[i], false, null);

                                    cur = (GroupNode*)(mem + pos);
                                }

                                return;
                            }
                            // left
                            else if (cmp < 0)
                            {
                                if (cur->Left == 0)
                                {
                                    action = ChangeAction.InsertTreeNodeLeftLast;

                                    goto insert;
                                }
                                
                                if (cur->Left < 0)
                                {
                                    cmp = g_comparer.CompareGroups(grp, -cur->Left);

                                    if (cmp == 0)
                                    {
                                        cmp = r_comparers[0].CompareRecords(obj, -cur->Left);

                                        if (cmp != 0)
                                        {
                                            // insert first
                                            var tmp = AllocMemory(r_comparers.Length * 4);
                                            var ptr_tmp = (int*)(mem + tmp);

                                            for (int i = 0; i < r_comparers.Length; ++i)
                                            {
                                                ptr_tmp[i] = cur->Left;

                                                Insert(obj, tmp + i * 4, r_comparers[i], false, null);

                                                cur = (GroupNode*)(mem + pos);
                                                ptr_tmp = (int*)(mem + tmp);
                                            }

                                            cur->Left = CreateGroupNode(tmp, 0, 0, 1);
                                        }

                                        goto exit;
                                    }

                                    // Lock
                                    action = ChangeAction.InsertTreeNodeLeftTree;

                                    goto insert;
                                }

                                pos = cur->Left;

                                continue;
                            }
                            // right
                            else
                            {
                                if (cur->Right == 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertTreeNodeRightLast;

                                    goto insert;
                                }
                                
                                if (cur->Right < 0)
                                {
                                    cmp = g_comparer.CompareGroups(grp, -cur->Right);

                                    if (cmp == 0)
                                    {
                                        cmp = r_comparers[0].CompareRecords(obj, -cur->Right);

                                        if (cmp != 0)
                                        {
                                            // insert first
                                            var tmp = AllocMemory(r_comparers.Length * 4);
                                            var ptr_tmp = (int*)(mem + tmp);

                                            for (int i = 0; i < r_comparers.Length; ++i)
                                            {
                                                ptr_tmp[i] = cur->Right;

                                                Insert(obj, tmp + i * 4, r_comparers[i], false, null);

                                                cur = (GroupNode*)(mem + pos);
                                                ptr_tmp = (int*)(mem + tmp);
                                            }

                                            cur->Right = CreateGroupNode(tmp, 0, 0, 1);
                                        }

                                        goto exit;
                                    }

                                    // Lock
                                    action = ChangeAction.InsertTreeNodeRightTree;

                                    goto insert;
                                }
                                
                                pos = cur->Right;

                                continue;
                            }
                        }

                        #endregion

                        #region ' Data '

                        case Flugs.Data:
                            {
                                var cur = (DataNode*)flug;
                                var dat = (int*)(mem + cur->Data);

                                int first = 0;
                                int last = cur->Count - 1;

                                // position is before 0
                                if (r_comparers[0].CompareRecords(obj, dat[first]) < 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertDataNodeFirst;

                                    goto insert;
                                }
                                // position is after last
                                else if (r_comparers[0].CompareRecords(obj, dat[last]) > 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertDataNodeLast;

                                    goto insert;
                                }
                                // position middle
                                else
                                {
                                    // search for position
                                    while (first < last)
                                    {
                                        int mid = (first + last) >> 1;

                                        if (r_comparers[0].CompareRecords(obj, dat[mid]) <= 0)
                                        {
                                            last = mid;
                                        }
                                        else
                                        {
                                            first = mid + 1;
                                        }
                                    }

                                    // already exist
                                    if (r_comparers[0].CompareRecords(obj, dat[last]) == 0)
                                    {
                                        return;
                                    }

                                    // Insertion index found. last - desired position
                                    ext_data = last;

                                    // Lock
                                    action = ChangeAction.InsertDataNodeMiddle;

                                    goto insert;
                                }
                            }

                        #endregion

                        #region ' Other '

                        default:
                            {
                                Debugger.Break();

                                break;
                            }

                        #endregion
                    }
                }

            insert: ;

                #endregion

                #region ' Correct count '

                for (int i = 0; i < lvl; ++i)
                {
                    flug = (Flugs*)(mem + stack[i]);

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                            {
                                var cur = (GroupNode*)flug;

                                cur->Count++;

                                break;
                            }

                        #endregion

                        #region ' Data '

                        case Flugs.Data:
                            {
                                continue;
                            }

                        #endregion

                        #region ' Other '

                        default:
                            {
                                return;
                            }

                        #endregion
                    }
                }

                #endregion

                #region ' Insert '

                switch (action)
                {
                    case ChangeAction.InsertTreeNodeLeftLast:
                    {
                        var cur = (GroupNode*)flug;

                        cur->Left = -obj.Code;

                        goto exit; //return;
                    }
                    case ChangeAction.InsertTreeNodeLeftTree:
                    {
                        var cur = (GroupNode*)flug;

                        // micro rotate
                        if (cur->Right == 0)
                        {
                            if (cur->Data < 0)
                            {
                                cur->Right = cur->Data;
                            }
                            else
                            {
                                cur->Right = CreateGroupNode(cur->Data, 0, 0, 1);
                            }

                            if (cmp < 0)
                            {
                                cur->Data = cur->Left;
                                cur->Left = -obj.Code;
                            }
                            else if (cmp > 0)
                            {
                                cur->Data = -obj.Code;
                            }
                        }
                        else
                        {
                            if (cmp < 0)
                            {
                                cur->Left = CreateGroupNode(cur->Left, -obj.Code, 0, 2);
                            }
                            else
                            {
                                cur->Left = CreateGroupNode(cur->Left, 0, -obj.Code, 2);
                            }
                        }

                        goto exit;
                    }
                    case ChangeAction.InsertTreeNodeRightLast:
                    {
                        var cur = (GroupNode*)flug;
                        cur->Right = -obj.Code;

                        goto exit; 
                    }
                    case ChangeAction.InsertTreeNodeRightTree:
                    {
                        var cur = (GroupNode*)flug;

                        // micro rotate
                        if (cur->Left == 0)
                        {
                            if (cur->Data < 0)
                            {
                                cur->Left = cur->Data;
                            }
                            else
                            {
                                cur->Left = CreateGroupNode(cur->Data, 0, 0, 1);
                            }

                            if (cmp < 0)
                            {
                                cur->Data = -obj.Code;
                            }
                            else if (cmp > 0)
                            {
                                cur->Data = cur->Right;
                                cur->Right = -obj.Code;
                            }
                        }
                        else
                        {
                            if (cmp < 0)
                            {
                                cur->Right = CreateGroupNode(cur->Right, -obj.Code, 0, 2);
                            }
                            else
                            {
                                cur->Right = CreateGroupNode(cur->Right, 0, -obj.Code, 2);
                            }
                        }

                        goto exit;
                    }
                    case ChangeAction.InsertDataNodeFirst:
                    {
                        var cur = (DataNode*)flug;
                        var par = (LinkNode*)(mem + stack[lvl - 2]); // correct link
                        int* par_ptr;

                        // get parent link
                        if (par->Left == pos)
                        {
                            par_ptr = &par->Left;
                        }
                        else if (par->Right == pos)
                        {
                            par_ptr = &par->Right;
                        }
                        else
                        {
                            ErrorReport(90);

                            return;
                        }

                        var adr = CreateTreeNode(obj.Code, 0, *par_ptr, cur->Count + 1);

                        *par_ptr = adr;

                        goto exit;
                    }
                    case ChangeAction.InsertDataNodeLast:
                    {
                        var cur = (DataNode*)flug;
                        var par = (LinkNode*)(mem + stack[lvl - 2]);
                        int* par_ptr;

                        // get parent link
                        if (par->Left == pos)
                        {
                            par_ptr = &par->Left;
                        }
                        else if (par->Right == pos)
                        {
                            par_ptr = &par->Right;
                        }
                        else
                        {
                            ErrorReport(90);

                            return;
                        }

                        var adr = CreateTreeNode(obj.Code, *par_ptr, 0, cur->Count + 1);

                        *par_ptr = adr;

                        goto exit;
                    }
                    case ChangeAction.InsertDataNodeMiddle:
                    {
                        var cur = (DataNode*)flug;
                        var dat = (int*)(mem + cur->Data);
                        var par = (LinkNode*)(mem + stack[lvl - 2]);
                        int* par_ptr;

                        // get parent link
                        if (par->Left == pos)
                        {
                            par_ptr = &par->Left;
                        }
                        else if (par->Right == pos)
                        {
                            par_ptr = &par->Right;
                        }
                        else
                        {
                            ErrorReport(90);

                            return;
                        }

                        var last = ext_data;

                        // create right data node
                        var right_pos = CreateDataNode();
                        var right_node = (DataNode*)(mem + right_pos);

                        right_node->Count = cur->Count - last;
                        right_node->Min = dat[last];
                        right_node->Max = cur->Max;
                        right_node->Data = cur->Data + last * sizeof(int);

                        var c = cur->Count;

                        // correct cur data node
                        cur->Count = last;
                        cur->Max = dat[last - 1];

                        // create tree node
                        *par_ptr = CreateTreeNode(obj.Code, pos, right_pos, c + 1);

                        goto exit;
                    }
                    default:
                    {
                        return;
                    }
                }

                exit:
                #endregion

                #region ' Optimization data structure '

                if (lvl >= maxLevel)
                {
                    //if (lvl >= maxLevel * 2)
                    //{
                    //    SmartOptimization(stack[9], stack[10]);
                    //}
                    //else
                    //{
                    //var c = 0;

                    //for (int i = lvl - 1; i > 0; i--)

                    for (int i = 1; i < lvl; ++i)
                    {
                        var par = (GroupNode*)(mem + stack[i - 1]);
                        var itm = (Flugs*)(mem + stack[i]);

                        int* par_ptr;

                        if (par->Left == stack[i])
                        {
                            par_ptr = &par->Left;
                        }
                        else if (par->Right == stack[i])
                        {
                            par_ptr = &par->Right;
                        }
                        else
                        {
                            ErrorReport(65);

                            return;
                        }

                        if (*itm == Flugs.Tree)
                        {
                            var cur = (GroupNode*)itm;
                            var l_count = 0;
                            var r_count = 0;

                            if (cur->Left < 0)
                            {
                                l_count = 1;
                            }
                            else if (cur->Left > 0)
                            {
                                var f = (Flugs*)(mem + cur->Left);

                                if (*f == Flugs.Tree)
                                {
                                    l_count = ((GroupNode*)f)->Count;
                                }
                                else
                                {
                                    l_count = 1;
                                }
                            }

                            if (cur->Right < 0)
                            {
                                r_count = 1;
                            }
                            else if (cur->Right > 0)
                            {
                                var f = (Flugs*)(mem + cur->Right);

                                if (*f == Flugs.Tree)
                                {
                                    r_count = ((GroupNode*)f)->Count;
                                }
                                else
                                {
                                    r_count = 1;
                                }
                            }

                            var l_lvl = GetLevel(l_count);
                            var r_lvl = GetLevel(r_count);

                            cmp = l_lvl - r_lvl;

                            // To right
                            if (cmp > 1)
                            {
                                var l_flug = (Flugs*)(mem + cur->Left);

                                if (*l_flug != Flugs.Tree)
                                {
                                    continue;
                                }

                                var l_node = (GroupNode*)l_flug;
                                var lr_count = 0;

                                if (l_node->Right > 0)
                                {
                                    lr_count = ((GroupNode*)(mem + l_node->Right))->Count;
                                }
                                else if (l_node->Right < 0)
                                {
                                    lr_count = 1;
                                }

                                *par_ptr = cur->Left;

                                cur->Left = l_node->Right;
                                l_node->Right = stack[i];

                                l_node->Count = cur->Count;
                                cur->Count = cur->Count - l_count + lr_count;

                                i += 2;
                            }
                            // to left
                            else if (cmp < -1)
                            {
                                var r_flug = (Flugs*)(mem + cur->Right);

                                if (*r_flug != Flugs.Tree)
                                {
                                    continue;
                                }

                                var r_node = (GroupNode*)r_flug;
                                var rl_count = 0;

                                if (r_node->Left > 0)
                                {
                                    rl_count = ((GroupNode*)(mem + r_node->Left))->Count;
                                }
                                else if (r_node->Left < 0)
                                {
                                    rl_count = 1;
                                }

                                *par_ptr = cur->Right;

                                cur->Right = r_node->Left;
                                r_node->Left = stack[i];

                                r_node->Count = cur->Count;
                                cur->Count = cur->Count - r_count + rl_count;

                                i += 2;
                            }
                        }
                    }
                }

                #endregion
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Geting values tree from multy tree
        public int GetMultyTree<T>(T grp, int memoryKey, IGroupComparer<T> g_comparer, int multyIndex, ValueLockRW locker) where T : IComparable<T>
        {
            try
            {
                if (locker != null)
                {
                    locker.ReadLock();
                }

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    return 0;
                }
                else if (*ptr < 0)
                {
                    cmp = g_comparer.CompareGroups(grp, -*ptr);

                    if (cmp == 0)
                    {
                        return pos;
                    }

                    return 0;
                }

                Flugs* flug;
                pos = *ptr;

                #endregion

                #region ' Find '

                while (true)
                {
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                        case Flugs.Tree:
                        {
                            var cur = (GroupNode*)(mem + pos);
                            var cur_data = -cur->Data;

                            if (cur->Data > 0)
                            {
                                var ptr_tmp = (int*)(mem + cur->Data);

                                if (*ptr_tmp > 0)
                                {
                                    var tmp_tree_flug = (Flugs*)(mem + *ptr_tmp);

                                    switch (*tmp_tree_flug)
                                    {
                                        case Flugs.Tree:
                                        {
                                            var tmp_tree = (TreeNode*)tmp_tree_flug;

                                            cur_data = tmp_tree->Data;

                                            break;
                                        }
                                        case Flugs.Data:
                                        {
                                            var tmp_tree = (DataNode*)tmp_tree_flug;
                                            var tmp_tree_dat = (int*)(mem + tmp_tree->Data);

                                            cur_data = tmp_tree_dat[0];

                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    cur_data = -*ptr_tmp;
                                }
                            }

                            cmp = g_comparer.CompareGroups(grp, cur_data);

                            // already exist
                            if (cmp == 0)
                            {
                                if (cur->Data < 0)
                                {
                                    return pos + 1;
                                }
                                else
                                {
                                    return cur->Data + multyIndex * 4;
                                }
                            }
                            // left
                            else if (cmp < 0)
                            {
                                if (cur->Left > 0)
                                {
                                    pos = cur->Left;

                                    continue;
                                }
                                
                                if (cur->Left < 0)
                                {
                                    cmp = g_comparer.CompareGroups(grp, -cur->Left);

                                    if (cmp == 0)
                                    {
                                        var off = (byte*)&cur->Left - (byte*)cur;

                                        pos += (int)off;

                                        return pos;
                                    }
                                }

                                return 0;
                            }
                            // right
                            else
                            {
                                if (cur->Right > 0)
                                {
                                    pos = cur->Right;

                                    continue;
                                }
                                
                                if (cur->Right < 0)
                                {
                                    cmp = g_comparer.CompareGroups(grp, -cur->Right);

                                    if (cmp == 0)
                                    {
                                        var off = (byte*)&cur->Right - (byte*)cur;

                                        pos += (int)off;

                                        return pos;
                                    }
                                }

                                return 0;
                            }
                        }
                        case Flugs.Data:
                        {

                            break;
                        }
                    }

                }

                #endregion

            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Delete element from multy tree
        public void DeleteFromMulty<G, T>(int memoryKey, G grp, T obj, IGroupComparer<G> g_comparer, IRecordsComparer<T>[] r_comparers, ValueLockRW locker) where G : IComparable<G> where T : Record
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    return;
                }
                else if (*ptr < 0)
                {
                    cmp = g_comparer.CompareGroups(grp, -*ptr);

                    if (cmp == 0 && r_comparers[0].CompareRecords(obj, -*ptr) == 0)
                    {
                        *ptr = 0;
                    }

                    return;
                }

                var action = ChangeAction.None;
                var stack = stackalloc int[100];
                var lvl = 0;
                var ext_data = 0;
                Flugs* flug;

                var cur_data = 0;

                pos = *ptr;

                #endregion

                #region ' Find '

                while (true)
                {
                    stack[lvl++] = pos;
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                        case Flugs.Tree:
                        {
                            var cur = (GroupNode*)flug;

                            cur_data = -cur->Data;

                            if (cur->Data > 0)
                            {
                                var ptr_tmp = (int*)(mem + cur->Data);

                                // sub trees each has only one element
                                if (*ptr_tmp > 0)
                                {
                                    var tmp_tree_flug = (Flugs*)(mem + *ptr_tmp);

                                    switch (*tmp_tree_flug)
                                    {
                                        case Flugs.Tree:
                                        {
                                            var tmp_tree = (TreeNode*)tmp_tree_flug;

                                            cur_data = tmp_tree->Data;

                                            break;
                                        }
                                        case Flugs.Data:
                                        {
                                            var tmp_tree = (DataNode*)tmp_tree_flug;
                                            var tmp_tree_dat = (int*)(mem + tmp_tree->Data);

                                            cur_data = tmp_tree_dat[0];

                                            pos = 1;
                                                // todo найти позицию элемента в массиве. если остается 0 элементов удалить массив и все остальные действия связанные с массивами...

                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    cur_data = -*ptr_tmp;
                                }
                            }

                            cmp = g_comparer.CompareGroups(grp, cur_data);

                            // already exist
                            if (cmp == 0)
                            {
                                if (cur->Data < 0)
                                {
                                    cur->Data = 0;

                                    action = ChangeAction.RemoveTreeNode;

                                    goto remove;
                                }
                                else
                                {
                                    for (int i = 0; i < r_comparers.Length; ++i)
                                    {
                                        Delete(obj, cur->Data + i * 4, r_comparers[i], null);
                                    }

                                    var ptr_tmp = *(int*)(mem + cur->Data);

                                    if (ptr_tmp < 0)
                                    {
                                        ReleaseMemory(cur->Data, r_comparers.Length * 4);

                                        cur->Data = ptr_tmp;
                                    }
                                    else if (ptr_tmp == 0)
                                    {
                                        ReleaseMemory(cur->Data, r_comparers.Length * 4);

                                        cur->Data = 0;

                                        action = ChangeAction.RemoveTreeNode;

                                        goto remove;
                                    }
                                }
                                
                                return;
                            }
                            // left
                            else if (cmp < 0)
                            {
                                if (cur->Left > 0)
                                {
                                    pos = cur->Left;

                                    continue;
                                }
                                
                                if (cur->Left < 0)
                                {
                                    cmp = g_comparer.CompareGroups(grp, -cur->Left);

                                    if (cmp == 0 && r_comparers[0].CompareRecords(obj, -cur->Left) == 0)
                                    {
                                        cur->Left = 0;
                                        
                                        goto remove;
                                    }
                                }

                                return;
                            }
                            // right
                            else
                            {
                                if (cur->Right > 0)
                                {
                                    pos = cur->Right;

                                    continue;
                                }
                                
                                if (cur->Right < 0)
                                {
                                    cmp = g_comparer.CompareGroups(grp, -cur->Right);

                                    if (cmp == 0 && r_comparers[0].CompareRecords(obj, -cur->Right) == 0)
                                    {
                                        cur->Right = 0;
                                        
                                        goto remove;
                                    }
                                }

                                return;
                            }
                        }
                    }
                }

                remove:

                #endregion

                #region ' Correct count '

                for (int i = 0; i < lvl; ++i)
                {
                    flug = (Flugs*)(mem + stack[i]);

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                            {
                                var cur = (GroupNode*)flug;

                                cur->Count--;

                                break;
                            }

                        #endregion

                        #region ' Data '

                        case Flugs.Data:
                            {
                                var cur = (DataNode*)flug;

                                cur->Count--;

                                break;
                            }

                        #endregion

                        #region ' Other '

                        default:
                            {
                                return;
                            }

                        #endregion
                    }
                }

                #endregion

                #region ' Remove '

                int* par_ptr;
                var par = (GroupNode*)(mem + stack[lvl - 2]);

                var cur_pos = stack[lvl - 1];

                // cur node is root
                if (lvl == 1)
                {
                    par_ptr = (int*)(mem + memoryKey);
                }
                else
                {
                    if (par->Right == cur_pos)
                    {
                        par_ptr = &par->Right;
                    }
                    // cur node is left
                    else if (par->Left == cur_pos)
                    {
                        par_ptr = &par->Left;
                    }
                    // bug
                    else
                    {
                        ErrorReport(90);

                        return;
                    }
                }

                switch (action)
                {
                    case ChangeAction.RemoveTreeNode:
                    {
                        var cur = (GroupNode*)(mem + cur_pos);

                        if (cur->Left == 0 && cur->Right == 0)
                        {
                            *par_ptr = 0;

                            ReleaseMemory(cur_pos, sizeof(GroupNode));
                        }
                        else if (cur->Left == 0)
                        {
                            *par_ptr = cur->Right;

                            ReleaseMemory(cur_pos, sizeof(GroupNode));
                        }
                        else if (cur->Right == 0)
                        {
                            *par_ptr = cur->Left;

                            ReleaseMemory(cur_pos, sizeof(GroupNode));
                        }
                        else if (cur->Left < 0)
                        {
                            cur->Data = cur->Left;
                            cur->Left = 0;
                        }
                        else if (cur->Right < 0)
                        {
                            cur->Data = cur->Right;
                            cur->Right = 0;
                        }
                        else
                        {
                            par_ptr = &cur->Right;

                            var tmp_pos = cur->Right;
                            var tmp = (GroupNode*)(mem + tmp_pos);

                            while (tmp->Left > 0)
                            {
                                tmp->Count--;

                                par_ptr = &tmp->Left;
                                tmp_pos = tmp->Left;
                                tmp = (GroupNode*)(mem + tmp_pos);
                            }

                            tmp->Count--;

                            if (tmp->Left < 0)
                            {
                                cur->Data = tmp->Left;
                                tmp->Left = 0;

                                return;
                            }

                            cur->Data = tmp->Data; // todo необходимо применить политику не изменения адреса

                            ReleaseMemory(tmp_pos, sizeof(GroupNode));
                            *par_ptr = tmp->Right;
                        }

                        break;
                    }
                }

                #endregion
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Очистить и освободить память
        public void ClearMulty(int memoryKey, int size, ValueLockRW locker)
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }

                var ptr = (int*)(mem + memoryKey);

                if (*ptr == 0)
                {
                    return;
                }
                else if (*ptr < 0)
                {
                    *ptr = 0;

                    return;
                }

                Flugs* flug;
                var lvl = 0;
                var pos = *ptr;
                var stack = stackalloc int[100];

                while (true)
                {
                    flug = (Flugs*)(mem + pos);
                    stack[lvl++] = pos;

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (GroupNode*)flug;

                            // goto left
                            if (cur->Left > 0)
                            {
                                pos = cur->Left;
                            }
                            // goto left
                            else if (cur->Right > 0)
                            {
                                pos = cur->Right;
                            }
                            // remove
                            else
                            {
                                // Вложенные деревья
                                if (cur->Data > 0)
                                {
                                    for (int i = 0; i < size; ++i)
                                    {
                                        ClearTree(cur->Data + i * 4, null);
                                    }
                                }

                                ReleaseMemory(pos, sizeof(GroupNode));

                                // root node
                                if (lvl == 1)
                                {
                                    *ptr = 0;

                                    return;
                                }

                                lvl -= 2;

                                var par = (GroupNode*)(mem + stack[lvl]);

                                if (par->Left == pos)
                                {
                                    par->Left = 0;
                                }
                                else
                                {
                                    par->Right = 0;
                                }
                            }

                            break;
                        }

                        #endregion

                        #region ' Other '

                        default:
                        {
                            ErrorReport(111);

                            return;
                        }

                        #endregion
                    }
                }
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        #endregion

        #region ' Free Multy '

        // Inserting new element to multy tree
        public void InsertToFreeMulty<TKey, T>(int memoryKey, int grp, T obj, IMapedComparer<TKey> maper, IRecordsComparer<T>[] r_comparers, ValueLockRW locker)
            where T : Record 
            where TKey : IComparable<TKey>
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }

                MakeSpace();

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    var tmp = AllocMemory(r_comparers.Length * 4);
                    var ptr_tmp = (int*)(mem + tmp);

                    for (int i = 0; i < r_comparers.Length; ++i)
                    {
                        ptr_tmp[i] = -obj.Code;
                    }

                    *ptr = CreateMultyNode(grp, 0, 0, 1, tmp);

                    return;
                }

                var action = ChangeAction.None;
                var stack = stackalloc int[100];
                var lvl = 0;
                var ext_data = 0;
                Flugs* flug;

                var cur_data = 0;

                pos = *ptr;

                #endregion

                #region ' Find place to insert '

                while (true)
                {
                    stack[lvl++] = pos;
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                       #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (MultyNode*)flug;

                            cmp = maper.Compare(grp, cur->Data);

                            // already exist
                            if (cmp == 0)
                            {
                                // insert to sub trees
                                for (int i = 0; i < r_comparers.Length; ++i)
                                {
                                    Insert(obj, cur->Tree + i * 4, r_comparers[i], false, null);

                                    // В процее добавления элемента в поддерево память может быть заполнится и выделится новая, и все ссылки становятся не актуальны
                                    cur = (MultyNode*)(mem + pos);
                                }

                                return;
                            }
                            // left
                            else if (cmp < 0)
                            {
                                if (cur->Left == 0)
                                {
                                    action = ChangeAction.InsertTreeNodeLeftLast;

                                    goto insert;
                                }

                                pos = cur->Left;

                                continue;
                            }
                            // right
                            else
                            {
                                if (cur->Right == 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertTreeNodeRightLast;

                                    goto insert;
                                }

                                pos = cur->Right;

                                continue;
                            }
                        }

                            #endregion

                       #region ' Data '

                        case Flugs.Data:
                        {
                            var cur = (DataNode*)flug;
                            var dat = (int*)(mem + cur->Data);

                            int first = 0;
                            int last = cur->Count - 1;

                            // position is before 0
                            if (r_comparers[0].CompareRecords(obj, dat[first]) < 0)
                            {
                                // Lock
                                action = ChangeAction.InsertDataNodeFirst;

                                goto insert;
                            }
                                // position is after last
                            else if (r_comparers[0].CompareRecords(obj, dat[last]) > 0)
                            {
                                // Lock
                                action = ChangeAction.InsertDataNodeLast;

                                goto insert;
                            }
                                // position middle
                            else
                            {
                                // search for position
                                while (first < last)
                                {
                                    int mid = (first + last) >> 1;

                                    if (r_comparers[0].CompareRecords(obj, dat[mid]) <= 0)
                                    {
                                        last = mid;
                                    }
                                    else
                                    {
                                        first = mid + 1;
                                    }
                                }

                                // already exist
                                if (r_comparers[0].CompareRecords(obj, dat[last]) == 0)
                                {
                                    return;
                                }

                                // Insertion index found. last - desired position
                                ext_data = last;

                                // Lock
                                action = ChangeAction.InsertDataNodeMiddle;

                                goto insert;
                            }
                        }

                            #endregion

                       #region ' Other '

                        default:
                        {
                            Debugger.Break();

                            break;
                        }

                            #endregion
                    }
                }

            insert: ;

                #endregion

                #region ' Correct count '

                for (int i = 0; i < lvl; ++i)
                {
                    flug = (Flugs*)(mem + stack[i]);

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                            {
                                var cur = (MultyNode*)flug;

                                cur->Count++;

                                break;
                            }

                        #endregion

                        #region ' Data '

                        case Flugs.Data:
                            {
                                continue;
                            }

                        #endregion

                        #region ' Other '

                        default:
                            {
                                return;
                            }

                        #endregion
                    }
                }

                #endregion

                #region ' Insert '

                switch (action)
                {
                    case ChangeAction.InsertTreeNodeLeftLast:
                    {
                        var cur = (MultyNode*)flug;
                        var tmp = AllocMemory(r_comparers.Length * 4);
                        var ptr_tmp = (int*)(mem + tmp);

                        for (int i = 0; i < r_comparers.Length; ++i)
                        {
                            ptr_tmp[i] = -obj.Code;
                        }

                        cur->Left = CreateMultyNode(grp, 0, 0, 1, tmp);

                        goto exit; 
                    }
                    case ChangeAction.InsertTreeNodeRightLast:
                    {
                        var cur = (MultyNode*)flug;
                        var tmp = AllocMemory(r_comparers.Length * 4);
                        var ptr_tmp = (int*)(mem + tmp);

                        for (int i = 0; i < r_comparers.Length; ++i)
                        {
                            ptr_tmp[i] = -obj.Code;
                        }

                        cur->Right = CreateMultyNode(grp, 0, 0, 1, tmp);

                        goto exit;
                    }
                    default:
                    {
                        return;
                    }
                }

                exit:
                #endregion

                #region ' Optimization data structure '

                if (lvl >= maxLevel)
                {
                    for (int i = 1; i < lvl; ++i)
                    {
                        var par = (MultyNode*)(mem + stack[i - 1]);
                        var itm = (Flugs*)(mem + stack[i]);

                        int* par_ptr;

                        if (par->Left == stack[i])
                        {
                            par_ptr = &par->Left;
                        }
                        else if (par->Right == stack[i])
                        {
                            par_ptr = &par->Right;
                        }
                        else
                        {
                            ErrorReport(65);

                            return;
                        }

                        if (*itm == Flugs.Tree)
                        {
                            var cur = (MultyNode*)itm;
                            var l_count = 0;
                            var r_count = 0;

                            if (cur->Left < 0)
                            {
                                l_count = 1;
                            }
                            else if (cur->Left > 0)
                            {
                                var f = (Flugs*)(mem + cur->Left);

                                if (*f == Flugs.Tree)
                                {
                                    l_count = ((MultyNode*)f)->Count;
                                }
                                else
                                {
                                    l_count = 1;
                                }
                            }

                            if (cur->Right < 0)
                            {
                                r_count = 1;
                            }
                            else if (cur->Right > 0)
                            {
                                var f = (Flugs*)(mem + cur->Right);

                                if (*f == Flugs.Tree)
                                {
                                    r_count = ((MultyNode*)f)->Count;
                                }
                                else
                                {
                                    r_count = 1;
                                }
                            }

                            var l_lvl = GetLevel(l_count);
                            var r_lvl = GetLevel(r_count);

                            cmp = l_lvl - r_lvl;

                            // To right
                            if (cmp > 1)
                            {
                                var l_flug = (Flugs*)(mem + cur->Left);

                                if (*l_flug != Flugs.Tree)
                                {
                                    continue;
                                }

                                var l_node = (MultyNode*)l_flug;
                                var lr_count = 0;

                                if (l_node->Right > 0)
                                {
                                    lr_count = ((MultyNode*)(mem + l_node->Right))->Count;
                                }
                                else if (l_node->Right < 0)
                                {
                                    lr_count = 1;
                                }

                                *par_ptr = cur->Left;

                                cur->Left = l_node->Right;
                                l_node->Right = stack[i];

                                l_node->Count = cur->Count;
                                cur->Count = cur->Count - l_count + lr_count;

                                i += 2;
                            }
                            // to left
                            else if (cmp < -1)
                            {
                                var r_flug = (Flugs*)(mem + cur->Right);

                                if (*r_flug != Flugs.Tree)
                                {
                                    continue;
                                }

                                var r_node = (MultyNode*)r_flug;
                                var rl_count = 0;

                                if (r_node->Left > 0)
                                {
                                    rl_count = ((MultyNode*)(mem + r_node->Left))->Count;
                                }
                                else if (r_node->Left < 0)
                                {
                                    rl_count = 1;
                                }

                                *par_ptr = cur->Right;

                                cur->Right = r_node->Left;
                                r_node->Left = stack[i];

                                r_node->Count = cur->Count;
                                cur->Count = cur->Count - r_count + rl_count;

                                i += 2;
                            }
                        }
                    }
                }

                #endregion
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Geting values tree from multy tree
        public int GetFreeMultyTree<TKey>(int memoryKey, TKey grp, IMapedComparer<TKey> maper, int multyIndex, ValueLockRW locker) 
            where TKey : IComparable<TKey>
        {
            try
            {
                if (locker != null)
                {
                    locker.ReadLock();
                }

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    return 0;
                }

                Flugs* flug;
                pos = *ptr;

                #endregion

                #region ' Find '

                while (true)
                {
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                        case Flugs.Tree:
                        {
                            var cur = (MultyNode*)(mem + pos);

                            cmp = maper.Compare(grp, cur->Data);

                            // already exist
                            if (cmp == 0)
                            {
                                return cur->Tree + multyIndex * 4;
                            }
                            // left
                            else if (cmp < 0)
                            {
                                if (cur->Left > 0)
                                {
                                    pos = cur->Left;

                                    continue;
                                }

                                return 0;
                            }
                            // right
                            else
                            {
                                if (cur->Right > 0)
                                {
                                    pos = cur->Right;

                                    continue;
                                }

                                return 0;
                            }
                        }
                        case Flugs.Data:
                        {
                            break;
                        }
                    }

                }

                #endregion

            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Delete element from multy tree
        public void DeleteFromFreeMulty<TKey, T>(int memoryKey, int grp, T obj, IMapedComparer<TKey> maper, IRecordsComparer<T>[] r_comparers, bool dontRemoveGroups, ValueLockRW locker)
            where T : Record 
            where TKey : IComparable<TKey>
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    return;
                }

                var action = ChangeAction.None;
                var stack = stackalloc int[100];
                var lvl = 0;
                var ext_data = 0;
                Flugs* flug;

                var cur_data = 0;

                pos = *ptr;

                #endregion

                #region ' Find '

                while (true)
                {
                    stack[lvl++] = pos;
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                        case Flugs.Tree:
                        {
                            var cur = (MultyNode*)flug;

                            cmp = maper.Compare(grp, cur->Data);

                            // already exist
                            if (cmp == 0)
                            {
                                for (int i = 0; i < r_comparers.Length; ++i)
                                {
                                    Delete(obj, cur->Tree + i * 4, r_comparers[i], null);
                                }

                                var ptr_tmp = *(int*)(mem + cur->Tree);

                                //if (ptr_tmp < 0)
                                //{
                                //    ReleaseMemory(cur->Tree, r_comparers.Length * 4);

                                //    cur->Data = ptr_tmp;
                                //}
                                //else 
                                if (ptr_tmp == 0 && !dontRemoveGroups)
                                {
                                    ReleaseMemory(cur->Tree, r_comparers.Length * 4);

                                    //cur->Data = 0;

                                    action = ChangeAction.RemoveTreeNode;

                                    goto remove;
                                }

                                return;
                            }
                            // left
                            else if (cmp < 0)
                            {
                                if (cur->Left > 0)
                                {
                                    pos = cur->Left;

                                    continue;
                                }

                                return;
                            }
                                // right
                            else
                            {
                                if (cur->Right > 0)
                                {
                                    pos = cur->Right;

                                    continue;
                                }

                                return;
                            }
                        }
                    }
                }

                remove:

                #endregion

                #region ' Correct count '

                for (int i = 0; i < lvl; ++i)
                {
                    flug = (Flugs*)(mem + stack[i]);

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                            {
                                var cur = (MultyNode*)flug;

                                cur->Count--;

                                break;
                            }

                        #endregion

                        #region ' Data '

                        case Flugs.Data:
                            {
                                var cur = (DataNode*)flug;

                                cur->Count--;

                                break;
                            }

                        #endregion

                        #region ' Other '

                        default:
                            {
                                return;
                            }

                        #endregion
                    }
                }

                #endregion

                #region ' Remove '

                int* par_ptr;
                var par = (MultyNode*)(mem + stack[lvl - 2]);

                var cur_pos = stack[lvl - 1];

                // cur node is root
                if (lvl == 1)
                {
                    par_ptr = (int*)(mem + memoryKey);
                }
                else
                {
                    if (par->Right == cur_pos)
                    {
                        par_ptr = &par->Right;
                    }
                    // cur node is left
                    else if (par->Left == cur_pos)
                    {
                        par_ptr = &par->Left;
                    }
                    // bug
                    else
                    {
                        ErrorReport(90);

                        return;
                    }
                }

                switch (action)
                {
                    case ChangeAction.RemoveTreeNode:
                        {
                            var cur = (MultyNode*)(mem + cur_pos);

                            if (cur->Left == 0 && cur->Right == 0)
                            {
                                *par_ptr = 0;

                                ReleaseMemory(cur_pos, sizeof(MultyNode));
                            }
                            else if (cur->Left == 0)
                            {
                                *par_ptr = cur->Right;

                                ReleaseMemory(cur_pos, sizeof(MultyNode));
                            }
                            else if (cur->Right == 0)
                            {
                                *par_ptr = cur->Left;

                                ReleaseMemory(cur_pos, sizeof(MultyNode));
                            }
                            else
                            {
                                par_ptr = &cur->Right;

                                var tmp_pos = cur->Right;
                                var tmp = (MultyNode*)(mem + tmp_pos);

                                while (tmp->Left > 0)
                                {
                                    tmp->Count--;

                                    par_ptr = &tmp->Left;
                                    tmp_pos = tmp->Left;
                                    tmp = (MultyNode*)(mem + tmp_pos);
                                }

                                tmp->Count--;

                                cur->Data = tmp->Data; // todo необходимо применить политику не изменения адреса
                                cur->Tree = tmp->Tree;

                                ReleaseMemory(tmp_pos, sizeof(MultyNode));
                                *par_ptr = tmp->Right;
                            }

                            break;
                        }
                }

                #endregion
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }
        
        // Find multy group code by key. 
        public int GetFreeMultyCode<TKey>(int memoryKey, TKey grp, IMapedComparer<TKey> maper, ValueLockRW locker)
            where TKey : IComparable<TKey>
        {
            try
            {
                if (locker != null)
                {
                    locker.ReadLock();
                }

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*) (mem + pos);

                if (*ptr == 0)
                {
                    return 0;
                }

                Flugs* flug;
                pos = *ptr;

                #endregion

                #region ' Find place to insert '

                while (true)
                {
                    flug = (Flugs*) (mem + pos);

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (MultyNode*) flug;

                            cmp = maper.Compare(grp, cur->Data);

                            // already exist
                            if (cmp == 0)
                            {
                                return cur->Data;
                            }
                            // left
                            else if (cmp < 0)
                            {
                                if (cur->Left == 0)
                                {
                                    goto exit;
                                }

                                pos = cur->Left;

                                continue;
                            }
                            // right
                            else
                            {
                                if (cur->Right == 0)
                                {
                                    goto exit;
                                }

                                pos = cur->Right;

                                continue;
                            }
                        }

                        #endregion

                        #region ' Data '

                        case Flugs.Data:
                        {
                            break;
                        }

                        #endregion

                        #region ' Other '

                        default:
                        {
                            Debugger.Break();

                            break;
                        }

                        #endregion
                    }
                }

                exit:

                #endregion

                return 0;
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Очистить и освободить память
        public void ClearFreeMulty(int memoryKey, int size, ValueLockRW locker)
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }

                var ptr = (int*)(mem + memoryKey);

                if (*ptr == 0)
                {
                    return;
                }
                else if (*ptr < 0)
                {
                    *ptr = 0;

                    return;
                }

                Flugs* flug;
                var lvl = 0;
                var pos = *ptr;
                var stack = stackalloc int[100];

                while (true)
                {
                    flug = (Flugs*)(mem + pos);
                    stack[lvl++] = pos;

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (MultyNode*)flug;

                            // goto left
                            if (cur->Left > 0)
                            {
                                pos = cur->Left;
                            }
                            // goto left
                            else if (cur->Right > 0)
                            {
                                pos = cur->Right;
                            }
                            // remove
                            else
                            {
                                // Вложенные деревья
                                for (int i = 0; i < size; ++i)
                                {
                                    ClearTree(cur->Tree + i * 4, null);
                                }

                                ReleaseMemory(pos, sizeof(MultyNode));

                                // root node
                                if (lvl == 1)
                                {
                                    *ptr = 0;

                                    return;
                                }

                                lvl -= 2;

                                var par = (MultyNode*)(mem + stack[lvl]);

                                if (par->Left == pos)
                                {
                                    par->Left = 0;
                                }
                                else
                                {
                                    par->Right = 0;
                                }
                            }

                            break;
                        }

                        #endregion

                        #region ' Other '

                        default:
                        {
                            ErrorReport(111);

                            return;
                        }

                        #endregion
                    }
                }
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        #endregion

        #region ' InlineKey Multy '
        // Структура данных хранящая ключ, в дереве в виде массива байтов

        // Inserting new element to multy tree
        public void InlineKeyMulty_Insert<TKey, T>(int memoryKey, TKey key, T obj, IRecordsComparer<T>[] r_comparers, ValueLockRW locker)
            where T : Record 
            where TKey : IInlineKey
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }

                MakeSpace();

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);
                var grp = key.KeyData; 

                if (*ptr == 0)
                {
                    var tmp = AllocMemory(r_comparers.Length * 4);
                    var ptr_tmp = (int*)(mem + tmp);

                    for (int i = 0; i < r_comparers.Length; ++i)
                    {
                        ptr_tmp[i] = -obj.Code;
                    }

                    *ptr = CreateByteKeyMultyNode(grp, 0, 0, 1, tmp);

                    return;
                }

                var action = ChangeAction.None;
                var stack = stackalloc int[100];
                var lvl = 0;
                var ext_data = 0;
                Flugs* flug;

                pos = *ptr;

                #endregion

                #region ' Find place to insert '

                while (true)
                {
                    stack[lvl++] = pos;
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                       #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (MultyNode*)flug;

                            cmp = CompareByteKeys(grp, cur->Data);

                            // already exist
                            if (cmp == 0)
                            {
                                // insert to sub trees
                                for (int i = 0; i < r_comparers.Length; ++i)
                                {
                                    Insert(obj, cur->Tree + i * 4, r_comparers[i], false, null);
                                    cur = (MultyNode*)(mem + pos);
                                }

                                return;
                            }
                            // left
                            else if (cmp < 0)
                            {
                                if (cur->Left == 0)
                                {
                                    action = ChangeAction.InsertTreeNodeLeftLast;

                                    goto insert;
                                }

                                pos = cur->Left;

                                continue;
                            }
                            // right
                            else
                            {
                                if (cur->Right == 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertTreeNodeRightLast;

                                    goto insert;
                                }

                                pos = cur->Right;

                                continue;
                            }
                        }

                            #endregion

                       #region ' Data '

                        case Flugs.Data:
                        {
                            var cur = (DataNode*)flug;
                            var dat = (int*)(mem + cur->Data);

                            int first = 0;
                            int last = cur->Count - 1;

                            // position is before 0
                            if (r_comparers[0].CompareRecords(obj, dat[first]) < 0)
                            {
                                // Lock
                                action = ChangeAction.InsertDataNodeFirst;

                                goto insert;
                            }
                                // position is after last
                            else if (r_comparers[0].CompareRecords(obj, dat[last]) > 0)
                            {
                                // Lock
                                action = ChangeAction.InsertDataNodeLast;

                                goto insert;
                            }
                                // position middle
                            else
                            {
                                // search for position
                                while (first < last)
                                {
                                    int mid = (first + last) >> 1;

                                    if (r_comparers[0].CompareRecords(obj, dat[mid]) <= 0)
                                    {
                                        last = mid;
                                    }
                                    else
                                    {
                                        first = mid + 1;
                                    }
                                }

                                // already exist
                                if (r_comparers[0].CompareRecords(obj, dat[last]) == 0)
                                {
                                    return;
                                }

                                // Insertion index found. last - desired position
                                ext_data = last;

                                // Lock
                                action = ChangeAction.InsertDataNodeMiddle;

                                goto insert;
                            }
                        }

                            #endregion

                       #region ' Other '

                        default:
                        {
                            Debugger.Break();

                            break;
                        }

                            #endregion
                    }
                }

            insert: ;

                #endregion

                #region ' Correct count '

                for (int i = 0; i < lvl; ++i)
                {
                    flug = (Flugs*)(mem + stack[i]);

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                            {
                                var cur = (MultyNode*)flug;

                                cur->Count++;

                                break;
                            }

                        #endregion

                        #region ' Data '

                        case Flugs.Data:
                            {
                                continue;
                            }

                        #endregion

                        #region ' Other '

                        default:
                            {
                                return;
                            }

                        #endregion
                    }
                }

                #endregion

                #region ' Insert '

                switch (action)
                {
                    case ChangeAction.InsertTreeNodeLeftLast:
                    {
                        var cur = (MultyNode*)flug;
                        var tmp = AllocMemory(r_comparers.Length * 4);
                        var ptr_tmp = (int*)(mem + tmp);

                        for (int i = 0; i < r_comparers.Length; ++i)
                        {
                            ptr_tmp[i] = -obj.Code;
                        }

                        cur->Left = CreateByteKeyMultyNode(grp, 0, 0, 1, tmp);

                        goto exit; 
                    }
                    case ChangeAction.InsertTreeNodeRightLast:
                    {
                        var cur = (MultyNode*)flug;
                        var tmp = AllocMemory(r_comparers.Length * 4);
                        var ptr_tmp = (int*)(mem + tmp);

                        for (int i = 0; i < r_comparers.Length; ++i)
                        {
                            ptr_tmp[i] = -obj.Code;
                        }

                        cur->Right = CreateByteKeyMultyNode(grp, 0, 0, 1, tmp);

                        goto exit;
                    }
                    default:
                    {
                        return;
                    }
                }

                exit:
                #endregion

                #region ' Optimization data structure '

                if (lvl >= maxLevel)
                {
                    for (int i = 1; i < lvl; ++i)
                    {
                        var par = (MultyNode*)(mem + stack[i - 1]);
                        var itm = (Flugs*)(mem + stack[i]);

                        int* par_ptr;

                        if (par->Left == stack[i])
                        {
                            par_ptr = &par->Left;
                        }
                        else if (par->Right == stack[i])
                        {
                            par_ptr = &par->Right;
                        }
                        else
                        {
                            ErrorReport(65);

                            return;
                        }

                        if (*itm == Flugs.Tree)
                        {
                            var cur = (MultyNode*)itm;
                            var l_count = 0;
                            var r_count = 0;

                            if (cur->Left < 0)
                            {
                                l_count = 1;
                            }
                            else if (cur->Left > 0)
                            {
                                var f = (Flugs*)(mem + cur->Left);

                                if (*f == Flugs.Tree)
                                {
                                    l_count = ((MultyNode*)f)->Count;
                                }
                                else
                                {
                                    l_count = 1;
                                }
                            }

                            if (cur->Right < 0)
                            {
                                r_count = 1;
                            }
                            else if (cur->Right > 0)
                            {
                                var f = (Flugs*)(mem + cur->Right);

                                if (*f == Flugs.Tree)
                                {
                                    r_count = ((MultyNode*)f)->Count;
                                }
                                else
                                {
                                    r_count = 1;
                                }
                            }

                            var l_lvl = GetLevel(l_count);
                            var r_lvl = GetLevel(r_count);

                            cmp = l_lvl - r_lvl;

                            // To right
                            if (cmp > 1)
                            {
                                var l_flug = (Flugs*)(mem + cur->Left);

                                if (*l_flug != Flugs.Tree)
                                {
                                    continue;
                                }

                                var l_node = (MultyNode*)l_flug;
                                var lr_count = 0;

                                if (l_node->Right > 0)
                                {
                                    lr_count = ((MultyNode*)(mem + l_node->Right))->Count;
                                }
                                else if (l_node->Right < 0)
                                {
                                    lr_count = 1;
                                }

                                *par_ptr = cur->Left;

                                cur->Left = l_node->Right;
                                l_node->Right = stack[i];

                                l_node->Count = cur->Count;
                                cur->Count = cur->Count - l_count + lr_count;

                                i += 2;
                            }
                            // to left
                            else if (cmp < -1)
                            {
                                var r_flug = (Flugs*)(mem + cur->Right);

                                if (*r_flug != Flugs.Tree)
                                {
                                    continue;
                                }

                                var r_node = (MultyNode*)r_flug;
                                var rl_count = 0;

                                if (r_node->Left > 0)
                                {
                                    rl_count = ((MultyNode*)(mem + r_node->Left))->Count;
                                }
                                else if (r_node->Left < 0)
                                {
                                    rl_count = 1;
                                }

                                *par_ptr = cur->Right;

                                cur->Right = r_node->Left;
                                r_node->Left = stack[i];

                                r_node->Count = cur->Count;
                                cur->Count = cur->Count - r_count + rl_count;

                                i += 2;
                            }
                        }
                    }
                }

                #endregion
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Geting values tree from multy tree
        public int InlineKeyMulty_GetTree<TKey>(int memoryKey, TKey key, int multyIndex, ValueLockRW locker) 
            where TKey : IInlineKey
        {
            try
            {
                if (locker != null)
                {
                    locker.ReadLock();
                }

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    return 0;
                }

                var grp = key.KeyData;

                Flugs* flug;
                pos = *ptr;

                #endregion

                #region ' Find '

                while (true)
                {
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                        case Flugs.Tree:
                        {
                            var cur = (MultyNode*)(mem + pos);

                            cmp = CompareByteKeys(grp, cur->Data);

                            // already exist
                            if (cmp == 0)
                            {
                                return cur->Tree + multyIndex * 4;
                            }
                            // left
                            else if (cmp < 0)
                            {
                                if (cur->Left > 0)
                                {
                                    pos = cur->Left;

                                    continue;
                                }

                                return 0;
                            }
                            // right
                            else
                            {
                                if (cur->Right > 0)
                                {
                                    pos = cur->Right;

                                    continue;
                                }

                                return 0;
                            }
                        }
                        case Flugs.Data:
                        {
                            break;
                        }
                    }

                }

                #endregion

            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Delete element from multy tree
        public void InlineKeyMulty_Delete<TKey, T>(int memoryKey, TKey key, T obj, IRecordsComparer<T>[] r_comparers, bool dontRemoveGroups, ValueLockRW locker)
            where T : Record 
            where TKey : IInlineKey
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }

                #region ' Prepare '

                var cmp = 0;
                var pos = memoryKey;
                var ptr = (int*)(mem + pos);

                if (*ptr == 0)
                {
                    return;
                }

                Flugs* flug;
                var action = ChangeAction.None;
                var stack = stackalloc int[100];
                var lvl = 0;
                var ext_data = 0;
                var cur_data = 0;
                var grp = key.KeyData;

                pos = *ptr;

                #endregion

                #region ' Find '

                while (true)
                {
                    stack[lvl++] = pos;
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                        case Flugs.Tree:
                        {
                            var cur = (MultyNode*)flug;

                            cmp = CompareByteKeys(grp, cur->Data);

                            // already exist
                            if (cmp == 0)
                            {
                                for (int i = 0; i < r_comparers.Length; ++i)
                                {
                                    Delete(obj, cur->Tree + i * 4, r_comparers[i], null);
                                }

                                var ptr_tmp = *(int*)(mem + cur->Tree);

                                //if (ptr_tmp < 0)
                                //{
                                //    ReleaseMemory(cur->Tree, r_comparers.Length * 4);

                                //    cur->Data = ptr_tmp;
                                //}
                                //else 
                                if (ptr_tmp == 0 && !dontRemoveGroups)
                                {
                                    ReleaseMemory(cur->Tree, r_comparers.Length * 4);

                                    //cur->Data = 0;

                                    action = ChangeAction.RemoveTreeNode;

                                    goto remove;
                                }

                                return;
                            }
                            // left
                            else if (cmp < 0)
                            {
                                if (cur->Left > 0)
                                {
                                    pos = cur->Left;

                                    continue;
                                }

                                return;
                            }
                                // right
                            else
                            {
                                if (cur->Right > 0)
                                {
                                    pos = cur->Right;

                                    continue;
                                }

                                return;
                            }
                        }
                    }
                }

                remove:

                #endregion

                #region ' Correct count '

                for (int i = 0; i < lvl; ++i)
                {
                    flug = (Flugs*)(mem + stack[i]);

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                            {
                                var cur = (MultyNode*)flug;

                                cur->Count--;

                                break;
                            }

                        #endregion

                        #region ' Data '

                        case Flugs.Data:
                            {
                                var cur = (DataNode*)flug;

                                cur->Count--;

                                break;
                            }

                        #endregion

                        #region ' Other '

                        default:
                            {
                                return;
                            }

                        #endregion
                    }
                }

                #endregion

                #region ' Remove '

                int* par_ptr;
                var par = (MultyNode*)(mem + stack[lvl - 2]);

                var cur_pos = stack[lvl - 1];

                // cur node is root
                if (lvl == 1)
                {
                    par_ptr = (int*)(mem + memoryKey);
                }
                else
                {
                    if (par->Right == cur_pos)
                    {
                        par_ptr = &par->Right;
                    }
                    // cur node is left
                    else if (par->Left == cur_pos)
                    {
                        par_ptr = &par->Left;
                    }
                    // bug
                    else
                    {
                        ErrorReport(90);

                        return;
                    }
                }

                switch (action)
                {
                    case ChangeAction.RemoveTreeNode:
                        {
                            var cur = (MultyNode*)(mem + cur_pos);

                            if (cur->Left == 0 && cur->Right == 0)
                            {
                                *par_ptr = 0;

                                ReleaseMemory(cur_pos, sizeof(MultyNode));
                            }
                            else if (cur->Left == 0)
                            {
                                *par_ptr = cur->Right;

                                ReleaseMemory(cur_pos, sizeof(MultyNode));
                            }
                            else if (cur->Right == 0)
                            {
                                *par_ptr = cur->Left;

                                ReleaseMemory(cur_pos, sizeof(MultyNode));
                            }
                            else
                            {
                                par_ptr = &cur->Right;

                                var tmp_pos = cur->Right;
                                var tmp = (MultyNode*)(mem + tmp_pos);

                                while (tmp->Left > 0)
                                {
                                    tmp->Count--;

                                    par_ptr = &tmp->Left;
                                    tmp_pos = tmp->Left;
                                    tmp = (MultyNode*)(mem + tmp_pos);
                                }

                                tmp->Count--;

                                cur->Data = tmp->Data; // todo необходимо применить политику не изменения адреса
                                cur->Tree = tmp->Tree;

                                ReleaseMemory(tmp_pos, sizeof(MultyNode));
                                *par_ptr = tmp->Right;
                            }

                            break;
                        }
                }

                #endregion
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        // Очистить и освободить память
        public void InlineKeyMulty_Clear(int memoryKey, int size, ValueLockRW locker)
        {
            try
            {
                if (locker != null)
                {
                    locker.WriteLock();
                }

                var ptr = (int*)(mem + memoryKey);

                if (*ptr == 0)
                {
                    return;
                }
                else if (*ptr < 0)
                {
                    *ptr = 0;

                    return;
                }

                Flugs* flug;
                var lvl = 0;
                var pos = *ptr;
                var stack = stackalloc int[100];

                while (true)
                {
                    flug = (Flugs*)(mem + pos);
                    stack[lvl++] = pos;

                    switch (*flug)
                    {
                        #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (MultyNode*)flug;

                            // goto left
                            if (cur->Left > 0)
                            {
                                pos = cur->Left;
                            }
                            // goto left
                            else if (cur->Right > 0)
                            {
                                pos = cur->Right;
                            }
                            // remove
                            else
                            {
                                // Ключ дерева
                                var ptr_tmp = mem + cur->Data;

                                ReleaseMemory(cur->Data, *ptr_tmp);

                                // Вложенные деревья
                                for (int i = 0; i < size; ++i)
                                {
                                    ClearTree(cur->Tree + i * 4, null);
                                }

                                ReleaseMemory(pos, sizeof(MultyNode));

                                // root node
                                if (lvl == 1)
                                {
                                    *ptr = 0;

                                    return;
                                }

                                lvl -= 2;

                                var par = (MultyNode*)(mem + stack[lvl]);

                                if (par->Left == pos)
                                {
                                    par->Left = 0;
                                }
                                else
                                {
                                    par->Right = 0;
                                }
                            }

                            break;
                        }

                        #endregion

                        #region ' Other '

                        default:
                        {
                            ErrorReport(111);

                            return;
                        }

                        #endregion
                    }
                }
            }
            finally
            {
                if (locker != null)
                {
                    locker.Unlock();
                }
            }
        }

        #endregion

        #region ' Helper '

        // Функция сравнивает ключи
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int CompareByteKeys(byte[] grp, int data)
        {
            var ptr_tmp = mem + data;

            var len = *ptr_tmp++;

            if (grp.Length > len)
            {
                return 1;
            }
            else if (grp.Length < len)
            {
                return -1;
            }

            for (var i = 0; i < grp.Length; i++)
            {
                if (grp[i] > ptr_tmp[i])
                {
                    return 1;
                }
                else if (grp[i] < ptr_tmp[i])
                {
                    return -1;
                }
            }

            return 0;
        }

        // Creating three node
        int CreateTreeNode(int val, int left, int right, int count)
        {
            var ret = AllocMemory(sizeof(TreeNode));
            var ptr = (TreeNode*)(mem + ret);

            ptr->Flug = Flugs.Tree;
            ptr->Data = val;
            ptr->Left = left;
            ptr->Right = right;
            ptr->Count = count;

            return ret;
        }

        // Creating data node
        int CreateDataNode(int count)
        {
            var ret = AllocMemory(count * 4 + sizeof(DataNode));
            var ptr = (DataNode*) (mem + ret);

            ptr->Flug = Flugs.Data;
            ptr->Count = count;
            ptr->Min = int.MaxValue;
            ptr->Max = 0;
            ptr->Data = ret + sizeof (DataNode);

            return ret;
        }

        // Creating data node
        int CreateDataNode()
        {
            var ret = AllocMemory(sizeof (DataNode));
            var ptr = (DataNode*) (mem + ret);

            ptr->Flug = Flugs.Data;
            ptr->Count = 0;
            ptr->Min = int.MaxValue;
            ptr->Max = 0;
            ptr->Data = 0;

            return ret;
        }

        // Creating group node
        int CreateGroupNode(int data, int left, int right, int count)
        {
            var ret = AllocMemory(sizeof(GroupNode));
            var ptr = (GroupNode*)(mem + ret);

            ptr->Flug = Flugs.Tree;
            ptr->Data = data;
            ptr->Left = left;
            ptr->Right = right;
            ptr->Count = count;

            return ret;
        }

        // Creating group node
        int CreateMultyNode(int data, int left, int right, int count, int tree)
        {
            var ret = AllocMemory(sizeof(MultyNode));
            var ptr = (MultyNode*)(mem + ret);

            ptr->Flug = Flugs.Tree;
            ptr->Data = data;
            ptr->Tree = tree;
            ptr->Left = left;
            ptr->Right = right;
            ptr->Count = count;

            return ret;
        }

        // Creating group node
        int CreateByteKeyMultyNode(byte[] data, int left, int right, int count, int tree)
        {
            // Выделяем память под ключ
            var tmp = AllocMemory(data.Length + 1);
            var ptr_tmp = mem + tmp;
            
            // Первый байт длинна ключа
            *ptr_tmp++ = (byte)data.Length;

            for (int i = 0; i < data.Length; ++i)
            {
                ptr_tmp[i] = data[i];
            }
            
            var ret = AllocMemory(sizeof(MultyNode));
            var ptr = (MultyNode*)(mem + ret);

            ptr->Flug = Flugs.Tree;
            ptr->Data = tmp;
            ptr->Tree = tree;
            ptr->Left = left;
            ptr->Right = right;
            ptr->Count = count;

            return ret;
        }

        // Creating index link
        int CreateIndexLink<T>(IDataIndexBase<T> index) where T : Record
        {
            var ret = AllocMemory(sizeof(IndexLink) + index.Name.Length * 2);
            var ptr = (IndexLink*)(mem + ret);

            ptr->Length = index.Name.Length;
            ptr->Root = AllocMemory(4);
            ptr->Next = 0;

            var str = (char*)(mem + ret + sizeof(IndexLink));

            for (int i = 0; i < index.Name.Length; ++i)
            {
                str[i] = index.Name[i];
            }

            // clear
            var ptr2 = (int*)(mem + ptr->Root);

            *ptr2 = 0;

            return ret;
        }

        // Optimization of data structure
        void SmartOptimization2(int par_pos, int cur_pos)
        {
            var par = (LinkNode*)(mem + par_pos);
            var cur = (Flugs*)(mem + cur_pos);
            var count = ((CountNode*)cur)->Count;

            bool isParLeft;
            var tmp_adr = 0;

            if (par->Left == cur_pos)
            {
                isParLeft = true;

                tmp_adr = CreateDataNode(count);
            }
            else if (par->Right == cur_pos)
            {
                isParLeft = false;

                tmp_adr = CreateDataNode(count);
            }
            else
            {
                return;
            }


        }

        // Optimization of data structure
        void SmartOptimization(int par_pos, int cur_pos)
        {
            var par     = (LinkNode*)(mem + par_pos);
            var cur = (Flugs*)(mem + cur_pos);
            var count = ((CountNode*)cur)->Count;

            bool isParLeft;
            var tmp_adr = 0;

            if (par->Left == cur_pos)
            {
                isParLeft = true;

                tmp_adr = CreateDataNode(count);
            }
            else if (par->Right == cur_pos)
            {
                isParLeft = false;

                tmp_adr = CreateDataNode(count);
            }
            else
            {
                return;
            }

            par = (LinkNode*)(mem + par_pos);
            cur = (Flugs*)(mem + cur_pos);

            var stack = stackalloc Flugs*[100];
            var arr = (int*) (mem + tmp_adr + sizeof (TreeNode));
            var lvl = 0;
            var ind = 0;

            var flug = cur;
            var isUp = false;
            var isLeft = false;

            while (lvl >= 0)
            {
                #region ' Tree '

                if ((*flug & Flugs.Tree) == Flugs.Tree)
                {
                    var itm = (TreeNode*) flug;

                    if (!isUp)
                    {
                        if (itm->Left == 0)
                        {
                            arr[ind++] = itm->Data;
                        }
                        else if (itm->Left < 0)
                        {
                            arr[ind++] = -itm->Left;
                            arr[ind++] = itm->Data;
                        }
                        else
                        {
                            stack[lvl++] = flug;

                            flug = (Flugs*) (mem + itm->Left);

                            isUp = false;

                            continue;
                        }
                    }

                    if (!isUp || isLeft)
                    {
                        if (itm->Right > 0)
                        {
                            stack[lvl++] = flug;

                            flug = (Flugs*) (mem + itm->Right);

                            isUp = false;

                            continue;
                        }
                        else if (itm->Right < 0)
                        {
                            arr[ind++] = -itm->Right;
                        }
                    }

                    // exit
                    if (ind >= count)
                    {
                        break;
                    }
                }

                    #endregion

                #region ' Data '

                else if ((*flug & Flugs.Data) == Flugs.Data)
                {
                    var itm = (DataNode*) flug;
                    var dat = (int*) (mem + itm->Data);

                    for (int i = 0; i < itm->Count; ++i)
                    {
                        arr[ind++] = dat[i];
                    }
                }

                    #endregion

                #region ' Other '

                else
                {
                    ErrorReport(10);

                    break;
                }

                #endregion

                var last = (LinkNode*) stack[lvl - 1];

                if (lvl == 0)
                {
                    break;
                }

                // last node is right
                if (last->Right == (byte*)flug - mem)
                {
                    isLeft = false;
                }
                // last node is left
                else if (last->Left == (byte*)flug - mem)
                {
                    isLeft = true;

                    if ((*(Flugs*)last & Flugs.Tree) == Flugs.Tree)
                    {
                        arr[ind++] = ((TreeNode*) last)->Data;
                    }
                }
                else
                {
                    ErrorReport(9);

                    break;
                }

                isUp = true;

                flug = stack[--lvl];
            }

            if (isParLeft)
            {
                par->Left = tmp_adr;
            }
            else
            {
                par->Right = tmp_adr;
            }

            var tmp = (DataNode*) (mem + tmp_adr);

            tmp->Min = arr[0];
            tmp->Max = arr[tmp->Count - 1];
        }

        // Remove tree node
        void RemoveTreeNode(int* par_ptr, int cur_pos)
        {
            var cur = (TreeNode*)(mem + cur_pos);

            if (cur->Left == 0 && cur->Right == 0)
            {
                *par_ptr = 0;

                ReleaseMemory(cur_pos, sizeof(TreeNode));
            }
            else if (cur->Left == 0)
            {
                *par_ptr = cur->Right;

                ReleaseMemory(cur_pos, sizeof(TreeNode));
            }
            else if (cur->Right == 0)
            {
                *par_ptr = cur->Left;

                ReleaseMemory(cur_pos, sizeof(TreeNode));
            }
            else if (cur->Left < 0)
            {
                cur->Data = -cur->Left;
                cur->Left = 0;
            }
            else if (cur->Right < 0)
            {
                cur->Data = -cur->Right;
                cur->Right = 0;
            }
            else
            {
                par_ptr = &cur->Right;

                var tmp_pos = cur->Right;
                var tmp = (TreeNode*)(mem + tmp_pos);

                while (tmp->Left > 0)
                {
                    tmp->Count--;

                    par_ptr = &tmp->Left;
                    tmp_pos = tmp->Left;
                    tmp = (TreeNode*)(mem + tmp_pos);
                }

                tmp->Count--;

                if (tmp->Left < 0)
                {
                    cur->Data = -tmp->Left;
                    tmp->Left = 0;

                    if (tmp->Right == 0)
                    {
                        *par_ptr = -tmp->Data;
                        ReleaseMemory(tmp_pos, sizeof(TreeNode));
                    }

                    return;
                }

                cur->Data = tmp->Data;

                ReleaseMemory(tmp_pos, sizeof(TreeNode));
                *par_ptr = tmp->Right;
            }
        }

        // Remove data node
        void RemoveDataNode(int pos)
        {
            var cur = (DataNode*)(mem + pos);

            ReleaseMemory(cur->Data, cur->Count * 4);
            ReleaseMemory(pos, sizeof(DataNode));
        }

        // Release memory
        void ReleaseMemory(int pos, int size)
        {
            var len = size;
            int* ptr;

            if (size <= 20)
            {
                ptr = (int*)(header + (size - 4) * 4 + 12);
            }
            else
            {
                ptr = (int*)(header + 80);

                var tmp = (int*)(mem + *ptr);
                len = tmp[1];
            }

            if (*ptr == 0)
            {
                *ptr = pos;

                var tmp = (int*)(mem + *ptr);

                tmp[0] = 0;

                if (size > 20)
                {
                    tmp[1] = size;
                }
            }
            else
            {
                var tmp = (int*)(mem + *ptr);

                if (pos + size == *ptr)
                {
                    *ptr = *tmp;

                    ReleaseMemory(pos, len + size);

                    return;
                }
                
                if (*ptr + len == pos)
                {
                    var ppc = *ptr;

                    *ptr = *tmp;

                    ReleaseMemory(ppc, len + size);

                    return;
                }

                var qwe = (int*)(mem + pos);

                *qwe = *ptr;
                *ptr = pos;

                if (size > 20)
                {
                    qwe[1] = size;
                }
            }
        }

        // Aloc
        int AllocMemory(int size)
        {
            int bar = 0;
            int* ptr = &bar;
            var len = size;

            if (size <= 20)
            {
                ptr = (int*)(header + (size - 4) * 4 + 12);
            }

            var ret = 0;

            if (*ptr == 0)
            {
                ptr = (int*)(header + 80);

                for (int i = 0; i < 20 && *ptr != 0; ++i)
                {
                    var tmp = (int*)(mem + *ptr);

                    len = tmp[1];

                    var ttr = len - size;

                    if (ttr == 0)
                    {
                        ret = *ptr;

                        *ptr = *tmp;

                        goto exit;
                    }
                    if (ttr > 4)
                    {
                        var ppc = *ptr;

                        if (ttr <= 20)
                        {
                            *ptr = *tmp;

                            ReleaseMemory(ppc, ttr);
                        }

                        tmp[1] = ttr;

                        ret = ppc + ttr;

                        goto exit;
                    }

                    ptr = tmp;
                }

                var h = (int*)header;

                ret = *h;

                *h += size;

                if (*h >= length)
                {
                    fs.SetLength(length * 2);

                    InitFile();
                }

                //Debug.WriteLine(*h + ", " + size);
            }
            else
            {
                ret = *ptr;

                var tmp = (int*)(mem + ret);
                
                *ptr = *tmp;
            }

            exit:

            return ret;
        }

        // Report
        void ErrorReport(int mes)
        {
            Debugger.Break();
        }

        // Init memory mapped file
        void InitFile()
        {
            if (mapping != null)
            {
                mapping.SafeMemoryMappedViewHandle.ReleasePointer();
                mapping.Dispose();
            }
            if (file != null)
            {
                file.Dispose();
            }

            if (disposed)
            {
                return;
            }

            file = fs.CreateMMF(key);

            length  = fs.Length;
            mapping = file.CreateViewAccessor(0, length, MemoryMappedFileAccess.ReadWrite);
            header  = mapping.Pointer(0);
            mem     = header + headerSize;
        }

        // Release Resources
        public void Dispose()
        {
            if (disposed)
            {
                return;
            }

            disposed = true;

            if (mapping != null)
            {
                mapping.SafeMemoryMappedViewHandle.ReleasePointer();
                mapping.Dispose();
            }

            if (file != null)
            {
                file.Dispose();
            }

            if (fs != null)
            {
                fs.Dispose();
            }
        }

        // Check tree data for errors
        public bool CheckTree<T>(int memoryKey, IRecordsComparer<T> comparer, IRecordsGetter<T> getter) where T : Record
        {
            bool ret = false;
            var ptr = (int*)(mem + memoryKey);
            var tmp = 0;

            if (*ptr > 0)
            {
                CheckTree(*ptr, ref ret, comparer, getter, out tmp);
            }

            return ret;
        }

        // Check node data for errors
        int CheckTree<T>(int node, ref bool flug, IRecordsComparer<T> comparer, IRecordsGetter<T> getter, out int data) where T : Record
        {
            if (node < 1)
            {
                Debug.WriteLine("Warning: The node " + node + " has offset < 1. Situation A");

                flug |= true;

                data = 0;

                return 0;
            }

            var tmp = (Flugs*)(mem + node);
            var cmp = 0;

            switch (*tmp)
            {
                case Flugs.Tree:
                {
                    var cur = (TreeNode*)tmp;

                    data = cur->Data;

                    #region ' check data and count '

                    if (cur->Count < 0)
                    {
                        Debug.WriteLine("Warning: The tree node " + node + "(value=" + cur->Data + ",count=" + cur->Count + ")" + " has count < 0. Situation B");

                        flug |= true;
                    }

                    if (cur->Data < 1)
                    {
                        Debug.WriteLine("Warning: The tree node " + node + "(value=" + cur->Data + ",count=" + cur->Count + ")" + " has data < 1. Situation C");

                        flug |= true;
                    }

                    // left data and count
                    var l_count = 0;
                    var l_data = 0;

                    if (cur->Left > 0)
                    {
                        //l_data = ((TreeNode*)(mem + (cur->Left)))->Data;

                        l_count = CheckTree(cur->Left, ref flug, comparer, getter, out l_data);
                    }
                    else if (cur->Left < 0)
                    {
                        l_count = 1;

                        l_data = -cur->Left;
                    }

                    // right data and count
                    var r_count = 0;
                    var r_data = 0;

                    if (cur->Right > 0)
                    {
                        //r_data = ((TreeNode*)(mem + (cur->Right)))->Data;

                        r_count = CheckTree(cur->Right, ref flug, comparer, getter, out r_data);
                    }
                    else if (cur->Right < 0)
                    {
                        r_count = 1;

                        r_data = -cur->Right;
                    }
                    
                    if (l_data != 0)
                    {
                        var ppc = getter.GetRecord(l_data);
                        cmp = comparer.CompareRecords(ppc, cur->Data);

                        if (cmp >= 0)
                        {
                            Debug.WriteLine("Warning: The tree node " + node + "(value=" + cur->Data + ",count=" + cur->Count + ")" + " has wrong data on left. l_data >= cur->Data. Situation D");

                            flug |= true;
                        }
                    }

                    if (r_data != 0)
                    {
                        cmp = comparer.CompareRecords(getter.GetRecord(r_data), cur->Data);

                        if (cmp <= 0)
                        {
                            Debug.WriteLine("Warning: The tree node " + node + "(value=" + cur->Data + ",count=" + cur->Count + ")" + " has wrong data on left. r_data <= cur->Data. Situation E");

                            flug |= true;
                        }
                    }

                    var c = r_count + l_count + 1;

                    if (cur->Count != c)
                    {
                        Debug.WriteLine("Warning: The tree node " + node + "(value=" + cur->Data + ",count=" + cur->Count + ")" + " has wrong count. Must be:" + c + ". Situation F");

                        flug |= true;
                    } 
                    #endregion

                    return cur->Count;
                }
                case Flugs.Data:
                {
                    var cur = (DataNode*)tmp;
                    var dat = (int*)(mem + cur->Data);

                    #region ' check count '

                    if (cur->Count > 0)
                    {
                        if (dat[0] != cur->Min)
                        {
                            Debug.WriteLine("Warning: The data node " + node + "(min=" + cur->Min + ", max=" + cur->Max + ",count=" + cur->Count + ")" + " the Min does not meet the first element!. Situation Q");

                            flug |= true;
                        }

                        if (dat[cur->Count-1] != cur->Max)
                        {
                            Debug.WriteLine("Warning: The data node " + node + "(min=" + cur->Min + ", max=" + cur->Max + ",count=" + cur->Count + ")" + " the Max does not meet the lastt element!. Situation R");

                            flug |= true;
                        }

                        for (int i = cur->Count-1; i > 0; i--)
                        {
                            cmp = comparer.CompareRecords(getter.GetRecord(dat[i - 1]), dat[i]);

                            if (cmp >= 0)
                            {
                                Debug.WriteLine("Warning: The data node " + node + "(min=" + cur->Min + ", max=" + cur->Max + ",count=" + cur->Count + ")" + " wrong data structure!. Situation S");

                                flug |= true;

                                break;
                            }
                        }

                        data = dat[cur->Count - 1];
                    }
                    else
                    {
                        data = 0;
                    }

                    #endregion

                    return cur->Count;
                }
                default:
                {
                    Debug.WriteLine("Warning: The tree node " + node + " has unknown type!");

                    flug |= true;
                    data = 0;

                    break;
                }
            }

            return 0;
        }

        // Check tree data for errors
        public bool CheckTree<T>(int memoryKey, IRecordsComparer<T> comparer, IRecordsGetter<T> getter, Log log) where T : Record
        {
            bool ret = false;
            var ptr = (int*)(mem + memoryKey);
            var tmp = 0;

            if (*ptr > 0)
            {
                int max_level = 0;

                try
                {
                    CheckTree(*ptr, ref ret, comparer, getter, out tmp, ref max_level, 0, log);
                }
                catch (Exception ex)
                {
                    log.Append(ex.Message);

                    ret = true;
                }

                if (max_level >= 100)
                {
                    log.Append("Warning: The maximum level is exceeded. Tree at " + memoryKey);

                    ret = true;
                }
            }

            return ret;
        }

        // Check node data for errors
        int CheckTree<T>(int node, ref bool flug, IRecordsComparer<T> comparer, IRecordsGetter<T> getter, out int data, ref int max_level, int cur_level, Log log) where T : Record
        {
            if (cur_level > max_level)
            {
                max_level = cur_level;
            }

            data = 0;

            if (node < 1)
            {
                log.Append("Warning: The node ");
                log.Append(node);
                log.Append(" has offset < 1. Situation A\r\n");

                flug = true;

                data = 0;

                return 0;
            }

            var tmp = (Flugs*)(mem + node);
            var cmp = 0;

            switch (*tmp)
            {
                case Flugs.Tree:
                    {
                        var cur = (TreeNode*)tmp;

                        data = cur->Data;

                        #region ' check data and count '

                        if (cur->Count < 1)
                        {
                            log.Append("Warning: The tree node ");
                            log.Append(node);
                            log.Append("(value=");
                            log.Append(cur->Data);
                            log.Append(",count=");
                            log.Append(cur->Count);
                            log.Append( ") has count < 1. Situation C1\r\n");

                            flug = true;

                            return cur->Count;
                        }

                        var cur_rec = getter.GetRecord(cur->Data);

                        if (cur_rec == null)
                        {
                            log.Append("Warning: The tree node ");
                            log.Append(node);
                            log.Append("(value=");
                            log.Append(cur->Data);
                            log.Append(",count=");
                            log.Append(cur->Count);
                            log.Append(") has wrong current data. Object is not exist. Situation C2\r\n");

                            flug = true;

                            return cur->Count;
                        }

                        // left data and count
                        var l_count = 0;
                        var l_data = 0;

                        if (cur->Left > 0)
                        {
                            l_count = CheckTree(cur->Left, ref flug, comparer, getter, out l_data, ref max_level, cur_level + 1, log);
                        }
                        else if (cur->Left < 0)
                        {
                            l_count = 1;

                            l_data = -cur->Left;
                        }

                        // right data and count
                        var r_count = 0;
                        var r_data = 0;

                        if (cur->Right > 0)
                        {
                            r_count = CheckTree(cur->Right, ref flug, comparer, getter, out r_data, ref max_level, cur_level + 1, log);
                        }
                        else if (cur->Right < 0)
                        {
                            r_count = 1;

                            r_data = -cur->Right;
                        }

                        if (l_data != 0)
                        {
                            var ppc = getter.GetRecord(l_data);

                            if (ppc == null)
                            {
                                log.Append("Warning: The tree node ");
                                log.Append(node);
                                log.Append("(value=");
                                log.Append(cur->Data);
                                log.Append(",count=");
                                log.Append(cur->Count);
                                log.Append(") has wrong data on left. Object is not exist. Situation L1\r\n");

                                flug = true;

                                return cur->Count;
                            }

                            cmp = comparer.CompareRecords(ppc, cur->Data);

                            if (cmp >= 0)
                            {
                                log.Append("Warning: The tree node ");
                                log.Append(node);
                                log.Append("(value=");
                                log.Append(cur->Data);
                                log.Append(",count=");
                                log.Append(cur->Count);
                                log.Append(") has wrong data on left. l_data >= cur->Data. Situation L2\r\n");

                                flug = true;

                                return cur->Count;
                            }
                        }

                        if (r_data != 0)
                        {
                            var ppc = getter.GetRecord(r_data);

                            if (ppc == null)
                            {
                                log.Append("Warning: The tree node ");
                                log.Append(node);
                                log.Append("(value=");
                                log.Append(cur->Data);
                                log.Append(",count=");
                                log.Append(cur->Count);
                                log.Append(") has wrong data on right. Object is not exist. Situation R1\r\n");

                                flug = true;

                                return cur->Count;
                            }

                            cmp = comparer.CompareRecords(ppc, cur->Data);

                            if (cmp <= 0)
                            {
                                log.Append("Warning: The tree node ");
                                log.Append(node);
                                log.Append("(value=");
                                log.Append(cur->Data);
                                log.Append(",count=");
                                log.Append(cur->Count);
                                log.Append(") has wrong data on right. r_data <= cur->Data. Situation R2\r\n");

                                flug = true;

                                return cur->Count;
                            }
                        }

                        var c = r_count + l_count + 1;

                        if (cur->Count != c)
                        {
                            log.Append("Warning: The tree node ");
                            log.Append(node);
                            log.Append("(value=");
                            log.Append(cur->Data);
                            log.Append(",count=");
                            log.Append(cur->Count);
                            log.Append(") has wrong count. Must be:" + c + ". Situation C4\r\n");

                            flug = true;

                            return cur->Count;
                        }
                        #endregion

                        return cur->Count;
                    }
                case Flugs.Data:
                    {
                        var cur = (DataNode*)tmp;
                        var dat = (int*)(mem + cur->Data);

                        #region ' check count '

                        if (cur->Count > 0)
                        {
                            if (dat[0] != cur->Min)
                            {
                                log.Append("Warning: The data node ");
                                log.Append(node);
                                log.Append("(min=");
                                log.Append(cur->Min);
                                log.Append(", max=");
                                log.Append(cur->Max);
                                log.Append(",count=");
                                log.Append(cur->Count);
                                log.Append(") the Min does not meet the first element!. Situation Q\r\n");

                                flug = true;

                                return cur->Count;
                            }

                            if (dat[cur->Count - 1] != cur->Max)
                            {
                                log.Append("Warning: The data node ");
                                log.Append(node);
                                log.Append("(min=");
                                log.Append(cur->Min);
                                log.Append(", max=");
                                log.Append(cur->Max);
                                log.Append(",count=");
                                log.Append(cur->Count);
                                log.Append(") the Max does not meet the lastt element!. Situation R\r\n");

                                flug = true;

                                return cur->Count;
                            }

                            for (int i = cur->Count - 1; i > 0; i--)
                            {
                                cmp = comparer.CompareRecords(getter.GetRecord(dat[i - 1]), dat[i]);

                                if (cmp >= 0)
                                {
                                    log.Append("Warning: The data node ");
                                    log.Append(node);
                                    log.Append("(min=");
                                    log.Append(cur->Min);
                                    log.Append(", max=");
                                    log.Append(cur->Max);
                                    log.Append(",count=");
                                    log.Append(cur->Count);
                                    log.Append(") wrong data structure!. Situation S\r\n");

                                    flug = true;

                                    return cur->Count;
                                }
                            }

                            data = dat[cur->Count - 1];
                        }

                        #endregion

                        return cur->Count;
                    }
                default:
                    {
                        log.Append("Warning: The tree node ");
                        log.Append(node);
                        log.Append(" has unknown type!");

                        flug = true;

                        return 0;
                    }
            }

            return 0;
        }

        // Check group data for errors
        public bool CheckGroup<G,T>(int memoryKey, IRecordsComparer<T> t_comparer, IGroupComparer<G> g_comparer, IHashed<G,T> hash, IRecordsGetter<T> getter, Log log) where G : IComparable<G> where T : Record
        {
            bool ret = false;
            var ptr = (int*)(mem + memoryKey);
            var tmp = 0;

            int max_level = 0;

            try
            {
                if (*ptr > 0)
                {
                    CheckGroup(*ptr, ref ret, t_comparer, g_comparer, hash, getter, out tmp, ref max_level, 0, log);
                }
            }
            catch (Exception ex)
            {
                log.Append(ex);

                ret = true;
            }

            if (max_level >= 100)
            {
                log.Append("Warning: The maximum level is exceeded");

                ret = true;
            }
            
            return ret;
        }

        // Check node data for errors
        int CheckGroup<G,T>(int node, ref bool flug, IRecordsComparer<T> t_comparer, IGroupComparer<G> g_comparer, IHashed<G,T> hash, IRecordsGetter<T> getter, out int data, ref int max_level, int cur_level, Log log) where G : IComparable<G> where T : Record
        {
            if (cur_level > max_level)
            {
                max_level = cur_level;
            }
            
            data = 0;

            if (node < 1)
            {
                log.Append("Warning: The group ");
                log.Append(node);
                log.Append(" has offset < 1. Situation GA\r\n");

                flug = true;

                return 0;
            }

            var tmp = (Flugs*)(mem + node);
            var cmp = 0;

            switch (*tmp)
            {
                case Flugs.Tree:
                    {
                        var cur = (GroupNode*)tmp;

                        var cur_data = -cur->Data;

                        if (cur->Data > 0)
                        {
                            var tmp_tree_flug = (Flugs*)(mem + cur->Data);

                            switch (*tmp_tree_flug)
                            {
                                case Flugs.Tree:
                                {
                                    var tmp_tree = (TreeNode*)tmp_tree_flug;

                                    cur_data = tmp_tree->Data;

                                    break;
                                }
                                case Flugs.Data:
                                {
                                    var tmp_tree = (DataNode*)tmp_tree_flug;
                                    var tmp_tree_dat = (int*)(mem + tmp_tree->Data);

                                    cur_data = tmp_tree_dat[0];

                                    break;
                                }
                            }
                        }

                        data = cur_data;

                        #region ' check data and count '

                        if (cur->Count < 1)
                        {
                            log.Append("Warning: The group ");
                            log.Append(node);
                            log.Append("(value=");
                            log.Append(cur->Data);
                            log.Append(",count=");
                            log.Append(cur->Count);
                            log.Append(") has count < 1. Situation GC1\r\n");

                            flug = true;

                            return cur->Count;
                        }

                        if (cur->Data == 0)
                        {
                            log.Append("Warning: The group ");
                            log.Append(node);
                            log.Append("(value=");
                            log.Append(cur->Data);
                            log.Append(",count=");
                            log.Append(cur->Count);
                            log.Append(") has data == 0. Situation GC2\r\n");

                            flug = true;

                            return cur->Count;
                        }
                        // Check sub tree
                        else if (cur->Data > 0)
                        {
                            flug |= CheckTree(node + 1, t_comparer, getter, log);
                        }

                        // left data and count
                        var l_count = 0;
                        var l_data = 0;

                        if (cur->Left > 0)
                        {
                            l_count = CheckGroup(cur->Left, ref flug, t_comparer, g_comparer, hash, getter, out l_data, ref max_level, cur_level + 1, log);
                        }
                        else if (cur->Left < 0)
                        {
                            l_count = 1;

                            l_data = -cur->Left;
                        }

                        // right data and count
                        var r_count = 0;
                        var r_data = 0;

                        if (cur->Right > 0)
                        {
                            r_count = CheckGroup(cur->Right, ref flug, t_comparer, g_comparer, hash, getter, out r_data, ref max_level, cur_level + 1, log);
                        }
                        else if (cur->Right < 0)
                        {
                            r_count = 1;

                            r_data = -cur->Right;
                        }

                        var cur_rec = getter.GetRecord(cur_data);

                        if (cur_rec == null)
                        {
                            log.Append("Warning: The group ");
                            log.Append(node);
                            log.Append("(value=");
                            log.Append(cur->Data);
                            log.Append(",count=");
                            log.Append(cur->Count);
                            log.Append(") has wrong cur data. Read is not exist. Situation GC3\r\n");

                            flug = true;

                            return cur->Count;
                        }

                        if (l_data != 0)
                        {
                            var cur_grp = hash.GetHashKey(cur_rec);
                            var l_rec = getter.GetRecord(l_data);

                            if (l_rec == null)
                            {
                                log.Append("Warning: The group ");
                                log.Append(node);
                                log.Append("(value=");
                                log.Append(cur->Data);
                                log.Append(",count=");
                                log.Append(cur->Count);
                                log.Append(") has wrong node on left. Read is not exist. Situation GL1\r\n");

                                flug = true;

                                return cur->Count;
                            }

                            cmp = g_comparer.CompareGroups(cur_grp, l_data);

                            if (cmp <= 0)
                            {
                                log.Append("Warning: The group ");
                                log.Append(node);
                                log.Append("(value=");
                                log.Append(cur->Data);
                                log.Append(",count=");
                                log.Append(cur->Count);
                                log.Append(") has wrong data on left. l_data >= cur->Data. Situation GL2\r\n");

                                flug = true;

                                return cur->Count;
                            }
                        }

                        if (r_data != 0)
                        {
                            var cur_grp = hash.GetHashKey(cur_rec);
                            var r_rec = getter.GetRecord(r_data);

                            if (r_rec == null)
                            {
                                log.Append("Warning: The group ");
                                log.Append(node);
                                log.Append("(value=");
                                log.Append(cur->Data);
                                log.Append(",count=");
                                log.Append(cur->Count);
                                log.Append(") has wrong node on right. Read is not exist. Situation GR1\r\n");

                                flug = true;

                                return cur->Count;
                            }

                            cmp = g_comparer.CompareGroups(cur_grp, r_data);

                            if (cmp >= 0)
                            {
                                log.Append("Warning: The group ");
                                log.Append(node);
                                log.Append("(value=");
                                log.Append(cur->Data);
                                log.Append(",count=");
                                log.Append(cur->Count);
                                log.Append(") has wrong data on right. r_data <= cur->Data. Situation GR2\r\n");

                                flug = true;

                                return cur->Count;
                            }
                        }

                        var c = r_count + l_count + 1;

                        if (cur->Count != c)
                        {
                            log.Append("Warning: The group ");
                            log.Append(node);
                            log.Append("(value=");
                            log.Append(cur->Data);
                            log.Append(",count=");
                            log.Append(cur->Count);
                            log.Append(") has wrong count. Must be:" + c + ". Situation GC4\r\n");

                            flug = true;

                            return cur->Count;
                        }
                        #endregion

                        return cur->Count;
                    }
                case Flugs.Data:
                    {
                        var cur = (GroupDataNode*)tmp;
                        var dat = (int*)(mem + cur->Data);

                        #region ' check count '

                        if (cur->Count > 0)
                        {
                            for (int i = cur->Count - 1; i > 0; i--)
                            {
                                cmp = t_comparer.CompareRecords(getter.GetRecord(dat[i - 1]), dat[i]);

                                if (cmp >= 0)
                                {
                                    log.Append("Warning: The data node ");
                                    log.Append(node);
                                    log.Append(",count=");
                                    log.Append(cur->Count);
                                    log.Append(") wrong data structure!. Situation S\r\n");

                                    flug = true;

                                    return cur->Count;
                                }
                            }

                            data = dat[cur->Count - 1];
                        }

                        #endregion

                        return cur->Count;
                    }
                default:
                    {
                        log.Append("Warning: The group ");
                        log.Append(node);
                        log.Append(" has unknown type! Situation G0");

                        flug = true;

                        return 0;
                    }
            }

            return 0;
        }
        
        // Check index tree
        public bool CheckIndexTree(Log log)
        {
            var flug = false;

            try
            {
                var count = (int*)(header + 4);
                var root = (int*)(header + 8);

                log.AppendLine("Begin check index catalog");
                log.AppendLine("Total count: " + *count);
                log.AppendLine("Root: " + *root);
                log.AppendLine("");

                if (*count == 0)
                {
                    return false;
                }

                if (*root >= length || *root < 0)
                {
                    log.AppendLine("Wrong root item offest.");

                    return true;
                }

                var cur = (IndexLink*)(mem + *root);

                for (int i = 0; i < *count; ++i)
                {
                    log.Append("Index: ");

                    var str = (char*)((byte*)cur + sizeof (IndexLink));

                    for (int j = 0; j < cur->Length && j < 512; ++j)
                    {
                        log.Append(str[j]);
                    }

                    log.AppendLine("");

                    if (cur->Length > 512 || cur->Length < 0)
                    {
                        flug = true;

                        log.AppendLine("Wrong name length. Is more than 512.");
                    }

                    if (cur->Root >= length || cur->Root < 0)
                    {
                        flug = true;

                        log.AppendLine("Wrong root item offest.");
                    }
                    else
                    {
                        var tmp = (int*)(mem + cur->Root);

                        log.AppendLine("Root val: " + *tmp);
                    }

                    if (cur->Next >= length || cur->Next < 0 || (cur->Next == 0 && i < *count - 1))
                    {
                        flug = true;

                        log.AppendLine("Wrong next item offest.");

                        break;
                    }

                    cur = (IndexLink*)(mem + cur->Next);
                }
            }
            catch (Exception e)
            {
                log.AppendLine(e.Message);

                return true;
            }

            return flug;
        }

        // Этот костыль возник в результате изменения размера файла и как следствие изменения базового адреса файла в памяти
        // при этом все указатели слетают. Этот костыль будет вызываться какждый раз перед потенциальной возможностью выделения памяти.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void MakeSpace()
        {
            var h = (int*)header;

            if (*h + 20 * 1000 >= length)
            {
                fs.SetLength(length * 2);

                InitFile();
            }
        }
        #endregion

        #region ' Stuctures '

        // TreeNode 17
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        struct TreeNode
        {
            public Flugs Flug;
            public int  Count;
            public int  Left;
            public int  Right;
            public int  Data;

            public override string ToString()
            {
                return "count: " + Count + ", val: " + Data;
            }
        }

        // 
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        struct DataNode
        {
            public Flugs Flug;
            public int Count;
            public int Min;
            public int Max;
            public int Data;

            public override string ToString()
            {
                return "count: " + Count + ", min: " + Min + ", max: " + Max;
            }

            public string ToString(byte* mem)
            {
                var val = "";
                var ptr = (int*) (mem + Data);

                for (int i = 0; i < Count && i < 50; ++i)
                {
                    val += ptr[i] + ", ";
                }

                if (Count > 50)
                {
                    val += ", and more " + (Count - 50) + " ... ";
                }

                return "count: " + Count + ", min: " + Min + ", max: " + Max + ", values: " + val;
            }
        }

        // 
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        struct GroupNode
        {
            public Flugs Flug;
            public int Data; // Contains code of any object included in this group
            public int Count;
            public int Left;
            public int Right;

            public override string ToString()
            {
                return "right: " + Right + ", left: " + Left;
            }
        }

        // 
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        struct MultyNode
        {
            public Flugs Flug;
            public int Data;    // Group code
            public int Count;   // Count of chields in group tree
            public int Tree;    // Offset tree elements
            public int Left;
            public int Right;

            public override string ToString()
            {
                return "right: " + Right + ", left: " + Left;
            }
        }

        // 
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        struct ByteKeyMultyNode
        {
            public Flugs Flug;
            public int Data;    // Group code
            public int Count;   // Count of chields in group tree
            public int Tree;    // Offset tree elements
            public int Left;
            public int Right;

            public override string ToString()
            {
                return "right: " + Right + ", left: " + Left;
            }
        }

        // 
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        struct GroupDataNode
        {
            public Flugs Flug;
            public int Count;
            public int Data;

            public string ToString(byte* mem)
            {
                var val = "";
                var ptr = (int*)(mem + Data);

                for (int i = 0; i < Count && i < 50; ++i)
                {
                    val += ptr[i] + ", ";
                }

                if (Count > 50)
                {
                    val += ", and more " + (Count - 50) + " ... ";
                }

                return "count: " + Count + ", values: " + val;
            }

            public override string ToString()
            {
                return "count: " + Count;
            }
        }

        // Base struct for TreeNode and BlockNode
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        struct LinkNode
        {
            public Flugs Flug;
            public int Count;
            public int Left;
            public int Right;
        }

        // Base struct for TreeNode, BlockNode and DataNode
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        struct CountNode
        {
            public Flugs Flug;
            public int Count;
        }

        // Struct for index catalog
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        struct IndexLink
        {
            public int Next;
            public int Root;
            public int Length;
        }

        // Struct for any task
        //[StructLayout(LayoutKind.Sequential, Pack = 1)]
        //struct LinkedNode
        //{
        //    public int Next;
        //    public int Value;
        //}

        // Node type
        enum Flugs : byte
        {
            None    = 00,
            Tree    = 01,
            Data    = 02,
        }

        // Action for change nodes. Used by no locking change in multy thread algoritm.
        enum ChangeAction
        {
            None,

            InsertTreeNodeLeftLast,
            InsertTreeNodeLeftTree,
            InsertTreeNodeRightLast,
            InsertTreeNodeRightTree,

            InsertDataNodeFirst,
            InsertDataNodeLast,
            InsertDataNodeMiddle,

            RemoveTreeNodeOnLeftLast,
            RemoveTreeNodeOnRightLast,
            RemoveTreeNode,

            RemoveDataNode,
            RemoveDataNodeFirst,
            RemoveDataNodeMiddle,

            RemoveDataNodeLast,
            RemoveDataNodeTwoFirst,
            RemoveDataNodeTwoLast,
            RemoveDataNodeThree,
            RemoveDataNodeFour1,
            RemoveDataNodeFour2
        }

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //int GetCount(int pos)
        //{
        //    var ptr = (int*)(mem + pos);

        //    var c = 1;

        //    if (ptr->Left < 0)
        //    {
        //        c++;
        //    }
        //    else if (ptr->Left > 0)
        //    {
        //        c += GetPtr(ptr->Left)->Count;
        //    }

        //    if (ptr->Right < 0)
        //    {
        //        c++;
        //    }
        //    else if (ptr->Right > 0)
        //    {
        //        c += GetPtr(ptr->Right)->Count;
        //    }

        //    return c;
        //}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static int GetLevel(int val)
        {
            var ptr = (byte*)&val;

            if (ptr[3] > 0)
            {
                return level_3[ptr[3]];
            }
            else if (ptr[2] > 0)
            {
                return level_2[ptr[2]];
            }
            else if (ptr[1] > 0)
            {
                return level_1[ptr[1]];
            }
            else
            {
                return level_0[ptr[0]];
            }
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

        #endregion

        #region ' Test '
#if !DEBUG1

        public void PrintToDebug(int memoryKey)
        {
            var ptr = (int*)(mem + memoryKey);

            if (*ptr == 0)
            {
                Debug.WriteLine("Tree is empty");
            }
            else if (*ptr < 0)
            {
                Debug.WriteLine("Tree has only one element = " + *ptr);
            }
            else
            {
                var flug = (Flugs*)(mem + *ptr);

                PrintToDebug(flug, 0, "");
            }
        }
        
        void PrintToDebug(Flugs* flug, int lvl, string str2)
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

            // Tree
            if ((*flug & Flugs.Tree) == Flugs.Tree)
            {
                var cur = (TreeNode*)flug;

                Debug.WriteLine(str0 + *cur);

                if (cur->Left == 0)
                {
                    Debug.WriteLine(str1 + "|left is null");
                }
                else if (cur->Left < 0)
                {
                    Debug.WriteLine(str1 + "|left " + cur->Left);
                }
                else
                {
                    PrintToDebug((Flugs*)(mem + cur->Left), lvl + 1, str1 + "|left ");
                }

                if (cur->Right == 0)
                {
                    Debug.WriteLine(str1 + "|right is null");
                }
                else if (cur->Right < 0)
                {
                    Debug.WriteLine(str1 + "|right " + cur->Right);
                }
                else
                {
                    PrintToDebug((Flugs*)(mem + cur->Right), lvl + 1, str1 + "|right ");
                }
            }
            // Data
            else
            {
                var cur = (DataNode*)flug;

                Debug.WriteLine(str0 + "Data " + (*cur).ToString(mem));
            }
        }

        public void PrintGroupToDebug(int memoryKey)
        {
            var ptr = (int*)(mem + memoryKey);

            if (*ptr == 0)
            {
                Debug.WriteLine("Tree is empty");
            }
            else if (*ptr < 0)
            {
                Debug.WriteLine("Tree has only one element = " + *ptr);
            }
            else
            {
                PrintGroupToDebug(*ptr, 0, "");
            }
        }

        void PrintGroupToDebug(int pos, int lvl, string str2)
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

            var flug = (Flugs*)(mem + pos);

            // Tree
            if ((*flug & Flugs.Tree) == Flugs.Tree)
            {
                var cur = (GroupNode*)flug;

                Debug.WriteLine(str0 + "Group " + cur->Data + "(pos:" + pos + "; count:" + cur->Count + ") ");

                if (cur->Left == 0)
                {
                    Debug.WriteLine(str1 + "|left is null");
                }
                else if (cur->Left < 0)
                {
                    Debug.WriteLine(str1 + "|left " + cur->Left);
                }
                else
                {
                    PrintGroupToDebug(cur->Left, lvl + 1, str1 + "|left ");
                }

                if (cur->Right == 0)
                {
                    Debug.WriteLine(str1 + "|right is null");
                }
                else if (cur->Right < 0)
                {
                    Debug.WriteLine(str1 + "|right " + cur->Right);
                }
                else
                {
                    PrintGroupToDebug(cur->Right, lvl + 1, str1 + "|right ");
                }
            }
            // Data
            else
            {
                var cur = (GroupDataNode*)flug;

                Debug.WriteLine(str0 + "Data " + (*cur).ToString(mem));
            }
        }

        public void PrintMultyToDebug(int memoryKey)
        {
            var ptr = (int*)(mem + memoryKey);

            if (*ptr == 0)
            {
                Debug.WriteLine("Tree is empty");
            }
            else if (*ptr < 0)
            {
                Debug.WriteLine("Tree has only one element = " + *ptr);
            }
            else
            {
                PrintGroupToDebug(*ptr, 0, "");
            }
        }

        void PrintMultyToDebug(int pos, int lvl, string str2)
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

            var flug = (Flugs*)(mem + pos);

            // Tree
            if ((*flug & Flugs.Tree) == Flugs.Tree)
            {
                var cur = (GroupNode*)flug;

                Debug.WriteLine(str0 + "Group " + cur->Data + "(pos:" + pos + "; count:" + cur->Count + ") ");

                if (cur->Left == 0)
                {
                    Debug.WriteLine(str1 + "|left is null");
                }
                else if (cur->Left < 0)
                {
                    Debug.WriteLine(str1 + "|left " + cur->Left);
                }
                else
                {
                    PrintGroupToDebug(cur->Left, lvl + 1, str1 + "|left ");
                }

                if (cur->Right == 0)
                {
                    Debug.WriteLine(str1 + "|right is null");
                }
                else if (cur->Right < 0)
                {
                    Debug.WriteLine(str1 + "|right " + cur->Right);
                }
                else
                {
                    PrintGroupToDebug(cur->Right, lvl + 1, str1 + "|right ");
                }
            }
            // Data
            else
            {
                var cur = (GroupDataNode*)flug;

                Debug.WriteLine(str0 + "Data " + (*cur).ToString(mem));
            }
        }

        // Print index catalog to debug console
        public void PrintRegistredIndexesToDebug()
        {
            try
            {
                var count = (int*)(header + 4);
                var root = (int*)(header + 8);

                Debug.WriteLine("Begin print index catalog");
                Debug.WriteLine("Total count: " + *count);
                Debug.WriteLine("Root: " + *root);
                Debug.WriteLine("");

                if (*count == 0)
                {
                    return;
                }

                if (*root >= length || *root < 0)
                {
                    Debug.WriteLine("Wrong root item offest.");

                    return;
                }

                var cur = (IndexLink*)(mem + *root);

                for (int i = 0; i < *count; ++i)
                {
                    Debug.Write("Index: ");

                    var str = (char*)((byte*)cur + sizeof(IndexLink));

                    for (int j = 0; j < cur->Length && j < 512; ++j)
                    {
                        Debug.Write(str[j]);
                    }

                    Debug.WriteLine("");
                    Debug.WriteLine("Root ptr: " + cur->Root);

                    if (cur->Length > 512)
                    {
                        Debug.WriteLine("Wrong name length. Is more than 512.");
                    }

                    if (cur->Root >= length || cur->Root < 0)
                    {
                        Debug.WriteLine("Wrong root item offest.");
                    }
                    else
                    {
                        var tmp = (int*)(mem + cur->Root);

                        Debug.WriteLine("Root val: " + *tmp);
                    }

                    Debug.WriteLine("Next: " + cur->Next);

                    if (cur->Next >= length || cur->Next < 0 || (cur->Next == 0 && i < *count - 1))
                    {
                        Debug.WriteLine("Wrong next item offest.");

                        break;
                    }

                    Debug.WriteLine("");

                    cur = (IndexLink*)(mem + cur->Next);
                }
            }
            finally
            {
            }
        }

        // Print memory catalog to debug console
        public bool PrintMemoryToDebug()
        {
            var flug0 = false;
            var h = (int*)header;
            var hh = (int*)(header + 12);

            for (int i = 0; i < 18; i++)
            {
                var flug1 = false;
                var b = i + 4;

                Debug.Write(b + " bytes: ");

                var ptr = hh + i;

                if (*ptr > 0)
                {
                    var total = 0;

                    while (true)
                    {
                        total++;

                        var len = 0;

                        if (i < 17)
                        {
                            len = b;
                        }
                        else
                        {
                            len = ptr[1];
                        }

                        if (*ptr > *h)
                        {
                            Debug.Write("warning " + ptr[0] + ">" + *h + " len=" + len);

                            flug1 = true;

                            break;
                        }
                        else
                        {
                            Debug.Write(ptr[0] + ":" + len + " => ");
                        }

                        ptr = (int*)(mem + ptr[0]);

                        if (*ptr <= 0)
                        {
                            Debug.Write("Total: " + total);

                            break;
                        }
                    }

                    if (flug1)
                    {
                        Debug.WriteLine("Fail");
                    }
                    else
                    {
                        Debug.WriteLine("Done");
                    }
                }
                else
                {
                    Debug.WriteLine("No items");
                }

                flug0 |= flug1;
            }

            if (flug0)
            {
                Debug.WriteLine("Has errors!");
            }
            else
            {
                Debug.WriteLine("No errors");
            }

            return flug0;
        }

        // 
        public object TestFunction(TestAction action, params object[] args)
        {
            switch (action)
            {
                #region ' Core '
                
                case TestAction.GetNodeType:
                    {
                        if (args.Length != 1)
                        {
                            throw new ArgumentException("See enum TestAction coments.");
                        }

                        var key = (int)args[0];
                        var ptr = (Flugs*)(mem + key);

                        return (int)*ptr;
                    }

                case TestAction.SetRootOffset:
                    {
                        if (args.Length != 1)
                        {
                            throw new ArgumentException("See enum TestAction coments.");
                        }

                        var key = (int)args[0];
                        var len = (int*)header;

                        *len = key;

                        return null;
                    }

                case TestAction.ReadInt32:
                    {
                        if (args.Length != 1)
                        {
                            throw new ArgumentException("See enum TestAction coments.");
                        }

                        var key = (int)args[0];
                        var ptr = (int*)(mem + key);

                        return *ptr;
                    }

                case TestAction.WriteInt32:
                    {
                        if (args.Length != 2)
                        {
                            throw new ArgumentException("See enum TestAction coments.");
                        }

                        var key = (int)args[0];
                        var val = (int)args[1];
                        var ptr = (int*)(mem + key);

                        *ptr = val;

                        return null;
                    }

                case TestAction.MemoryAlloc:
                    {
                        if (args.Length != 1)
                        {
                            throw new ArgumentException("See enum TestAction coments.");
                        }

                        var size = (int)args[0];
                        var pos = AllocMemory(size);

                        return pos;
                    }

                case TestAction.MemoryRelease:
                    {
                        if (args.Length != 2)
                        {
                            throw new ArgumentException("See enum TestAction coments.");
                        }

                        var pos = (int)args[0];
                        var size = (int)args[1];

                        ReleaseMemory(pos, size);

                        return null;
                    } 
                #endregion

                #region ' DataNode '

                case TestAction.DataNodeCreate:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var arr = (int[])args[0];

                    var tmp = CreateDataNode(arr.Length);
                    var node = (DataNode*)(mem + tmp);

                    if (node->Flug != Flugs.Data)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    node->Min = arr[0];
                    node->Max = arr[arr.Length - 1];

                    var ptr = (int*)(mem + node->Data);

                    for (int i = 0; i < node->Count; ++i)
                    {
                        ptr[i] = arr[i];
                    }

                    return tmp;
                }
                case TestAction.DataNodeGetCount:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ptr = (DataNode*)(mem + key);

                    if (ptr->Flug != Flugs.Data)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->Count;
                }
                case TestAction.DataNodeGetMin:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ptr = (DataNode*)(mem + key);

                    if (ptr->Flug != Flugs.Data)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->Min;
                }
                case TestAction.DataNodeGetMax:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ptr = (DataNode*)(mem + key);

                    if (ptr->Flug != Flugs.Data)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->Max;
                }
                case TestAction.DataNodeGetValue:
                {
                    if (args.Length != 2)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ind = (int)args[1];
                    var ptr = (DataNode*)(mem + key);

                    if (ptr->Flug != Flugs.Data)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    if (ind >= ptr->Count)
                    {
                        throw new IndexOutOfRangeException();
                    }

                    var dat = (int*)(mem + ptr->Data);

                    return dat[ind];
                }

                #endregion

                #region ' TreeNode '

                case TestAction.TreeNodeCreate:
                {
                    if (args.Length != 4)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var val = (int)args[0];
                    var left = (int)args[1];
                    var right = (int)args[2];
                    var count = (int)args[3];
                    
                    var ret = CreateTreeNode(val, left, right, count);
                    var ptr = (TreeNode*)(mem + ret);

                    if (ptr->Flug != Flugs.Tree)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ret;
                }

                case TestAction.TreeNodeGetCount:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ptr = (TreeNode*)(mem + key);

                    if (ptr->Flug != Flugs.Tree)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->Count;
                }


                case TestAction.TreeNodeSetCount:
                {
                    if (args.Length != 2)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var count = (int)args[1];
                    var ptr = (TreeNode*)(mem + key);

                    if (ptr->Flug != Flugs.Tree)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    ptr->Count = count;

                    return null;
                }

                case TestAction.TreeNodeGetValue:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ptr = (TreeNode*)(mem + key);

                    if (ptr->Flug != Flugs.Tree)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->Data;
                }
                    
                case TestAction.TreeNodeSetValue:
                {
                    if (args.Length != 2)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var val = (int)args[1];
                    var ptr = (TreeNode*)(mem + key);

                    if (ptr->Flug != Flugs.Tree)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    ptr->Data = val;

                    return null;
                }

                case TestAction.TreeNodeGetRight:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ptr = (TreeNode*)(mem + key);

                    if (ptr->Flug != Flugs.Tree)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->Right;
                }

                case TestAction.TreeNodeGetLeft:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ptr = (TreeNode*)(mem + key);

                    if (ptr->Flug != Flugs.Tree)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->Left;
                }

                #endregion

                #region ' GroupTreeNode '

                case TestAction.GroupTreeNodeCreate:
                {
                    if (args.Length != 4)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var val = (int)args[0];
                    var left = (int)args[1];
                    var right = (int)args[2];
                    var count = (int)args[3];

                    var ret = CreateGroupNode(val, left, right, count);
                    var ptr = (GroupNode*)(mem + ret);

                    if (ptr->Flug != Flugs.Tree)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ret;
                }

                case TestAction.GroupTreeNodeGetValue:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ptr = (GroupNode*)(mem + key);

                    if (ptr->Flug != Flugs.Tree)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->Data;
                }

                case TestAction.GroupTreeNodeSetValue:
                {
                    if (args.Length != 2)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var val = (int)args[1];
                    var ptr = (GroupNode*)(mem + key);

                    if (ptr->Flug != Flugs.Tree)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    ptr->Data = val;

                    return null;
                }

                case TestAction.GroupTreeNodeGetRight:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ptr = (GroupNode*)(mem + key);

                    if (ptr->Flug != Flugs.Tree)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->Right;
                }

                case TestAction.GroupTreeNodeGetLeft:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ptr = (GroupNode*)(mem + key);

                    if (ptr->Flug != Flugs.Tree)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->Left;
                }

                case TestAction.GroupTreeNodeGetCount:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ptr = (GroupNode*)(mem + key);

                    if (ptr->Flug != Flugs.Tree)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->Count;
                }


                case TestAction.GroupTreeNodeSetCount:
                {
                    if (args.Length != 2)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var count = (int)args[1];
                    var ptr = (GroupNode*)(mem + key);

                    if (ptr->Flug != Flugs.Tree)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    ptr->Count = count;

                    return null;
                }

                #endregion

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
            /// Params: [0](int)MemoryOffset, [ret](int)Flugs
            /// </summary>
            GetNodeType,

            /// <summary>
            /// Params: [0](int[])SortedDataArray, [ret](int)MemoryOffset
            /// </summary>
            DataNodeCreate,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Count
            /// </summary>
            DataNodeGetCount,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Min
            /// </summary>
            DataNodeGetMin,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Max
            /// </summary>
            DataNodeGetMax,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [1](int)Index, [ret](int)Value
            /// </summary>
            DataNodeGetValue,

            /// <summary>
            /// Params: [0](int)Val, [1](int)Left, [2](int)Right, [3](int)Count, [ret](int)MemoryOffset
            /// </summary>
            TreeNodeCreate,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Count
            /// </summary>
            TreeNodeGetCount,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [1](int)NewCount, [ret]null
            /// </summary>
            TreeNodeSetCount,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Value
            /// </summary>
            TreeNodeGetValue,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [1](int)NewValue, [ret]null
            /// </summary>
            TreeNodeSetValue,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Left
            /// </summary>
            TreeNodeGetLeft,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Right
            /// </summary>
            TreeNodeGetRight,

            /// <summary>
            /// Params: [0](int)Val, [1](int)Left, [2](int)Right, [3](int)Count, [ret](int)MemoryOffset
            /// </summary>
            GroupTreeNodeCreate,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Count
            /// </summary>
            GroupTreeNodeGetCount,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [1](int)NewCount, [ret]null
            /// </summary>
            GroupTreeNodeSetCount,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Value
            /// </summary>
            GroupTreeNodeGetValue,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [1](int)NewValue, [ret]null
            /// </summary>
            GroupTreeNodeSetValue,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Left
            /// </summary>
            GroupTreeNodeGetLeft,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Right
            /// </summary>
            GroupTreeNodeGetRight,

            /// <summary>
            /// Params: [0](int)NewRootOffset, [ret]null
            /// </summary>
            SetRootOffset,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Value
            /// </summary>
            ReadInt32,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [1](int)Value, [ret]null
            /// </summary>
            WriteInt32,

            /// <summary>
            /// Params: [0](int)Size, [ret](int)MemoryOffset
            /// </summary>
            MemoryAlloc,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [1](int)Size, [ret]null
            /// </summary>
            MemoryRelease,
        }

#endif
        #endregion

        /// <summary>
        /// Clear all indexes and root index tree
        /// </summary>
        public void Clear()
        {
            for (int i = 0; i < headerSize; ++i)
            {
                header[i] = 0;
            }
        }
    }
}

// RU: Описание класса
//  
// 1. memoryKey это смещение относительно указателя mem. По этому смещению содержится переменная типа int. Если переменная
// меньше 0 значит это код элемента. Если больше нуля это смещение указывающее на начало дерева. Это может быть дерево 
// элементов или дерево групп с типами TreeNode, DataNode, GroupTreeNode, GroupDataNode. 
// 
// 
// 