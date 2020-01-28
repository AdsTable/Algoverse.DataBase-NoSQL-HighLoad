using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MilkyCode.DataBase
{
    internal unsafe class Index : IDisposable
    {
        const int headerSize    = 100;
        const int maxLevel      = 30;

        bool disposed;
        long length;
        int capacity;
        FileStream fs;
        MemoryMappedFile file;
        MemoryMappedViewAccessor mapping;
        ValueLockRW locker = new ValueLockRW();
        string key;
        byte* header;
        byte* mem;
        
        // Constructor
        public Index(string fullPath)
        {
            capacity = 1024 * 1024 * 40;
            key = Path.GetFileName(fullPath);

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
        }

        // Count
        public int Count(int memoryKey)
        {
            try
            {
                locker.ReadLock();

                return ((CountNode*)(mem + memoryKey))->Count;
            }
            finally
            {
                locker.Unlock();
            }
        }
        
        // Return element by index
        public int GetByIndex(int index, int memoryKey)
        {
            try
            {
                locker.ReadLock();

                var flug = (Flugs*)(mem + memoryKey);

                if (index >= ((CountNode*)flug)->Count)
                {
                    throw new IndexOutOfRangeException();
                }

                var ind = index;
            
                while (true)
                {
                    #region ' Block '

                    if ((*flug & Flugs.Block) == Flugs.Block)
                    {
                        var cur = (BlockNode*)flug;
                        var l_count = 0;

                        //if (cur->left == 0)
                        //{
                            
                        //}
                        //else 
                        if (cur->Left < 0)
                        {
                            if (ind == 0)
                            {
                                return -cur->Left;
                            }

                            l_count = 1;
                        }
                        else if (cur->Left > 0)
                        {
                            l_count = ((CountNode*)(mem + cur->Left))->Count;
                        }

                        // going to left node
                        if (ind < l_count)
                        {
                            flug = (Flugs*)(mem + cur->Left);
                        }
                        // going to right node
                        else
                        {
                            ind -= l_count;

                            if (ind < 0)
                            {
                                ErrorReport(2);

                                return 0;
                            }

                            // exception
                            if (cur->Right == 0)
                            {
                                ErrorReport(3);

                                return 0;
                            }
                            else if (cur->Right < 0)
                            {
                                if (ind == 0)
                                {
                                    return -cur->Right;
                                }

                                ErrorReport(4);

                                return 0;
                            }

                            flug = (Flugs*)(mem + cur->Right);
                        }
                    }

                    #endregion

                    #region ' Tree '

                    else if ((*flug & Flugs.Tree) == Flugs.Tree)
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

                            l_count = 0;
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
                            else 
                            if (ind < 0)
                            {
                                ErrorReport(6);

                                return 0;
                            }

                            // exception
                            if (cur->Right == 0)
                            {
                                ErrorReport(7);

                                return 0;
                            }
                            else if (cur->Right < 0)
                            {
                                if (ind == 1)
                                {
                                    return -cur->Right;
                                }

                                ErrorReport(8);

                                return 0;
                            }

                            ind--;
                            flug = (Flugs*)(mem + cur->Right);
                        }
                    }

                    #endregion

                    #region ' Data '

                    else if ((*flug & Flugs.Data) == Flugs.Data)
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
                    }

                    #endregion

                    #region ' Other '

                    else
                    {
                        ErrorReport(111);

                        return 0;
                    }

                    #endregion

                }
            }
            finally
            {
                locker.Unlock();
            }
        }
        
        // Inserting new element to index
        public void Insert<T>(T obj, int memoryKey, IRecordsComparer<T> comparer, bool isUnique) where T : Record
        {
            try
            {
                locker.WriteLock();

                #region ' Prepare '

                var pos = memoryKey;
                var flug = (Flugs*)(mem + pos);
                var root = (BlockNode*)flug;

                if (root->Count == 0)
                {
                    root->Count = 1;
                    root->Left = -obj.Code;
                    root->MaxOnLeft = obj.Code;

                    return;
                }

                var stack = stackalloc int[100];
                var lvl = 0;
                var ext_data = 0;
                
                var action = ChangeAction.None;

                #endregion

                #region ' Find place to insert '

                while (true)
                {
                    stack[lvl++] = pos;
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                            #region ' Block '

                        case Flugs.Block:
                        {
                            var cur = (BlockNode*)flug;
                            var cmp = comparer.CompareRecords(obj, cur->MaxOnLeft);

                            // already exist
                            if (cmp == 0 && isUnique)
                            {
                                return;
                            }
                                    // left
                            else if (cmp <= 0)
                            {
                                // insert last node to left
                                if (cur->Left == 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertBlockNodeLeftLast;

                                    goto insert;
                                }
                                        // insert tree node to left
                                else if (cur->Left < 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertBlockNodeLeftTree;

                                    goto insert;
                                }
                                        // looking left
                                else
                                {
                                    //flug = (Flugs*)(mem + cur->left);
                                    pos = cur->Left;

                                    continue;
                                }
                            }
                                    // right
                            else
                            {
                                // insert last node to right
                                if (cur->Right == 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertBlockNodeRightLast;

                                    goto insert;
                                }
                                        // insert tree node to right
                                else if (cur->Right < 0)
                                {
                                    // Lock
                                    action = ChangeAction.InsertBlockNodeRightTree;

                                    goto insert;
                                }
                                        // looking right
                                else
                                {
                                    //flug = (Flugs*) (mem + cur->right);
                                    pos = cur->Right;

                                    continue;
                                }
                            }
                        }

                            #endregion

                            #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (TreeNode*) flug;
                            var cmp = comparer.CompareRecords(obj, cur->Data);

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
                            var cur = (DataNode*) flug;
                            var dat = (int*) (mem + cur->Data);

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
                            Debugger.Break();

                            break;
                        }

                            #endregion
                    }
                }

                #endregion

                #region ' Correct count '

                insert:

                for (int i = 0; i < lvl; ++i)
                {
                    flug = (Flugs*)(mem + stack[i]);

                    switch (*flug)
                    {
                            #region ' Block '

                        case Flugs.Block:
                        {
                            var cur = (BlockNode*)flug;

                            cur->Count++;

                            if (cur->Left == (byte*)stack[i + 1] - mem)
                            {
                                var cmp = comparer.CompareRecords(obj, cur->MaxOnLeft);

                                if (cmp > 0)
                                {
                                    cur->MaxOnLeft = obj.Code;
                                }
                            }

                            break;
                        }

                            #endregion

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
                    case ChangeAction.InsertBlockNodeLeftLast:
                    {
                        var cur = (BlockNode*)flug;
                        cur->Left = -obj.Code;

                        var cmp = comparer.CompareRecords(obj, cur->MaxOnLeft);

                        if (cmp > 0)
                        {
                            cur->MaxOnLeft = obj.Code;
                        }

                        return;
                    }
                    case ChangeAction.InsertBlockNodeLeftTree:
                    {
                        var cur = (BlockNode*)flug;
                        var cmp = comparer.CompareRecords(obj, -cur->Left);

                        if (cmp <= 0)
                        {
                            cur->Left = CreateTreeNode(-cur->Left, -obj.Code, 0);
                        }
                        else
                        {
                            cur->Left = CreateTreeNode(-cur->Left, 0, -obj.Code);
                        }

                        // compute new max value
                        cmp = comparer.CompareRecords(obj, cur->MaxOnLeft);

                        if (cmp > 0)
                        {
                            cur->MaxOnLeft = obj.Code;
                        }

                        goto exit;
                    }
                    case ChangeAction.InsertBlockNodeRightLast:
                    {
                        var cur = (BlockNode*)flug;
                        cur->Right = -obj.Code;

                        return;
                    }
                    case ChangeAction.InsertBlockNodeRightTree:
                    {
                        var cur = (BlockNode*)flug;
                        var cmp = comparer.CompareRecords(obj, -cur->Right);

                        if (cmp <= 0)
                        {
                            cur->Right = CreateTreeNode(-cur->Right, -obj.Code, 0);
                        }
                        else
                        {
                            cur->Right = CreateTreeNode(-cur->Right, 0, -obj.Code);
                        }

                        goto exit;
                    }
                    case ChangeAction.InsertTreeNodeLeftLast:
                    {
                        var cur = (TreeNode*)flug;

                        cur->Left = -obj.Code;

                        return;
                    }
                    case ChangeAction.InsertTreeNodeLeftTree:
                    {
                        var cur = (TreeNode*)flug;
                        var cmp = comparer.CompareRecords(obj, -cur->Left);

                        if (cmp <= 0)
                        {
                            cur->Left = CreateTreeNode(-cur->Left, -obj.Code, 0);
                        }
                        else
                        {
                            cur->Left = CreateTreeNode(-cur->Left, 0, -obj.Code);
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
                        var cmp = comparer.CompareRecords(obj, -cur->Right);

                        if (cmp <= 0)
                        {
                            cur->Right = CreateTreeNode(-cur->Right, -obj.Code, 0);
                        }
                        else
                        {
                            cur->Right = CreateTreeNode(-cur->Right, 0, -obj.Code);
                        }

                        goto exit;
                    }
                    case ChangeAction.InsertDataNodeFirst:
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

                        var adr = CreateBlockNode(-obj.Code, *par_ptr);

                        *par_ptr = adr;

                        var block = (BlockNode*)(mem + adr);

                        block->Count = cur->Count + 1;
                        block->MaxOnLeft = obj.Code;

                        goto exit;
                    }
                    case ChangeAction.InsertDataNodeLast:
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

                        var adr = CreateBlockNode(*par_ptr, -obj.Code);

                        *par_ptr = adr;

                        var block = (BlockNode*)(mem + adr);

                        block->Count = cur->Count + 1;
                        block->MaxOnLeft = cur->Max;

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

                        var adr_left = CreateBlockNode(*par_ptr, -obj.Code);
                        var adr_right = CreateDataNode();
                        var adr_root = CreateBlockNode(adr_left, adr_right);

                        *par_ptr = adr_root;

                        var block_root = (BlockNode*)(mem + adr_root);

                        block_root->Count = cur->Count + 1;
                        block_root->MaxOnLeft = obj.Code;

                        var block_left = (BlockNode*)(mem + adr_left);

                        block_left->Count = last + 1;
                        block_left->MaxOnLeft = dat[last - 1];

                        var data_new = (DataNode*)(mem + adr_right);

                        data_new->Count = cur->Count - last;
                        data_new->Min = dat[last];
                        data_new->Max = cur->Max;
                        data_new->Data = cur->Data + last * sizeof(int);

                        cur->Count = last;
                        cur->Max = dat[last - 1];

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
                    var ind = 0;

                    for (int i = lvl - 1; i > 0; i--)
                    {
                        var itm = (Flugs*)(mem + stack[i]);

                        ind = i;

                        if ((*itm & Flugs.Data) != Flugs.Data)
                        {
                            var cur = (DataNode*)itm;

                            if (cur->Count > 1000)
                            {
                                ind++;

                                break;
                            }
                        }
                    }

                    SmartOptimization(stack[ind - 1], stack[ind]);
                }

                #endregion
            }
            finally
            {
                locker.Unlock();
            }
        }

        // Remove element by code from index
        public void Delete<T>(T obj, int memoryKey, IRecordsComparer<T> comparer) where T : Record
        {
            try
            {
                locker.WriteLock();

                #region ' Prepare '

                var pos = memoryKey;
                var flug = (Flugs*)(mem + pos);
                var root = (CountNode*)flug;

                if (root->Count == 0)
                {
                    return;
                }

                var stack = stackalloc int[100];
                var stack_pos = 0;
                var action      = ChangeAction.None;
                var ext_data    = 0;

                #endregion

                #region ' Find place to remove '

                while (true)
                {
                    stack[stack_pos++] = pos;
                    flug = (Flugs*)(mem + pos);

                    switch (*flug)
                    {
                            #region ' Block '

                        case Flugs.Block:
                        {
                            var cur = (BlockNode*) flug;
                            var cmp = comparer.CompareRecords(obj, cur->MaxOnLeft);

                            // left
                            if (cmp <= 0)
                            {
                                // looking left
                                if (cur->Left > 0)
                                {
                                    flug = (Flugs*) (mem + cur->Left);

                                    continue;
                                }
                                        // remove last node
                                else if (cur->Left < 0)
                                {
                                    // Lock
                                    action = ChangeAction.RemoveBlockNodeOnLeftLast;

                                    goto exit_find;
                                }

                                return;
                            }
                                    // right
                            else
                            {
                                // looking right
                                if (cur->Right > 0)
                                {
                                    flug = (Flugs*) (mem + cur->Right);

                                    continue;
                                }
                                        // remove last node
                                else if (cur->Right < 0)
                                {
                                    // Lock
                                    action = ChangeAction.RemoveBlockNodeOnRightLast;

                                    goto exit_find;
                                }

                                return;
                            }
                        }

                            #endregion

                            #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (TreeNode*) flug;
                            var cmp = comparer.CompareRecords(obj, cur->Data);

                            // left
                            if (cmp < 0)
                            {
                                if (cur->Left > 0)
                                {
                                    flug = (Flugs*) (mem + cur->Left);

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
                                    flug = (Flugs*) (mem + cur->Right);

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

                                break;
                            }
                        }

                            #endregion

                            #region ' Data '

                        case Flugs.Data:
                        {
                            var cur = (DataNode*) flug;
                            var dat = (int*) (mem + cur->Data);

                            var first = 0;
                            var last = cur->Count - 1;
                            var cmp = comparer.CompareRecords(obj, dat[first]);

                            // not found
                            if (cmp < 0 || comparer.CompareRecords(obj, dat[last]) > 0)
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
                                        // remove first node
                                else
                                {
                                    // Lock
                                    action = ChangeAction.RemoveDataNodeFirst;

                                    goto exit_find;
                                }
                            }
                                    // last, remove data node
                            else if (cur->Count == 2)
                            {
                                // Lock
                                action = ChangeAction.RemoveDataNodeOne;

                                goto exit_find;
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

                exit_find:;

                #endregion

                #region ' Correct count '
                
                var max_list_first = 0;
                var max_list_last = 0;
                var max_new_val = 0;
                int tmp;

                for (int i = 0; i < stack_pos; ++i)
                {
                    flug = (Flugs*)(mem + stack[i]);

                    switch (*flug)
                    {
                            #region ' Block '

                        case Flugs.Block:
                        {
                            var cur = (BlockNode*) flug;

                            cur->Count--;

                            var cmp = comparer.CompareRecords(obj, cur->MaxOnLeft);

                            // this node need to be adjusted maxOnLeft
                            if (cmp == 0)
                            {
                                tmp = CreateFreeSpace(sizeof(LinkedNode));

                                if (max_list_first == 0)
                                {
                                    max_list_first = tmp;
                                    max_list_last = tmp;

                                    var ptr = (LinkedNode*) (mem + tmp);

                                    ptr->Value = (int) ((byte*) cur - mem);
                                }
                                else
                                {
                                    var ptr = (LinkedNode*) (mem + max_list_last);

                                    ptr->Next = tmp;

                                    ptr = (LinkedNode*) (mem + tmp);

                                    ptr->Value = (int) ((byte*) cur - mem);

                                    max_list_last = tmp;
                                }
                            }

                            break;
                        }

                            #endregion

                            #region ' Tree '

                        case Flugs.Tree:
                        {
                            var cur = (TreeNode*) flug;

                            cur->Count--;

                            break;
                        }

                            #endregion

                            #region ' Data '

                        case Flugs.Data:
                        {
                        //    var cur = (DataNode*) flug;
                        //    var dat = (int*) (mem + cur->Data);
                        //    var par = (LinkNode*) stack[stack_pos - 2];
                        //    //int* par_ptr;
                        //    var isLeft = false;

                        //    // getting link
                        //    if (par->Left == (byte*) cur - mem)
                        //    {
                        //        par_ptr = &par->Left;
                        //        isLeft = true;
                        //    }
                        //    else
                        //    {
                        //        par_ptr = &par->Right;
                        //    }

                        //    int last = cur->Count - 1;

                        //    switch (action)
                        //    {
                        //        case ChangeAction.RemoveDataNodeFirst:
                        //        {
                        //            cur->Min = dat[1];
                        //            last = 0;

                        //            break;
                        //        }
                        //        case ChangeAction.RemoveDataNodeMiddle:
                        //        {
                        //            last = ext_data;

                        //            break;
                        //        }
                        //        default:
                        //        {
                        //            return;
                        //        }
                        //    }

                        //    cur->Count--;

                        //    // left shift
                        //    if (cur->Count - last <= 500)
                        //    {
                        //        for (int n = last; n < cur->Count; ++n)
                        //        {
                        //            dat[n] = dat[n + 1];
                        //        }

                        //        // new max
                        //        if (cur->Count == last)
                        //        {
                        //            cur->Max = dat[last - 1];
                        //            max_new_val = cur->Max;
                        //        }

                        //        ReleaseMemory((byte*) (dat + cur->Count), sizeof(int));

                        //        break;
                        //    }

                        //    // right shift
                        //    if (last <= 500)
                        //    {
                        //        for (int n = last; n > 0; --n)
                        //        {
                        //            dat[n] = dat[n - 1];
                        //        }

                        //        cur->Data += 4;

                        //        ReleaseMemory((byte*) dat, sizeof(int));

                        //        break;
                        //    }

                        //    var adr_right = CreateDataNode();
                        //    var adr_root = CreateBlockNode((int) ((byte*) cur - mem), adr_right);

                        //    *par_ptr = adr_root;

                        //    var block_root = (BlockNode*) (mem + adr_root);

                        //    block_root->Count = cur->Count;
                        //    block_root->MaxOnLeft = dat[last - 1];

                        //    var data_new = (DataNode*) (mem + adr_right);

                        //    data_new->Count = cur->Count - last;
                        //    data_new->Min = dat[last + 1];
                        //    data_new->Max = cur->Max;
                        //    data_new->Data = cur->Data + (last + 1) * sizeof(int);

                        //    cur->Count = last;
                        //    cur->Max = dat[last - 1];

                        //    ReleaseMemory((byte*) (dat + last), sizeof(int));

                        //    return;
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
                var isLeft = false;
                var par = (LinkNode*)stack[stack_pos - 2];

                pos = stack[stack_pos - 1];

                // cur node is root
                if (stack_pos == 1)
                {
                    int tmp_ptr = 0;

                    par_ptr = &tmp_ptr;
                }
                else
                {
                    if (par->Right == pos)
                    {
                        par_ptr = &par->Right;
                    }
                    // cur node is left
                    else if (par->Left == pos)
                    {
                        par_ptr = &par->Left;

                        isLeft = true;
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
                    case ChangeAction.RemoveBlockNodeOnLeftLast:
                    {
                        var cur = (BlockNode*)(mem + stack[pos]);

                        // remove current block node
                        if (stack_pos == 1)
                        {
                            // compute new max value
                            if (max_list_first > 0)
                            {
                                // max in parrent node because here cur->right always 0 
                                switch (par->flugs)
                                {
                                    case Flugs.Block:
                                    {
                                        var ptr = (BlockNode*)par;

                                        max_new_val = ptr->MaxOnLeft;

                                        break;
                                    }
                                    case Flugs.Tree:
                                    {
                                        var ptr = (TreeNode*)par;

                                        max_new_val = ptr->Data;

                                        break;
                                    }
                                    default:
                                    {
                                        ErrorReport(55);

                                        return;
                                    }
                                }
                            }

                            *par_ptr = cur->Right;

                            RemoveBlockNode(cur);
                        }
                        else
                        {
                            cur->Left = 0;
                            cur->MaxOnLeft = 0;
                        }

                        break;
                    }
                    case ChangeAction.RemoveBlockNodeOnRightLast:
                    {
                        var cur = (BlockNode*)(mem + stack[pos]);

                        // remove current block node
                        if (stack_pos == 1)
                        {
                            // compute new max value
                            if (max_list_first > 0)
                            {
                                if (cur->Left == 0)
                                {
                                    max_new_val = FindMax((Flugs*)par);
                                }
                                else
                                {
                                    max_new_val = cur->MaxOnLeft;
                                }
                            }

                            *par_ptr = cur->Left;

                            RemoveBlockNode(cur);
                        }
                        else
                        {
                            cur->Right = 0;
                        }

                        break;
                    }
                    case ChangeAction.RemoveTreeNodeOnLeftLast:
                    {
                        var cur = (TreeNode*)(mem + stack[pos]);

                        cur->Left = 0;

                        max_new_val = cur->Data;

                        // remove this node
                        if (cur->Right == 0)
                        {
                            *par_ptr = -cur->Data;

                            ReleaseMemory((byte*)cur, sizeof(TreeNode));
                        }

                        break;
                    }
                    case ChangeAction.RemoveTreeNodeOnRightLast:
                    {
                        var cur = (TreeNode*)(mem + stack[pos]);

                        cur->Right = 0;

                        max_new_val = cur->Data;

                        if (cur->Left == 0)
                        {
                            *par_ptr = -cur->Data;

                            ReleaseMemory((byte*)cur, sizeof(TreeNode));
                        }

                        break;
                    }
                    case ChangeAction.RemoveTreeNode:
                    {
                        var cur = (TreeNode*)(mem + stack[pos]);

                        // find new max
                        if (max_list_first > 0)
                        {
                            RemoveTreeNode(par_ptr, cur, out max_new_val);
                        }
                        else
                        {
                            RemoveTreeNode(par_ptr, cur);
                        }

                        break;
                    }
                    case ChangeAction.RemoveDataNode:
                    {
                        var cur = (DataNode*)(mem + stack[pos]);
                        *par_ptr = 0;

                        if (isLeft)
                        {
                            max_new_val = 0;
                        }
                        else
                        {
                            max_new_val = FindMax((Flugs*)par);
                        }

                        RemoveDataNode(cur);

                        goto exit;
                    }
                    case ChangeAction.RemoveDataNodeOne:
                    {
                        var cur = (DataNode*)(mem + stack[pos]);
                        var dat = (int*) (mem + cur->Data);

                        // remove first
                        if (comparer.CompareRecords(obj, dat[0]) == 0)
                        {
                            *par_ptr = -dat[1];
                        }
                        // remove last
                        if (comparer.CompareRecords(obj, dat[1]) == 0)
                        {
                            *par_ptr = -dat[0];
                        }

                        RemoveDataNode(cur);

                        goto exit;
                    }
                    default:
                    {
                        return;
                    }
                }

                #endregion

                #region ' Correct max on left '

                tmp = max_list_first;

                // Writing new max
                while (tmp > 0)
                {
                    var ptr = (LinkedNode*)(mem + tmp);
                    var cur = (BlockNode*)(mem + ptr->Value);

                    cur->MaxOnLeft = max_new_val;

                    tmp = ptr->Next;

                    ReleaseMemory((byte*)ptr, sizeof(LinkedNode));
                }

                #endregion
            }
            finally
            {
                locker.Unlock();
            }
        }

        // Check containce element by code
        public bool Contains<T>(T obj, int memoryKey, IRecordsComparer<T> comparer) where T : Record
        {
            try
            {
                locker.ReadLock();

                var flug = (Flugs*)(mem + memoryKey);

                while (true)
                {
                    #region ' Block '

                    if ((*flug & Flugs.Block) == Flugs.Block)
                    {
                        var cur = (BlockNode*)flug;



                        return true;
                    }

                    #endregion

                    #region ' Tree '

                    else if ((*flug & Flugs.Tree) == Flugs.Tree)
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
                locker.Unlock();
            }
        }

        // Register index
        public void RegisterIndex<T>(IDataIndex<T> index) where T : Record
        {
            try
            {
                locker.WriteLock();

                var count   = (int*)(header + 4);
                var root    = (int*)(header + 8);

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
                        var str = (char*)((byte*)cur + sizeof(IndexLink));

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
                    ;

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
                locker.Unlock();
            }
        }

        // Find code
        public int Find<T>(T obj, int memoryKey, IKeyComparer<T> comparer) where T : IComparable<T>
        {
            try
            {
                locker.ReadLock();

                var flug = (Flugs*)(mem + memoryKey);

                while (true)
                {
                    #region ' Block '

                    if ((*flug & Flugs.Block) == Flugs.Block)
                    {
                        var cur = (BlockNode*)flug;
                        var cmp = comparer.Compare(obj, cur->MaxOnLeft);

                        if (cmp == 0)
                        {
                            return cur->MaxOnLeft;
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

                    #region ' Tree '

                    else if ((*flug & Flugs.Tree) == Flugs.Tree)
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
                locker.Unlock();
            }
        }

        #region ' Core '

        // Creating three node
        private int CreateTreeNode(int val, int left, int right)
        {
            var len = (int*)header;
            var ret = *len;

            *len += sizeof(TreeNode);

            CheckFileSize();

            var ptr = (TreeNode*)(mem + ret);

            ptr->flugs = Flugs.Tree;
            ptr->Data = val;
            ptr->Left = left;
            ptr->Right = right;
            ptr->Count = 2;

            return ret;
        }

        // Creating data node
        int CreateDataNode(int count)
        {
            var len = (int*)header;
            var ret = *len;

            *len += count * 4 + sizeof(DataNode);

            CheckFileSize();

            var ptr = (DataNode*) (mem + ret);

            ptr->flugs = Flugs.Data;
            ptr->Count = count;
            ptr->Min = int.MaxValue;
            ptr->Max = 0;
            ptr->Data = ret + sizeof (DataNode);

            return ret;
        }

        // Creating data node
        int CreateDataNode()
        {
            var len = (int*)header;
            var ret = *len;

            *len += sizeof (DataNode);

            CheckFileSize();

            var ptr = (DataNode*) (mem + ret);

            ptr->flugs = Flugs.Data;
            ptr->Count = 0;
            ptr->Min = int.MaxValue;
            ptr->Max = 0;
            ptr->Data = 0;

            return ret;
        }

        // Creating block node
        int CreateBlockNode(int left, int right)
        {
            var len = (int*)header;
            var ret = *len;

            *len += sizeof (BlockNode);

            CheckFileSize();

            var ptr = (BlockNode*)(mem + ret);

            ptr->flugs      = Flugs.Block;
            ptr->Count      = 0;
            ptr->Left       = left;
            ptr->Right      = right;
            ptr->MaxOnLeft  = 0;

            return ret;
        }

        // Creating group node
        int CreateGroupNode(int left, int right)
        {
            var len = (int*)header;
            var ret = *len;

            *len += sizeof(GroupNode);

            CheckFileSize();

            var ptr = (GroupNode*)(mem + ret);

            ptr->Left = left;
            ptr->Right = right;
            ptr->Data = CreateBlockNode(0, 0);

            return ret;
        }

        // Creating free space
        int CreateFreeSpace(int count)
        {
            var len = (int*)header;
            var ret = *len;

            *len += count;

            CheckFileSize();

            return ret;
        }

        // Creating index link
        int CreateIndexLink<T>(IDataIndex<T> index) where T : Record
        {
            var len = (int*)header;
            var ret = *len;

            *len += sizeof(IndexLink) + index.Name.Length * 2;

            CheckFileSize();

            var ptr = (IndexLink*)(mem + ret);

            ptr->Length = index.Name.Length;
            ptr->Root = CreateBlockNode(0, 0);
            ptr->Next = 0;

            var str = (char*)(mem + ret + sizeof(IndexLink));

            for (int i = 0; i < index.Name.Length; ++i)
            {
                str[i] = index.Name[i];
            }

            return ret;
        }

        // Проверка размера файла
        void CheckFileSize()
        {
            //if (len >= length)
            //{
            //    lock (fs)
            //    {
            
            var len = (int*)header;

            if (*len >= length)
            {
                fs.SetLength(length * 2);

                InitFile();
            }
                //}
            //}
        }

        // Optimization of data structure
        void SmartOptimization(int par_pos, int cur_pos)
        {
            var par     = (LinkNode*)(mem + par_pos); 
            var cur     = (Flugs*)(mem + cur_pos);
            var count   = ((CountNode*)cur)->Count;

            var tmp_adr = 0;
            bool isParLeft;

            if (par->Left + mem == cur)
            {
                tmp_adr = CreateDataNode(count);

                isParLeft = true;
            }
            else if (par->Right + mem == cur)
            {
                tmp_adr = CreateDataNode(count);

                isParLeft = false;
            }
            else
            {
                return;
            }

            var stack = stackalloc Flugs*[100];
            var arr = (int*) (mem + tmp_adr + sizeof (TreeNode));
            var lvl = 0;
            var ind = 0;

            var flug = cur;
            var isUp = false;
            var isLeft = false;

            while (lvl >= 0)
            {
                // block

                #region ' Block '

                if ((*flug & Flugs.Block) == Flugs.Block)
                {
                    var itm = (BlockNode*) flug;

                    if (!isUp)
                    {
                        if (itm->Left < 0)
                        {
                            arr[ind++] = -itm->Left;
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
                }

                    #endregion

                #region ' Tree '

                else if ((*flug & Flugs.Tree) == Flugs.Tree)
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

                    if ((*(Flugs*) last & Flugs.Tree) == Flugs.Tree)
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

        // Remove block node
        void RemoveBlockNode(BlockNode* cur)
        {
            //len -= sizeof(BlockNode);

            ReleaseMemory((byte*)cur, sizeof(BlockNode));
        }
        
        // Remove tree node
        void RemoveTreeNode(int* par_ptr, TreeNode* cur)
        {
            if (cur->Left == 0 && cur->Right == 0)
            {
                *par_ptr = 0;

                ReleaseMemory((byte*)cur, sizeof(TreeNode));
            }
            else if (cur->Left == 0)
            {
                *par_ptr = cur->Right;

                ReleaseMemory((byte*)cur, sizeof(TreeNode));
            }
            else if (cur->Right == 0)
            {
                *par_ptr = cur->Left;

                ReleaseMemory((byte*)cur, sizeof(TreeNode));
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
                var tmp = (TreeNode*)(mem + cur->Right);

                while (tmp->Left > 0)
                {
                    tmp->Count--;

                    par_ptr = &tmp->Left;
                    tmp = (TreeNode*)(mem + tmp->Left);
                }

                tmp->Count--;

                if (tmp->Left < 0)
                {
                    cur->Data = -tmp->Left;
                    tmp->Left = 0;

                    return;
                }

                cur->Data = tmp->Data;

                if (tmp->Right < 0)
                {
                    tmp->Data = -tmp->Right;
                    tmp->Right = 0;

                    return;
                }
                else if (tmp->Right > 0)
                {
                    *par_ptr = tmp->Right;

                    ReleaseMemory((byte*)tmp, sizeof(TreeNode));
                }
                else
                {
                    *par_ptr = 0;

                    ReleaseMemory((byte*)tmp, sizeof(TreeNode));
                }
            }
        }

        // Remove tree node
        void RemoveTreeNode(int* par_ptr, TreeNode* cur, out int newMax)
        {
            newMax = 0;

            if (cur->Left == 0 && cur->Right == 0)
            {
                *par_ptr = 0;

                ReleaseMemory((byte*)cur, sizeof(TreeNode));

                if ((par->flugs & Flugs.Block) == Flugs.Block)
                {
                    var ptr = (BlockNode*)par;

                    newMax = ptr->MaxOnLeft;
                }
                else if ((par->flugs & Flugs.Tree) == Flugs.Tree)
                {
                    var ptr = (TreeNode*)par;

                    newMax = ptr->Data;
                }
            }
            else if (cur->Left == 0)
            {
                *par_ptr = cur->Right;

                ReleaseMemory((byte*)cur, sizeof(TreeNode));

                var tmp = cur;

                while (tmp->Right > 0)
                {
                    tmp = (TreeNode*)(mem + tmp->Right);
                }

                if (tmp->Right == 0)
                {
                    newMax = tmp->Data;
                }
                else
                {
                    newMax = -tmp->Right;
                }
            }
            else if (cur->Right == 0)
            {
                *par_ptr = cur->Left;

                ReleaseMemory((byte*)cur, sizeof(TreeNode));

                if (cur->Left < 0)
                {
                    newMax = -cur->Left;

                    return;
                }
                
                var tmp = (TreeNode*)(mem + cur->Left);

                while (tmp->Right > 0)
                {
                    tmp = (TreeNode*)(mem + tmp->Right);
                }

                if (tmp->Right == 0)
                {
                    newMax = tmp->Data;
                }
                else
                {
                    newMax = -tmp->Right;
                }
            }
            else if (cur->Left < 0)
            {
                cur->Data = -cur->Left;
                cur->Left = 0;

                // newMax will not be in demand
            }
            else if (cur->Right < 0)
            {
                cur->Data = -cur->Right;
                cur->Right = 0;

                newMax = cur->Data;
            }
            else
            {
                par_ptr = &cur->Right;
                var tmp = (TreeNode*)(mem + cur->Right);

                while (tmp->Left > 0)
                {
                    tmp->Count--;

                    par_ptr = &tmp->Left;
                    tmp = (TreeNode*)(mem + tmp->Left);
                }

                tmp->Count--;

                if (tmp->Left < 0)
                {
                    cur->Data = -tmp->Left;
                    tmp->Left = 0;

                    return;
                }

                cur->Data = tmp->Data;

                if (tmp->Right < 0)
                {
                    tmp->Data = -tmp->Right;
                    tmp->Right = 0;

                    return;
                }
                else if (tmp->Right > 0)
                {
                    *par_ptr = tmp->Right;

                    ReleaseMemory((byte*)tmp, sizeof(TreeNode));
                }
                else
                {
                    *par_ptr = 0;

                    ReleaseMemory((byte*)tmp, sizeof(TreeNode));
                }

                // newMax will not be in demand
            }
        }

        // Remove data node
        void RemoveDataNode(DataNode* cur)
        {
            
        }
        
        // Release memory
        void ReleaseMemory(byte* ptr, int size)
        {
            
        }
        
        // Find max value on sub tree
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int FindMax(Flugs* flug)
        {
            while (true)
            {
                switch (*flug)
                {
                    case Flugs.Block:
                    {
                        var cur = (BlockNode*)flug;

                        if (cur->Right > 0)
                        {
                            flug = (Flugs*)(mem + cur->Right);

                            continue;
                        }
                        else if (cur->Right < 0)
                        {
                            return -cur->Right;
                        }
                        else
                        {
                            return cur->MaxOnLeft;
                        }
                    }
                    case Flugs.Data:
                    {
                        var ptr = (DataNode*)flug;

                        return ptr->Max;
                    }
                    case Flugs.Tree:
                    {
                        var cur = (TreeNode*)flug;

                        if (cur->Right > 0)
                        {
                            flug = (Flugs*)(mem + cur->Right);

                            continue;
                        }
                        else if (cur->Right < 0)
                        {
                            return -cur->Right;
                        }
                        else
                        {
                            return cur->Data;
                        }
                    }
                }
            }
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

            this.file = fs.CreateMMF(key);

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
            bool flug = false;

            CheckTree(memoryKey, ref flug, comparer, getter);

            return flug;
        }
        int CheckTree<T>(int node, ref bool flug, IRecordsComparer<T> comparer, IRecordsGetter<T> getter) where T : Record
        {
            if (node < 1)
            {
                Debug.WriteLine("Warning: The node " + node + " has offset < 1. Situation A");

                flug |= true;

                return 0;
            }

            var tmp = (Flugs*)(mem + node);

            switch (*tmp)
            {
                case Flugs.Tree:
                {
                    var cur = (TreeNode*)tmp;

                    #region ' check data and count '

                    if (cur->Count < 0)
                    {
                        Debug.WriteLine("Warning: The tree node " + node + "(value=" + cur->Data + ",count=" + cur->Count + ")" + " has count < 0. Situation B");

                        flug |= true;
                    }

                    if (cur->Data < 0)
                    {
                        Debug.WriteLine("Warning: The tree node " + node + "(value=" + cur->Data + ",count=" + cur->Count + ")" + " has data < 0. Situation C");

                        flug |= true;
                    }

                    var l_count = 0;
                    var l_data = 0;

                    if (cur->Left > 0)
                    {
                        l_data = ((TreeNode*)(mem + (cur->Left)))->Data;

                        l_count = CheckTree(cur->Left, ref flug, comparer, getter);
                    }
                    else if (cur->Left < 0)
                    {
                        l_count = 1;

                        l_data = -cur->Left;
                    }

                    var r_count = 0;
                    var r_data = 0;

                    if (cur->Right > 0)
                    {
                        r_data = ((TreeNode*)(mem + (cur->Right)))->Data;

                        r_count = CheckTree(cur->Right, ref flug, comparer, getter);
                    }
                    else if (cur->Right < 0)
                    {
                        r_count = 1;

                        r_data = -cur->Right;
                    }

                    if (l_data != 0 && l_data >= cur->Data)
                    {
                        Debug.WriteLine("Warning: The tree node " + node + "(value=" + cur->Data + ",count=" + cur->Count + ")" + " has wrong data on left. l_data >= cur->Data. Situation D");

                        flug |= true;
                    }

                    if (r_data != 0 && r_data <= cur->Data)
                    {
                        Debug.WriteLine("Warning: The tree node " + node + "(value=" + cur->Data + ",count=" + cur->Count + ")" + " has wrong data on left. r_data <= cur->Data. Situation E");

                        flug |= true;
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
                            var cmp = comparer.CompareRecords(getter.GetRecord(dat[i - 1]), dat[0]);

                            if (cmp >= 0)
                            {
                                Debug.WriteLine("Warning: The data node " + node + "(min=" + cur->Min + ", max=" + cur->Max + ",count=" + cur->Count + ")" + " wrong data structure!. Situation S");

                                flug |= true;

                                break;
                            }
                        }
                    }

                    #endregion

                    return cur->Count;
                }
                case Flugs.Block:
                {
                    var cur = (BlockNode*)tmp;

                    if (cur->Count < 0)
                    {
                        Debug.WriteLine("Warning: The block node " + node + "(max=" + cur->MaxOnLeft + ",count=" + cur->Count + ")" + " has count < 0. Situation G");

                        flug |= true;
                    }
                    
                    #region ' check max on left '

                    if (cur->Left < 0)
                    {
                        if (-cur->Left != cur->MaxOnLeft)
                        {
                            Debug.WriteLine("Warning: The block node " + node + "(max=" + cur->MaxOnLeft + ",count=" + cur->Count + ")" + " has wrong MaxOnLeft must be: " + -cur->Left + ". Situation H");

                            flug |= true;
                        }
                    }
                    else if (cur->Left == 0)
                    {
                        if (cur->MaxOnLeft != 0)
                        {
                            Debug.WriteLine("Warning: The block node " + node + "(max=" + cur->MaxOnLeft + ",count=" + cur->Count + ")" + " has wrong MaxOnLeft must be: 0. Situation I");

                            flug |= true;
                        }
                    }
                    else
                    {
                        var tmp_pos = cur->Left;

                        while (true)
                        {
                            var fff = (Flugs*)(mem + tmp_pos);

                            switch (*fff)
                            {
                                case Flugs.Block:
                                    {
                                        var ppc = (BlockNode*)fff;

                                        if (ppc->Right < 0)
                                        {
                                            if (cur->MaxOnLeft != -ppc->Right)
                                            {
                                                Debug.WriteLine("Warning: The block node " + node + "(max=" + cur->MaxOnLeft + ",count=" + cur->Count + ")" + " has wrong MaxOnLeft must be: " + -ppc->Right + ". Situation J");

                                                flug |= true;
                                            }

                                            goto exit;
                                        }
                                        else if (ppc->Right == 0)
                                        {
                                            if (cur->MaxOnLeft != ppc->MaxOnLeft)
                                            {
                                                Debug.WriteLine("Warning: The block node " + node + "(max=" + cur->MaxOnLeft + ",count=" + cur->Count + ")" + " has wrong MaxOnLeft must be: " + ppc->MaxOnLeft + ". Situation K");

                                                flug |= true;
                                            }

                                            goto exit;
                                        }
                                        else
                                        {
                                            tmp_pos = ppc->Right;

                                            continue;
                                        }
                                    }
                                case Flugs.Tree:
                                    {
                                        var ppc = (TreeNode*)fff;

                                        if (ppc->Right < 0)
                                        {
                                            if (cur->MaxOnLeft != -ppc->Right)
                                            {
                                                Debug.WriteLine("Warning: The block node " + node + "(max=" + cur->MaxOnLeft + ",count=" + cur->Count + ")" + " has wrong MaxOnLeft must be: " + -ppc->Right + ". Situation L");

                                                flug |= true;
                                            }

                                            goto exit;
                                        }
                                        else if (ppc->Right == 0)
                                        {
                                            if (cur->MaxOnLeft != ppc->Data)
                                            {
                                                Debug.WriteLine("Warning: The block node " + node + "(max=" + cur->MaxOnLeft + ",count=" + cur->Count + ")" + " has wrong MaxOnLeft must be: " + ppc->Data + ". Situation M");

                                                flug |= true;
                                            }

                                            goto exit;
                                        }
                                        else
                                        {
                                            tmp_pos = ppc->Right;

                                            continue;
                                        }
                                    }
                                case Flugs.Data:
                                    {
                                        var ppc = (DataNode*)fff;
                                        var dat = (int*)(mem + ppc->Data);

                                        if (cur->MaxOnLeft != ppc->Max)
                                        {
                                            Debug.WriteLine("Warning: The block node " + node + "(max=" + cur->MaxOnLeft + ",count=" + cur->Count + ")" + " has wrong MaxOnLeft must be: " + ppc->Data + ". Situation N");

                                            flug |= true;
                                        }

                                        if (cur->MaxOnLeft != dat[ppc->Count - 1])
                                        {
                                            Debug.WriteLine("Warning: The block node " + node + "(max=" + cur->MaxOnLeft + ",count=" + cur->Count + ")" + " has wrong MaxOnLeft must be: " + ppc->Data + ". Situation O");

                                            flug |= true;
                                        }

                                        goto exit;
                                    }
                            }
                        }

                    exit: ;
                    }

                    #endregion

                    #region ' check count '

                    var l_count = 0;
                    var l_data = 0;

                    if (cur->Left > 0)
                    {
                        l_count = CheckTree(cur->Left, ref flug, comparer, getter);
                    }
                    else if (cur->Left < 0)
                    {
                        l_count = 1;

                        l_data = -cur->Left;
                    }

                    var r_count = 0;
                    var r_data = 0;

                    if (cur->Right > 0)
                    {
                        r_count = CheckTree(cur->Right, ref flug, comparer, getter);
                    }
                    else if (cur->Right < 0)
                    {
                        r_count = 1;

                        r_data = -cur->Right;
                    }

                    if (l_data < 0 && r_data < 0 && l_data <= r_data)
                    {
                        Debug.WriteLine("Warning: The block node " + node + "(max=" + cur->MaxOnLeft + ",count=" + cur->Count + ")" + " has wrong data. Situation P");

                        flug |= true;
                    }

                    var c = l_count + r_count;

                    if (cur->Count != c)
                    {
                        Debug.WriteLine("Warning: The block node " + node + "(max=" + cur->MaxOnLeft + ",count=" + cur->Count + ")" + " has wrong count. Must be:" + c + ". Situation F");

                        flug |= true;
                    }

                    #endregion

                    return cur->Count;
                }
                default:
                {
                    Debug.WriteLine("Warning: The tree node " + node + " has unknown type!");

                    flug |= true;

                    break;
                }
            }

            return 0;
        }

        #endregion

        #region ' Stuctures '

        // 
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        struct BlockNode
        {
            public Flugs flugs;
            public int Count;
            public int Left;
            public int Right;
            public int MaxOnLeft;

            public override string ToString()
            {
                return "count: " + Count + ", max: " + MaxOnLeft;
            }
        }

        // 
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        struct TreeNode
        {
            public Flugs flugs;
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
            public Flugs flugs;
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
            public int Left;
            public int Right;
            public int Data;

            public override string ToString()
            {
                return "right: " + Right + ", left: " + Left;
            }
        }

        // Base struct for TreeNode and BlockNode
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        struct LinkNode
        {
            public Flugs flugs;
            public int Count;
            public int Left;
            public int Right;
        }

        // Base struct for TreeNode, BlockNode and DataNode
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        struct CountNode
        {
            public Flugs flugs;
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
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        struct LinkedNode
        {
            public int Next;
            public int Value;
        }

        // Node type
        enum Flugs : byte
        {
            Block   = 01,
            Data    = 02,
            Tree    = 03,
        }

        // Action for change nodes. Used by no locking change in multy thread algoritm.
        enum ChangeAction
        {
            None,

            RemoveBlockNodeOnLeftLast,
            RemoveBlockNodeOnRightLast,
            RemoveTreeNodeOnLeftLast,
            RemoveTreeNodeOnRightLast,
            RemoveTreeNode,
            RemoveDataNode,
            RemoveDataNodeOne,
            RemoveDataNodeFirst,
            RemoveDataNodeMiddle,

            InsertBlockNodeLeftLast,
            InsertBlockNodeLeftTree,
            InsertBlockNodeRightLast,
            InsertBlockNodeRightTree,
            InsertTreeNodeLeftLast,
            InsertTreeNodeLeftTree,
            InsertTreeNodeRightLast,
            InsertTreeNodeRightTree,
            InsertDataNodeFirst,
            InsertDataNodeLast,
            InsertDataNodeMiddle
        }

        #endregion

        #region ' Test '
#if DEBUG 

        public void PrintToDebug(int key)
        {
            var flug = (Flugs*)(mem + key);

            PrintToDebug(flug, 0, "");
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

            // Block
            if ((*flug & Flugs.Block) == Flugs.Block)
            {
                var cur = (BlockNode*)flug;

                Debug.WriteLine(str0 + "Block " + *cur);

                if (cur->Left == 0)
                {
                    Debug.WriteLine(str1 + "|left  is null");
                }
                else if (cur->Left < 0)
                {
                    Debug.WriteLine(str1 + "|left  " + cur->Left);
                }
                else
                {
                    PrintToDebug((Flugs*)(mem + cur->Left), lvl + 1, str1 + "|left  ");
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
            // Tree
            else if ((*flug & Flugs.Tree) == Flugs.Tree)
            {
                var cur = (TreeNode*)flug;

                Debug.WriteLine(str0 + "Tree " + *cur);

                if (cur->Left == 0)
                {
                    Debug.WriteLine(str1 + "|left  is null");
                }
                else if (cur->Left < 0)
                {
                    Debug.WriteLine(str1 + "|left  " + cur->Left);
                }
                else
                {
                    PrintToDebug((Flugs*)(mem + cur->Left), lvl + 1, str1 + "|left  ");
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

        // 
        public object TestFunction(TestAction action, params object[] args)
        {
            switch (action)
            {
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

                #region ' BlockNode '

                case TestAction.BlockNodeCreate:
                {
                    if (args.Length != 2)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    int left = (int)args[0];
                    int right = (int)args[1];

                    var tmp = CreateBlockNode(left, right);
                    var ptr = (BlockNode*)(mem + tmp);

                    if (ptr->flugs != Flugs.Block)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return tmp;
                }

                case TestAction.BlockNodeGetLeftLink:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ptr = (BlockNode*)(mem + key);

                    if (ptr->flugs != Flugs.Block)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->Left;
                }

                case TestAction.BlockNodeGetRightLink:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ptr = (BlockNode*)(mem + key);

                    if (ptr->flugs != Flugs.Block)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->Right;
                }

                case TestAction.BlockNodeGetCount:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ptr = (BlockNode*)(mem + key);

                    if (ptr->flugs != Flugs.Block)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->Count;
                }

                case TestAction.BlockNodeSetCount:
                {
                    if (args.Length != 2)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var val = (int)args[1];
                    var ptr = (BlockNode*)(mem + key);

                    if (ptr->flugs != Flugs.Block)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    ptr->Count = val;

                    return null;
                }

                case TestAction.BlockNodeGetMaxOnLeft:
                {
                    if (args.Length != 1)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var ptr = (BlockNode*)(mem + key);

                    if (ptr->flugs != Flugs.Block)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->MaxOnLeft;
                }

                case TestAction.BlockNodeSetMaxOnLeft:
                {
                    if (args.Length != 2)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var key = (int)args[0];
                    var val = (int)args[1];
                    var ptr = (BlockNode*)(mem + key);

                    if (ptr->flugs != Flugs.Block)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    ptr->MaxOnLeft = val;

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

                    if (node->flugs != Flugs.Data)
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

                    if (ptr->flugs != Flugs.Data)
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

                    if (ptr->flugs != Flugs.Data)
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

                    if (ptr->flugs != Flugs.Data)
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

                    if (ptr->flugs != Flugs.Data)
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
                    if (args.Length != 3)
                    {
                        throw new ArgumentException("See enum TestAction coments.");
                    }

                    var val = (int)args[0];
                    var left = (int)args[1];
                    var right = (int)args[2];
                    
                    var ret = CreateTreeNode(val, left, right);
                    var ptr = (BlockNode*)(mem + ret);

                    if (ptr->flugs != Flugs.Tree)
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

                    if (ptr->flugs != Flugs.Tree)
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

                    if (ptr->flugs != Flugs.Tree)
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

                    if (ptr->flugs != Flugs.Tree)
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

                    if (ptr->flugs != Flugs.Tree)
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

                    if (ptr->flugs != Flugs.Tree)
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

                    if (ptr->flugs != Flugs.Tree)
                    {
                        throw new ArgumentException("Node type wrong.");
                    }

                    return ptr->Left;
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
            /// Params: [0](int)LeftLink, [1](int)RightLink, [ret](int)MemoryOffset
            /// </summary>
            BlockNodeCreate,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Count
            /// </summary>
            BlockNodeGetCount,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [1](int)NewCount, [ret]null
            /// </summary>
            BlockNodeSetCount,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)maxOnLeft
            /// </summary>
            BlockNodeGetMaxOnLeft,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [1](int)NewMaxOnLeft, [ret]null
            /// </summary>
            BlockNodeSetMaxOnLeft,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Left
            /// </summary>
            BlockNodeGetLeftLink,

            /// <summary>
            /// Params: [0](int)MemoryOffset, [ret](int)Right
            /// </summary>
            BlockNodeGetRightLink,

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
            /// Params: [0](int)Val, [1](int)Left, [2](int)Right, [ret](int)MemoryOffset
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
        }

#endif
        #endregion

        #region ' Group '

        public int GetGroup<T>(T grp, int memoryKey, IGroupComparer<T> comparer) where T : IComparable<T>
        {
            try
            {
                locker.ReadLock();

                #region ' Prepare '

                var root = (BlockNode*)(mem + memoryKey);

                if (root->Left == 0)
                {
                    return 0;
                }

                #endregion

                #region ' Find '

                var cur = (GroupNode*)(mem + root->Left);

                while (true)
                {
                    var val = ReadGroupValue(cur->Data);
                    var cmp = comparer.CompareGroups(grp, val);

                    // already exist
                    if (cmp == 0)
                    {
                        return cur->Data;
                    }
                    // left
                    else if (cmp < 0)
                    {
                        if (cur->Left > 0)
                        {
                            cur = (GroupNode*)(mem + cur->Left);

                            continue;
                        }

                        break;
                    }
                        // right
                    else
                    {
                        if (cur->Right > 0)
                        {
                            cur = (GroupNode*)(mem + cur->Right);

                            continue;
                        }

                        break;
                    }
                }

                #endregion

            }
            finally
            {
                locker.Unlock();
            }

            return 0;
        }

        public int GetOrCreateGroup<T>(T grp, int memoryKey, IGroupComparer<T> comparer) where T : IComparable<T>
        {
            try
            {
                locker.WriteLock();

                #region ' Prepare '

                var root = (BlockNode*)(mem + memoryKey);

                if (root->Left == 0)
                {
                    root->Left = CreateGroupNode(0, 0);

                    var tmp = (GroupNode*)(mem + root->Left);

                    return tmp->Data;
                }

                #endregion

                #region ' Find place and insert '

                var cur = (GroupNode*)(mem + root->Left);

                while (true)
                {
                    var val = ReadGroupValue(cur->Data);
                    var cmp = comparer.CompareGroups(grp, val);

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
                            // Lock
                            cur->Left = CreateGroupNode(0, 0);

                            cur = (GroupNode*)(mem + cur->Left);

                            return cur->Data;
                        }

                        cur = (GroupNode*)(mem + cur->Left);
                    }
                    // right
                    else
                    {
                        if (cur->Right == 0)
                        {
                            // Lock
                            cur->Right = CreateGroupNode(0, 0);

                            cur = (GroupNode*)(mem + cur->Right);

                            return cur->Data;
                        }

                        cur = (GroupNode*)(mem + cur->Right);
                    }
                }

                #endregion

            }
            finally
            {
                locker.Unlock();
            }
        }

        public void RemoveGroup<T>(T grp, int memoryKey, IGroupComparer<T> comparer) where T : IComparable<T>
        {
            try
            {
                locker.WriteLock();

                #region ' Prepare '

                var root = (BlockNode*)(mem + memoryKey);

                if (root->Left == 0)
                {
                    return;
                }

                #endregion

                #region ' Find '

                var cur = (GroupNode*)(mem + root->Left);
                var par = &root->Left;

                while (true)
                {
                    var val = ReadGroupValue(cur->Data);

                    if (val == 0)
                    {
                        break;
                    }

                    var cmp = comparer.CompareGroups(grp, val);

                    // already exist
                    if (cmp == 0)
                    {
                        break;
                    }
                    // left
                    else if (cmp < 0)
                    {
                        if (cur->Left > 0)
                        {
                            par = &cur->Left;
                            cur = (GroupNode*)(mem + cur->Left);

                            continue;
                        }

                        return;
                    }
                    // right
                    else
                    {
                        if (cur->Right > 0)
                        {
                            par = &cur->Right;
                            cur = (GroupNode*)(mem + cur->Right);

                            continue;
                        }

                        return;
                    }
                }

                #endregion
              
                #region ' Remove '

                if (cur->Left == 0 && cur->Right == 0)
                {
                    *par = 0;
                }
                else if (cur->Left == 0)
                {
                    *par = cur->Right;
                }
                else if (cur->Right == 0)
                {
                    *par = cur->Left;
                }
                else
                {
                    par = &cur->Right;
                    var tmp = (GroupNode*)(mem + cur->Right);

                    while (tmp->Left > 0)
                    {
                        par = &tmp->Left;
                        tmp = (GroupNode*)(mem + tmp->Left);
                    }

                    cur->Data = tmp->Data;

                    if (tmp->Right > 0)
                    {
                        *par = tmp->Right;

                        ReleaseMemory((byte*)tmp, sizeof(GroupNode));
                    }
                    else
                    {
                        *par = 0;

                        ReleaseMemory((byte*)tmp, sizeof(GroupNode));
                    }
                }

                #endregion
            }
            finally
            {
                locker.Unlock();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int ReadGroupValue(int memoryKey)
        {
            var flug = (Flugs*)(mem + memoryKey);

            while (true)
            {
                switch (*flug)
                {
                    case Flugs.Block:
                    {
                        var cur = (BlockNode*)(flug);

                        if (cur->MaxOnLeft != 0)
                        {
                            return cur->MaxOnLeft;
                        }
                        else if (cur->Right > 0)
                        {
                            flug = (Flugs*)(mem + cur->Right);

                            continue;
                        }
                        else if (cur->Left > 0)
                        {
                            flug = (Flugs*)(mem + cur->Left);

                            continue;
                        }
                        else if (cur->Right < 0)
                        {
                            return -cur->Right;
                        }
                        else if (cur->Left < 0)
                        {
                            return -cur->Left;
                        }
                        else
                        {
                            return 0;
                        }
                    }
                    case Flugs.Data:
                    {
                        var cur = (DataNode*)(flug);

                        if (cur->Count > 0)
                        {
                            var ptr = (int*)(mem + cur->Data);

                            return ptr[0];
                        }
                        else
                        {
                            return 0;
                        }
                    }
                    case Flugs.Tree:
                    {
                        var cur = (TreeNode*)(flug);

                        return cur->Data;
                    }
                    default:
                    {
                        return 0;
                    }
                }
            }
        }

        #endregion
    }
}
