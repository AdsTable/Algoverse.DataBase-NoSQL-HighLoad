using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using static System.String;

namespace Algoverse.DataBase
{
    public class Log : IDisposable
    {
        const int Size = 256;

        static byte[] clear = new byte[Size];

        string code;
        string path;
        FileStream writer = null;
        byte[] wbuf = null;

        public Log(string path)
        {
            code = Guid.NewGuid().ToString("N");
            this.path = path;
        }

        public Log(string path, string code)
        {
            this.path = path;
            this.code = code;
        }

        public string Code
        {
            get
            {
                return code;
            }
        }

        public DateTime LastWriteTime { get; set; }

        public void Append(string message)
        {
            lock (path)
            {
                if (wbuf == null)
                {
                    wbuf = new Byte[Size];
                }

                if (writer == null)
                {
                    writer = new FileStream(path + code + ".log", FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
                }

                var l = message.Length * 2;

                for (int i = 0; i < l; )
                {
                    var c = l - i <= Size - 2 ? l - i : Size - 2;

                    wbuf[0] = (byte)c;
                    wbuf[1] = (byte)(c >> 8);

                    for (int j = 0; j < c; j += 2)
                    {
                        var ch = message[i / 2 + j / 2];

                        wbuf[j + 2] = (byte)ch;
                        wbuf[j + 3] = (byte)(ch >> 8);
                    }

                    writer.Write(wbuf, 0, c + 2);

                    if (c + 2 < Size)
                    {
                        writer.Write(clear, 0, Size - c -2);
                    }

                    i += c;
                }

                writer.Flush();
            }
        }
        
        public void Append(Exception e)
        {
            Append(e.Message);
        }

        public void Append(object message)
        {
            Append(message.ToString());
        }

        public void Append(params string[] args)
        {
            if (args != null)
            {
                Append(Concat(args));
            }
        }

        public void Append(params object[] args)
        {
            if (args != null)
            {
                Append(Concat(args));
            }
        }

        public void AppendLine(string message)
        {
            Append(message + "\r\n");
        }

        public void ReadToEnd(int pos, List<string> list)
        {
            var fs = new FileStream(path + code + ".log", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            var len = fs.Length / Size;
            var bb = new byte[Size];

            for (int i = pos; i < len; ++i)
            {
                var pp = i * Size;

                if (fs.Position != pp)
                {
                    fs.Position = pp;
                }

                fs.Read(bb, 0, Size);

                var count = (bb[0] + (bb[1] << 8)) / 2;
                var ch = new char[count];

                for (var j = 1; j < count + 1; ++j)
                {
                    ch[j - 1] = (char)(bb[j * 2] + (bb[j * 2 + 1] << 8));
                }

                list.Add(new string(ch));
            }

            fs.Close();
        }

        public void Dispose()
        {
            lock (path)
            {
                if (writer != null)
                {
                    writer.Close();
                    writer = null;
                }
            }
        }

        public string ToStringEx()
        {
            var list = new List<string>();

            ReadToEnd(0, list);

            return Concat(list);// Join("\r\n", list);
        }
    }
}
