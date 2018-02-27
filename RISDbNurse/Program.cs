using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RISDbNurse
{
    class Program
    {
        static void Main(string[] args)
        {
            RISDBNurse nurse = new RISDBNurse(".", "zpTest", "sa", "pass@word", "zp");
            Console.WriteLine("数据库是否存在：" + nurse.DatabaseExists());

            nurse = new RISDBNurse(".", "zjrTest", "sa", "pass@word", "zjr");
            nurse.ProcessBeforeEvent += new RISDBNurse.ProcessHandler(nurse_ProcessBeforeEvent);
            nurse.ProcessAfterEvent += new RISDBNurse.ProcessHandler(nurse_ProcessAfterEvent);
            Console.WriteLine(nurse.ConnectionString);
            Console.WriteLine(nurse.ConnectionStringToMaster);

            nurse.SmartMaintain();

            Console.ReadKey();
        }

        static void nurse_ProcessAfterEvent(object sender, RISDBNurse.DbProcessEventArgs e)
        {
            Console.WriteLine(e.ToString());
        }

        static void nurse_ProcessBeforeEvent(object sender, RISDBNurse.DbProcessEventArgs e)
        {
            Console.WriteLine(e.ToString());
        }
    }
}
