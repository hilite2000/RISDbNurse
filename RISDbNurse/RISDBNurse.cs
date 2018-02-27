using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace RISDbNurse
{
    public class RISDBNurse
    {
        #region ==========================================   通知事件

        public class DbProcessEventArgs : EventArgs
        {
            public DbProcessEventArgs()
            {
                this.AllStep = 1;
                this.CurStep = 1;
            }

            public int AllStep
            { get; set; }

            public int CurStep
            { get; set; }

            public string Message
            { get; set; }


            /// <summary>
            /// 已重载，显示当前内容
            /// </summary>
            /// <returns></returns>
            public override string ToString()
            {
                return string.Format("AllStep：{0}; CurStep：{1}; Msg：{2}", this.AllStep, this.CurStep, this.Message);
            }
        }

        public delegate void ProcessHandler(object sender, DbProcessEventArgs e);

        public event ProcessHandler ProcessBeforeEvent;

        public event ProcessHandler ProcessAfterEvent;

        protected void OnProcessBefore(string msg)
        {
            DbProcessEventArgs e = new DbProcessEventArgs();
            e.Message = msg;
            OnProcessBefore(e);
        }


        protected void OnProcessBefore(DbProcessEventArgs e)
        {
            if (this.ProcessBeforeEvent == null) return;
            this.ProcessBeforeEvent(this, e);
        }


        protected void OnProcessAfter(string msg)
        {
            DbProcessEventArgs e = new DbProcessEventArgs();
            e.Message = msg;
            OnProcessAfter(e);
        }


        protected void OnProcessAfter(DbProcessEventArgs e)
        {
            if (this.ProcessBeforeEvent == null) return;
            this.ProcessAfterEvent(this, e);
        }
        #endregion


        /// <summary>
        /// 目标Ris数据连接串
        /// </summary>
        public string ConnectionString
        { get; private set; }


        /// <summary>
        /// Db主机Mast连接串
        /// </summary>
        public string ConnectionStringToMaster
        { get; private set; }


        /// <summary>
        /// Ris数据库名
        /// </summary>
        public string DbName
        { get; private set; }


        /// <summary>
        /// 公司代码
        /// </summary>
        public string GroupCode
        { get; private set; }


        /// <summary>
        /// 平台连接串
        /// </summary>
        public string ConnectionStringForPlatform
        {
            get
            {
                //todo：改为平台连接串
                return this.ConnectionString;
            }
        }

        /// <summary>
        /// 脚本所在的基础路径
        /// </summary>
        public string ScriptFileBaseFolder
        {
            //todo：确认脚本的基础路径
            get
            {
                return Environment.CurrentDirectory;
            }
        }


        /// <summary>
        /// Ris数据库升级保姆
        /// </summary>
        /// <param name="host">数据库主机</param>
        /// <param name="dbName">公司数据库</param>
        /// <param name="dbUser">数据库用户名</param>
        /// <param name="dbPwd">数据库密码</param>
        public RISDBNurse(string host, string dbName, string dbUser, string dbPwd, string groupCode)
        {
            this.DbName = dbName;
            this.GroupCode = groupCode;

            SqlConnectionStringBuilder csBuilder = new SqlConnectionStringBuilder();
            csBuilder.DataSource = host;
            csBuilder.UserID = dbUser;
            csBuilder.Password = dbPwd;
            csBuilder.PersistSecurityInfo = false;
            csBuilder.MultipleActiveResultSets = true;
            csBuilder.ConnectTimeout = 30;

            csBuilder.InitialCatalog = "master";
            this.ConnectionStringToMaster = csBuilder.ConnectionString;

            csBuilder.InitialCatalog = dbName;
            this.ConnectionString = csBuilder.ConnectionString;
        }


        /// <summary>
        /// 根据设置智能维护数据库，包括创建和升级。一条命令全搞定。
        /// </summary>
        public void SmartMaintain()
        {
            if (DatabaseExists() == false)
            {
                CreateDatabase();
                CreateUpdateLog();
            }
            UpdateDatabase();
        }



        /// <summary>
        /// 测试Ris数据库是否存在
        /// </summary>
        /// <returns></returns>
        public bool DatabaseExists()
        {
            OnProcessBefore("开始检查数据库是否存在：" + this.DbName);

            string query = "SELECT Count(name) FROM sysdatabases WHERE name = '" + this.DbName.Replace("'", "''") + "'";
            using (var conn = new SqlConnection(this.ConnectionStringToMaster))
            {
                conn.Open();
                var command = new SqlCommand(query, conn);
                object result = command.ExecuteScalar();
                if (result != null && result != DBNull.Value)
                {
                    OnProcessAfter("检查数据库存在");
                    return Convert.ToInt32(result) > 0;
                }
                conn.Close();
            }

            OnProcessAfter("检查数据库不存在");

            return false;
        }


        /// <summary>
        /// 创建Ris数据库
        /// </summary>
        private void CreateDatabase()
        {
            OnProcessBefore("开始创建数据库：" + this.DbName);

            string sql = string.Format(" CREATE DATABASE [{0}] ", this.DbName);
            using (var conn = new SqlConnection(this.ConnectionStringToMaster))
            {
                conn.Open();
                var command = new SqlCommand(sql, conn);
                command.ExecuteNonQuery();
                conn.Close();
            }

            OnProcessAfter("数据库创建完毕");
        }


        /// <summary>
        /// 创建Ris数据库升级日志表
        /// </summary>
        private void CreateUpdateLog()
        {
            OnProcessBefore("开始创建升级日志");

            string sql = @"
CREATE TABLE [dbo].[TB_Platform_UpdateLog](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Version] [int] NOT NULL,
	[UpdateDate] [smalldatetime] NULL
) ON [PRIMARY]
ALTER TABLE [dbo].[TB_Platform_UpdateLog] ADD  CONSTRAINT [DF_TB_Platform_UpdateLog_UpdateDate]  DEFAULT (getdate()) FOR [UpdateDate]
";
            using (var conn = new SqlConnection(this.ConnectionString))
            {
                conn.Open();
                var command = new SqlCommand(sql, conn);
                command.ExecuteNonQuery();
                conn.Close();
            }

            OnProcessAfter("升级日志创建完毕");
        }


        /// <summary>
        /// 获得用户数据库中当前最大版本号
        /// </summary>
        /// <returns></returns>
        public int GetMaxVersion()
        {
            OnProcessBefore("开始检查最大版本号：" + this.DbName);

            string sql = @"select max([RisVersion]) from TB_Platform_UpdateLog";
            using (var conn = new SqlConnection(this.ConnectionString))
            {
                conn.Open();
                var command = new SqlCommand(sql, conn);
                object result = command.ExecuteScalar();
                if (result != null && result != DBNull.Value)
                {
                    OnProcessAfter("当前版本号：" + result.ToString());
                    return Convert.ToInt32(result);
                }
                conn.Close();
            }

            OnProcessAfter("当前版本号：0");

            return 0;
        }

        private void SetMaxVersion(SqlTransaction tran, int version)
        {
            OnProcessBefore("开始回写最大版本号：" + version);

            string sql = @"Insert into TB_Platform_UpdateLog([RisVersion]) values(@RisVersion) ";

            var command = new SqlCommand(sql, tran.Connection, tran);
            command.Parameters.AddWithValue("@RisVersion", version);
            command.ExecuteNonQuery();

            OnProcessAfter("最大版本回写完成");
        }


        /// <summary>
        /// 设置平台的版本信息，与Ris数据库版本同步
        /// </summary>
        /// <param name="version"></param>
        private void SetPlatformVersion(int risVersion)
        {
            return; //todo：打开代码
            OnProcessBefore("开始同步最大版本号到平台：" + risVersion);

            string sql = @"Update TB_GroupInformation Set Version=@risVersion where GroupCode=@groupCode ";

            using (SqlConnection conn = new SqlConnection(this.ConnectionStringForPlatform))
            {
                conn.Open();
                var command = new SqlCommand(sql, conn);
                command.Parameters.AddWithValue("@risVersion", risVersion);
                command.Parameters.AddWithValue("@groupCode", this.GroupCode);
                command.ExecuteNonQuery();
                conn.Close();
            }
            OnProcessAfter("同步最大版本号完成");
        }


        /// <summary>
        /// 获得要升级的版本号和与之对应的文件名列表
        /// </summary>
        /// <param name="maxVersion"></param>
        /// <returns></returns>
        public SortedList<int, string> GetUpdateFileList(int maxVersion)
        {
            OnProcessBefore("开始获得升级脚本文件名列表");

            SortedList<int, string> result = new SortedList<int, string>();

            string sql = @"
Select RisVersion, ScriptFile From TB_UpdateScriptLib 
Where RisVersion>@risVersion Order by RisVersion
";
            using (var conn = new SqlConnection(this.ConnectionStringForPlatform))
            {
                conn.Open();
                var command = new SqlCommand(sql, conn);
                command.Parameters.Add(new SqlParameter("@risVersion", maxVersion));
                using (IDataReader rdr = command.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        int version = rdr.GetInt32(0);
                        string fileName = rdr.GetString(1);
                        fileName = Path.Combine(this.ScriptFileBaseFolder, fileName);
                        result.Add(version, fileName);
                    }
                }
            }

            OnProcessAfter("得到升级脚本文件个数：" + result.Count);

            return result;
        }


        /// <summary>
        /// 升级数据库
        /// </summary>
        private void UpdateDatabase()
        {
            int maxVersion = GetMaxVersion();
            SortedList<int, string> scriptList = GetUpdateFileList(maxVersion);

            if (scriptList.Count <= 0)
            {
                OnProcessBefore(string.Format("开始升级数据库：{0} 从版本{1}到{2}", this.DbName, maxVersion, maxVersion));
                OnProcessBefore("数据库不需要升级");
                return;
            }

            OnProcessBefore(string.Format("开始升级数据库：{0} 从版本{1}到{2}",
                this.DbName, maxVersion, scriptList.Keys[scriptList.Keys.Count - 1]));

            using (var conn = new SqlConnection(this.ConnectionString)) //todo：更改此处的连接串为Platform
            {
                conn.Open();
                SqlTransaction tran = null;
                try
                {
                    int curStep = 1;
                    tran = conn.BeginTransaction();
                    foreach (var item in scriptList)
                    {
                        DbProcessEventArgs ea = new DbProcessEventArgs();
                        ea.AllStep = scriptList.Count;
                        ea.CurStep = curStep;

                        ea.Message = string.Format("开始升级至：{0}，脚本文件为：{1}", item.Key, Path.GetFileName(item.Value));
                        OnProcessBefore(ea);

                        ExecuteSqlScriptFile(tran, item.Key, item.Value);

                        ea.Message = "当前脚本文件执行完成";
                        OnProcessBefore(ea);

                        maxVersion = item.Key;
                        curStep++;
                    }

                    SetMaxVersion(tran, maxVersion);

                    SetPlatformVersion(maxVersion);

                    tran.Commit();
                }
                catch
                {
                    tran.Rollback();
                    throw;
                }

            }

            OnProcessBefore("数据库升级完毕");
        }


        private void ExecuteSqlScriptFile(SqlTransaction tran, int version, string scriptFile)
        {
            List<string> sqlList = new List<string>();

            using (StreamReader reader = new StreamReader(scriptFile))
            {
                string statement = string.Empty;
                while ((statement = ReadNextStatementFromStream(reader)) != null)
                {
                    sqlList.Add(statement);
                }
            }
            string remberSql = string.Empty;
            try
            {
                SqlCommand command = new SqlCommand();
                command.Connection = tran.Connection;
                command.Transaction = tran;
                command.CommandType = CommandType.Text;
                foreach (string curSql in sqlList)
                {
                    remberSql = curSql;
                    command.CommandText = curSql;
                    command.ExecuteNonQuery();
                }
            }
            catch (SqlException ex)
            {
                throw new ApplicationException(remberSql, ex);
            }
        }

        private string ReadNextStatementFromStream(StreamReader reader)
        {
            StringBuilder sql = new StringBuilder();

            string curLine;

            while (true)
            {
                curLine = reader.ReadLine();
                if (curLine == null)
                {
                    if (sql.Length > 0)
                        return sql.ToString();
                    else
                        return null;
                }

                if (curLine.TrimEnd().ToUpper() == "GO") break;

                sql.Append(curLine + Environment.NewLine);
            }

            return sql.ToString();
        }
    }
}



///****** Object：  Table [dbo].[TB_UpdateScriptLib]    Script Date： 03/11/2011 00：40：18 ******/
//SET ANSI_NULLS ON
//GO

//SET QUOTED_IDENTIFIER ON
//GO

//SET ANSI_PADDING ON
//GO

//CREATE TABLE [dbo].[TB_UpdateScriptLib](
//    [Id] [int] IDENTITY(1,1) NOT NULL,
//    [RisVersion] [int] NOT NULL,
//    [ScriptFile] [varchar](100) NOT NULL,
//    [Memo] [nvarchar](250) NOT NULL,
//    [CreationDate] [smalldatetime] NOT NULL
//) ON [PRIMARY]

//GO

//SET ANSI_PADDING OFF
//GO

//ALTER TABLE [dbo].[TB_UpdateScriptLib] ADD  CONSTRAINT [DF_TB_UpdateScriptLib_CreationDate]  DEFAULT (getdate()) FOR [CreationDate]
//GO



