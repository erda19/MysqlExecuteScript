using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MysqlExecuteScript
{
    class Program
    {
        static void Main(string[] args)
        {

            try
            {
                string constring =  System.Configuration.ConfigurationManager.AppSettings["DatabaseConnection"];
                string sqlFileDirectory = System.Configuration.ConfigurationManager.AppSettings["SqlFileDirectory"];
                MySqlConnection connection = new MySqlConnection(constring);
                connection.Open();

                if(string.IsNullOrEmpty(sqlFileDirectory.Trim()))
                {
                    sqlFileDirectory = System.Environment.CurrentDirectory;
                }

                if(!Directory.Exists(sqlFileDirectory))
                {
                    Console.WriteLine("Directory not exists : " + sqlFileDirectory);
                    Console.WriteLine("press any key  to close");
                    Console.ReadKey();
                    return;
                }

                var ext = new List<string> {".sql"};
                var myFiles = Directory.GetFiles(sqlFileDirectory, "*.*", SearchOption.AllDirectories)
                     .Where(s => ext.Contains(Path.GetExtension(s)));
                Console.CursorVisible = true;
                Console.CursorSize = 25;
                if (myFiles.Count() > 0)
                {
                    bool isFileExecuted = false;
                    Console.WriteLine("List Of sql files:");
                    int indexFile =  1;
                    foreach (var filename in myFiles)
                    {
                        FileInfo dataFile = new FileInfo(filename);
                        Console.WriteLine(indexFile + ". " + dataFile.Name);
                        indexFile++;
                    }

                    Console.WriteLine("Please enter (A) to execute all file. Or enter number " +
                                "from file for execute spesific file, " + System.Environment.NewLine +
                                "for example (1). " +
                                "If you want execute more than one spesific files, input with comma separator, for example (2,3,4).");

                    string executeMode = Convert.ToString(Console.ReadLine());

                    if (executeMode.ToLower().Trim() == "a")
                    {
                        foreach (var file in myFiles)
                        {
                            var task = Task.Run(() => ExecuteScript(connection, file));
                            Console.CursorVisible = false;
                            task.Wait();
                            isFileExecuted = true;
                        }
                    }
                    else
                    {
                        int indexFileExecute = 1;
                        string[] numberOfFile = executeMode.Split(',');
                        foreach (var file in myFiles)
                        {
                            if (numberOfFile.Contains(indexFileExecute.ToString()))
                            {
                                var task = Task.Run(() => ExecuteScript(connection, file));
                                Console.CursorVisible = false;
                                task.Wait();
                                isFileExecuted = true;
                            }
                            indexFileExecute++;
                        }

                    }

                    if(!isFileExecuted)
                    {
                        Console.WriteLine("no file that was executed!");
                    }

                }
                else
                {
                    Console.WriteLine("sql file not found");
                }
                connection.Close();
                Console.CursorVisible = true;
                while (Console.KeyAvailable)
                {
                    Console.ReadKey(false);
                }
                Console.WriteLine("press any key  to close");
                Console.ReadKey();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                Console.ReadLine();
                throw;
            }
        }


        private static async Task ExecuteScript(MySqlConnection connection, string file)
        {
            Stopwatch timer = new Stopwatch();
            timer.Start();
            FileInfo dataFile = new FileInfo(file);
            Console.WriteLine("start execute file : " + dataFile.Name);
            string text = await dataFile.OpenText().ReadToEndAsync();
            if (!Regex.IsMatch(text, @"\b(?i)USE\b"))
            {
                Console.WriteLine("Error when execute : " + dataFile.Name);
                Console.WriteLine("No database selected! ");
            }

            string cleanText = Regex.Replace(text, @"(DEFINER=)(\S+)", "");
            MySqlScript script = new MySqlScript(connection, cleanText);
            var execute = script.ExecuteAsync();
            timer.Stop();
            if (execute.Exception != null)
            {
                Console.WriteLine("Error when execute : " + dataFile.Name);
                Console.WriteLine(execute.Exception.ToString());
            }
            else
            {
                double spendTime = Convert.ToDouble(timer.ElapsedMilliseconds) / 1000;
                string spendUnit = "seconds";
                if(spendTime >= 60)
                {
                    spendTime = spendTime / 60 ;
                    spendUnit = "minutes";
                }

                Console.WriteLine("Success execute file " + dataFile.Name + " on "+ spendTime.ToString() + " " + spendUnit);
            }

            script.Query = "USE information_schema;";
            script.Execute();
        }


    }
}
