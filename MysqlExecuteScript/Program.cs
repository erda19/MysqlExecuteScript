using MySql.Data.MySqlClient;
using System;
using System.Collections;
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
        private static string connectionString;
        private static int cursorTop; 
        static void Main(string[] args)
        {

            try
            {
                connectionString = System.Configuration.ConfigurationManager.AppSettings["DatabaseConnection"];
                string sqlFileDirectory = System.Configuration.ConfigurationManager.AppSettings["SqlFileDirectory"];

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

                var arrayFile = myFiles.ToArray();

                Console.CursorVisible = true;
                Console.CursorSize = 25;
                if (myFiles.Count() > 0)
                {
                    int totalFileExecuted = 0;
                    Console.WriteLine("List Of sql files:");
                    int indexFile =  1;
                    foreach (var filename in arrayFile)
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

                    ParallelOptions parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = 5 };

                    if (executeMode.ToLower().Trim() == "a")
                    {
                        var taskParallel = Task.Run(() =>

                            Parallel.ForEach(arrayFile, parallelOptions,
                                 file =>
                            {
                                ExecuteScript(file);
                                totalFileExecuted++;
                            })
                        );
                        taskParallel.Wait();

                    }
                    else
                    {
                        //int indexFileExecute = 1;
                        string[] arrayFileNumber = executeMode.Split(',');
                        var taskParallel = 
                            Task.Run(() =>
                            Parallel.ForEach(arrayFile
                                        , parallelOptions
                                        , (file, state, index) =>
                                        {
                                            if(arrayFileNumber.Contains((index + 1).ToString()))
                                            {
                                                ExecuteScript(file);
                                                totalFileExecuted++;
                                            }
                                        })
                                    );
                        taskParallel.Wait();

                    }
                    Console.SetCursorPosition(0, Console.CursorTop);
                    if (totalFileExecuted == 0)
                    {
                        Console.WriteLine("No file that was executed!");
                    }
                    else if (totalFileExecuted > 1)
                    {
                        Console.WriteLine("Success execute all files!");
                    }

                }
                else
                {
                    Console.WriteLine("Sql file not found");
                }

                Console.CursorVisible = true;
                while (Console.KeyAvailable)
                {
                    Console.ReadKey(false);
                }
                Console.WriteLine("Press any key  to close");
                Console.ReadKey();

            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                Console.ReadLine();
                throw;
            }
        }


        private static void ExecuteScript(string file)
        {
            MySqlConnection connection = new MySqlConnection(connectionString);
            string fileName = "";
            try
            {
                connection.Open();
                Stopwatch timer = new Stopwatch();
                timer.Start();
                FileInfo dataFile = new FileInfo(file);
                fileName = dataFile.Name;
                string messageExecuteFile = "Execute file " + fileName;
                //int cursorTop = Console.CursorTop;
                //Console.SetCursorPosition(0, cursorTop + 1);
                Console.WriteLine(messageExecuteFile);
                string text = dataFile.OpenText().ReadToEnd();
                if (!Regex.IsMatch(text, @"\b(?i)USE\b"))
                {
                    Console.WriteLine("Error when execute : " + fileName);
                    Console.WriteLine("No database selected! ");
                }

                string cleanText = Regex.Replace(text, @"(DEFINER=)(\S+)", "");
                MySqlScript script = new MySqlScript(connection, cleanText);

                //script.StatementExecuted += new MySqlStatementExecutedEventHandler(script_StatementExecuted);

                var execute = script.ExecuteAsync();

                timer.Stop();
                if (execute.Exception != null)
                {
                    Console.WriteLine("Error when execute : " + fileName);
                    Console.WriteLine(execute.Exception.ToString());
                }
                else
                {
                    double spendTime = Convert.ToDouble(timer.ElapsedMilliseconds) / 1000;
                    string spendUnit = "seconds";
                    if (spendTime >= 60)
                    {
                        spendTime = spendTime / 60;
                        spendUnit = "minutes";
                    }

                    Console.WriteLine("Success execute file " + fileName + " on " + spendTime.ToString() + " " + spendUnit);
                    //Console.CursorLeft = 2;
                    //Console.WriteLine(messageExecuteFile + " [Done] Elapsed Time " + spendTime.ToString() + " " + spendUnit);
                }

                script.Query = "USE information_schema;";
                script.Execute();
            }
            catch(Exception e)
            {
                Console.WriteLine("Error when execute : " + fileName);
                Console.WriteLine(e.ToString());
            }
            finally
            {
                connection.Close();
            }
        }

        private static void script_StatementExecuted(object sender, MySqlScriptEventArgs args)
        {
            Console.WriteLine(args.StatementText);
        }


    }
}
