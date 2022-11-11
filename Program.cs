using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using CsvHelper;

namespace GenerateCSVForAllTablesInSQLServer
{
    /// <summary>
    /// Defines the <see cref="Program" />.
    /// </summary>
    internal class Program
    {
        /// <summary>
        /// The Main.
        /// </summary>
        /// <param name="args">The args<see cref="string[]"/>.</param>
        internal static void Main(string[] args)
        {
            string[] tables = { "TEST" };

            var datasource = @"."; //your server
            var database = "SysproCompanyU"; //your database name
            var username = "sa"; //username of server to connect
            var password = "@Admin123"; //password
            //your connection string
            var connString = @"Data Source=" + datasource + ";Initial Catalog="
                             + database + ";Persist Security Info=True;User ID=" + username + ";Password=" +
                             password;
            //create instanace of database connection

            var conn = new SqlConnection(connString);

            //create a new SQL Query using StringBuilder
            var strBuilder = new StringBuilder();

            //loop through each table to get the top 1000 records
            foreach (var table in tables)
            {
                //open connection
                conn.Open();

                strBuilder.Append("(Select top 1000 * from " + table + ") ");
                var sqlQuery = strBuilder.ToString();

                using (var command = new SqlCommand(sqlQuery, conn))

                {
                    command.CommandType = CommandType.Text;

                    var reader = command.ExecuteReader();

                    var dataList = new List<dynamic>();
                    var fileName = "";

                    if (reader.HasRows)
                    {
                        dataList = Read(reader).ToList();
                        fileName = @"C:\Users\Luna\Desktop\TableWithData\" + table + ".csv";
                    }
                    else
                    {
                        var tableSchema = reader.GetSchemaTable();

                        var expandoObject = new ExpandoObject() as IDictionary<string, object>;

                        // Each row in the table schema describes a column
                        foreach (DataRow row in tableSchema.Rows)
                            expandoObject.Add((string)row["ColumnName"], string.Empty);

                        dataList.Add(expandoObject);

                        fileName = @"C:\Users\Luna\Desktop\TablesEmpty\" + table + ".csv";
                    }

                    var directory = Path.GetDirectoryName(fileName);
                    if (!Directory.Exists(directory)) Directory.CreateDirectory(directory);

                    using (var writer = new StreamWriter(fileName))
                    using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                    {
                        csv.WriteRecords(dataList);
                        writer.ToString();
                    }
                }

                //close connection
                conn.Close();
                strBuilder.Clear();

                Console.WriteLine(tables + " successfully saved");
            }
        }


        private static dynamic ToExpando(IDataRecord record)
        {
            var expandoObject = new ExpandoObject() as IDictionary<string, object>;
            for (var i = 0; i < record.FieldCount; i++) expandoObject.Add(record.GetName(i), record[i]);
            return expandoObject;
        }

        private static IEnumerable<dynamic> Read(SqlDataReader reader)
        {
            while (reader.Read()) yield return ToExpando(reader);
        }
    }
}
