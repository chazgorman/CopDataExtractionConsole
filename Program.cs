using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Net;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CopDataExtractionConsole
{
    class Program
    {
        private static StreamWriter logStream;

        private static string CopDataUrlEnglishString = "copDataUrlEn";
        private static string CopDataUrlFrenchString = "copDataUrlFr";

        class FieldType
        {
            public string name { get; set; }
            public string type { get; set; }
            public string alias { get; set; }
            public int length { get; set; }
        };

        private static string[] layerNamesEn;

        private static string[] layerNamesFr;

        static void Main(string[] args)
        {
            try
            {
                var appSettings = ConfigurationManager.AppSettings;

                if(appSettings["fileLoggingEnabled"] == bool.TrueString)
                {
                    logStream = File.CreateText("log.txt");
                }
                else
                {
                    logStream = new StreamWriter(Console.OpenStandardOutput());
                    logStream.AutoFlush = true;
                    Console.SetOut(logStream);
                }

                if(appSettings["promptForRun"] == bool.TrueString)
                {
                    Console.WriteLine("Press enter to start...");
                    Console.ReadLine();
                }

                Log("Reading english layer names from configuration file");
                string engLayerNames = appSettings["englishLayerNamesList"];
                layerNamesEn = engLayerNames.Split(',');

                Log("Reading french layer names from configuration file");
                string frLayerNames = appSettings["frenchLayerNamesList"];
                layerNamesFr = frLayerNames.Split(',');

                var queryUrlEng = appSettings[CopDataUrlEnglishString];
                var queryUrlFr = appSettings[CopDataUrlFrenchString];

                createOutputDirectories();

                Log("Getting Json layer data for English layers");
                getLayers(queryUrlEng, layerNamesEn, "outputJsonDirectoryEn");

                Log("Getting Json layer data for French layers");
                getLayers(queryUrlFr, layerNamesFr, "outputJsonDirectoryFr");

                Log("Converting Json layer data for English layers to CSV");
                convertToCSV(appSettings["outputJsonDirectoryEn"]);

                Log("Converting Json layer data for French layers to CSV");
                convertToCSV(appSettings["outputJsonDirectoryFr"]);

                if (appSettings["promptForRun"] == bool.TrueString)
                {
                    Console.WriteLine("Press enter to close...");
                    Console.ReadLine();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            {
                logStream.Close();
            }   
        }

        public static void Log(string logMessage)
        {
            logStream.Write("\r\nLog Entry : ");
            logStream.WriteLine("{0} {1}", DateTime.Now.ToLongTimeString(),
                   DateTime.Now.ToLongDateString());
            logStream.WriteLine("  :");
            logStream.WriteLine("  :{0}", logMessage);
            logStream.WriteLine("-------------------------------");
        }

        private static void createOutputDirectories()
        {
            Directory.CreateDirectory(ConfigurationManager.AppSettings["outputJsonDirectoryEn"]);
            Directory.CreateDirectory(ConfigurationManager.AppSettings["outputJsonDirectoryFr"]);
        }

        static void getLayers(string url, string[] layerNames, string outputDirectory)
        {
            var appSettings = ConfigurationManager.AppSettings;

            string logRequestUrl = string.Empty;

            string[] copLayerConfigs = appSettings["copLayers"].Split(',');
            List<string> copLayers = new List<string>();

            foreach (string config in copLayerConfigs)
            {
                if (config.Contains("-"))
                {
                    string[] rangeLayers = config.Split('-');
                    int start = int.Parse(rangeLayers[0]);
                    int end = int.Parse(rangeLayers[1]);

                    for(int layer = start; layer <= end; layer++)
                    {
                        copLayers.Add(layer.ToString());
                    }
                }
                else
                {
                    copLayers.Add(config.ToString());
                }
            }

            try
            {
                copLayers.ForEach(delegate(string currentLayer)
                {
                    var intCurrentLayer = int.Parse(currentLayer);
                    var requestUrl = logRequestUrl = url + "/" + currentLayer + "/query?where=1=1&f=json&outFields=*";
                    Log("Requesting Json layer data at URL: " + logRequestUrl);

                    WebRequest request = WebRequest.Create(requestUrl);
                    request.Method = "GET";

                    WebResponse response = request.GetResponse();
                    Stream stream = response.GetResponseStream();
                    using (var memoryStream = new MemoryStream())
                    {
                        stream.CopyTo(memoryStream);
                        byte[] buffer = memoryStream.ToArray();
                        string responseString = System.Text.Encoding.UTF8.GetString(buffer);

                        string filePath = appSettings[outputDirectory] + "\\" + layerNames[intCurrentLayer - 1] + ".json";
                        using (FileStream fileStream = File.Create(filePath, buffer.Length))
                        {
                            fileStream.Write(buffer, 0, buffer.Length);
                        }
                    }
                });
            }
            catch(Exception e)
            {
                Log("Exception occurred while requesting Json layer data at URL: " + logRequestUrl + ": " + e.Message);
            }
        }

        static void convertToCSV(string directory)
        {
            IEnumerable<string> files = Directory.EnumerateFiles(directory);
            var csvDirectory = directory + "\\csv";

            // Create output CSV directory if it doesn't exist yet
            if (!Directory.Exists(csvDirectory)){
                Directory.CreateDirectory(csvDirectory);
            }

            foreach(string file in files)
            {
                convertJsonFile(file);
            }
        }

        static void convertJsonFile(string file)
        {
            List<List<string>> csvLineList = new List<List<string>>();
            List<List<string>> rows = new List<List<string>>();

            try
            {
                using (TextReader textReader = File.OpenText(file))
                {
                    // Read JSON feature file into a JObject
                    JsonTextReader jsonReader = new JsonTextReader(textReader);
                    JObject layerObject = JObject.Parse(textReader.ReadToEnd());

                    // Read out field list to get field types
                    JToken fieldToken = layerObject.SelectToken("$.fields");
                    JEnumerable<JToken> fieldTokens = fieldToken.Children();

                    // Read out feature list
                    JToken featureToken = layerObject.SelectToken("$.features");
                    JEnumerable<JToken> featureTokens = featureToken.Children();

                    List<FieldType> fieldList = new List<FieldType>();
                    Dictionary<string, FieldType> fieldDictionary = new Dictionary<string, FieldType>();
                    List<string> fieldRow = new List<string>();

                    foreach (JToken childFieldToken in fieldTokens)
                    {
                        FieldType field = childFieldToken.ToObject<FieldType>();
                        fieldList.Add(field);

                        fieldDictionary.Add(field.name, field);
                        fieldRow.Add(field.name);
                    }

                    rows.Add(fieldRow);

                    foreach (JToken childFeatureToken in featureTokens)
                    {
                        List<string> rowData = new List<string>();

                        JToken attributeToken = childFeatureToken.SelectToken("$.attributes");

                        foreach (JProperty featureAttributeValue in attributeToken.Children<JProperty>())
                        {
                            var name = featureAttributeValue.Name;
                            var value = featureAttributeValue.Value;
                            string valueString = null;
                            FieldType fieldType = fieldDictionary[name];

                            switch (fieldType.type)
                            {
                                case "esriFieldTypeString":
                                case "esriFieldTypeDate":
                                case "esriFieldTypeGUID":
                                    valueString = "\"" + value.ToString().Trim() + "\"";
                                    break;
                                case "esriFieldTypeBlob":
                                case "esriFieldTypeRaster":
                                case "esriFieldTypeXML":
                                case "esriFieldTypeGeometry":
                                    valueString = "";
                                    break;
                                case "esriFieldTypeDouble":
                                case "esriFieldTypeSingle":
                                case "esriFieldTypeInteger":
                                case "esriFieldTypeSmallInteger":
                                case "esriFieldTypeOID":
                                case "esriFieldTypeGlobalID":
                                    valueString = value.ToString();
                                    break;
                                default:
                                    valueString = value.ToString();
                                    break;
                            }

                            rowData.Add(valueString);
                        }
                        rows.Add(rowData);
                    }
                };
            }
            catch(Exception e)
            {
                Log("Exception occurred trying to build CSV rows from file: " + file + ": " + e.Message);
            }

            StringBuilder csvBuilder = new StringBuilder();

            try
            {
                rows.ForEach(delegate (List<string> rowData)
                {
                    csvBuilder.AppendLine(string.Join(",", rowData));
                });
            }
            catch(Exception e)
            {
                Log("Exception occurred trying to append CSV rows from file: " + file + ": " + e.Message);
            }

            FileInfo fileInfo = new FileInfo(file);
            string outputCsvFile = fileInfo.Directory.FullName + "\\csv" + "\\" + fileInfo.Name + ".csv";
            try
            {
                File.WriteAllText(outputCsvFile, csvBuilder.ToString());
            }
            catch(Exception e)
            {
                Log("Exception occurred trying to write CSV file: " + outputCsvFile + ": " + e.Message);
            }
        }
    }
}
