using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
using Microsoft.Azure;
using databaseStateDictionary = System.Collections.Generic.Dictionary<string, UCDDHourly2OMS.SubfolderState>;
using serverStateDictionary = System.Collections.Generic.Dictionary<string, System.Collections.Generic.Dictionary<string, UCDDHourly2OMS.SubfolderState>>;
using StateDictionary = System.Collections.Generic.Dictionary<string, System.Collections.Generic.Dictionary<string, System.Collections.Generic.Dictionary<string, UCDDHourly2OMS.SubfolderState>>>;
using System.IO.Compression;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System.Net.Http;

namespace UCDDHourly2OMS
{
    public static class OMSIngestionProcessor
    {
        private static int totalLogs = 0;
        private const int DefaultRetryCount = 3;
        private static readonly string stateFileName = Path.Combine(GetLocalStorageFolder(), "states.json");
        private static readonly StateDictionary statesList = GetStates(stateFileName);
        private static List<string> auditLogProcessingFailures = new List<string>();
        private static string connectionString = "";
        private static string containerName = "";
        private static string workspaceid = "";
        private static string workspacekey = "";
        private static TraceWriter log;

        public static void StartIngestion(string storageAccountConnectionString, string blobContainerName, string omsWorkspaceId, string omsWorkspaceKey, TraceWriter logger)
        {
            connectionString = storageAccountConnectionString;
            containerName = blobContainerName;
            workspaceid = omsWorkspaceId;
            workspacekey = omsWorkspaceKey;
            log = logger;
            
            var oms = new OMSIngestionApi(workspaceid, workspacekey, log);

            if (CloudStorageAccount.TryParse(connectionString, out CloudStorageAccount storageAccount) == false)
            {
                log.Error($"Connection string can't be parsed: {connectionString}");
                return;
            }

            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
                       
            log.Info("Sending logs to OMS");

            IEnumerable<CloudBlobDirectory> servers = container.ListBlobs().OfType<CloudBlobDirectory>().ToList();
            foreach (var server in servers)
            {
                SendLogsFromServer(server, oms);
            }

            log.Info($"Finished processing files, saving the state file");
            File.WriteAllText(stateFileName, JsonConvert.SerializeObject(statesList));
        }

        public static void StartIngestion(string omsWorkspaceId, string omsWorkspaceKey, TraceWriter log)
        {          
            workspaceid = omsWorkspaceId;
            workspacekey = omsWorkspaceKey;   
           
            log.Info("Sending logs to OMS");

            var oms = new OMSIngestionApi(workspaceid, workspacekey, log);
            Helper helper = new Helper();
            HttpClient client = helper.BuildClient();
            int count = helper.ProcessQuery(oms,client, helper.GetUsageQueryUrl(),log);
            log.Info($"Finished processing files");
        }

        private static string GetLocalStorageFolder()
        {
            string path = Environment.GetEnvironmentVariable("HOME");
            return string.IsNullOrEmpty(path) ? "" : path + "\\site\\wwwroot\\";           
        }

        public static IEnumerable<List<T>> Chunk<T>(this List<T> source, int chunkSize)
        {
            int offset = 0;
            while (offset < source.Count)
            {
                yield return source.GetRange(offset, Math.Min(source.Count - offset, chunkSize));
                offset += chunkSize;
            }
        }

        private static void PrintHeaders(RequestEventArgs e)
        {
            if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.OK)
            {
                e.Request.Headers.Remove("Authorization");
                if (e.Response != null)
                {
                    log.Error($"Dumpping headers: Failed processing: {e.Request.Address}. Reason: { e.RequestInformation.Exception} \nHTTP Request:\n{e.Request.Headers}HTTP Response:\n{ e.Response.Headers}");                   
                }
                else
                {                    
                    log.Info($"Dumpping headers: Failed processing: {e.Request.Address}. Reason: {e.RequestInformation.Exception} \nHTTP Request:\n { e.Request.Headers}");
                }
            }
        }

        private static async Task<int> SendBlobToOMS(CloudBlob blob, int eventNumber, OMSIngestionApi oms)
        {
            RetryPolicy retryPolicy = new RetryPolicy(RetryPolicy.DefaultFixed.ErrorDetectionStrategy, DefaultRetryCount);

            log.Info($"Processing: {blob.Uri}");

            string fileName = Path.Combine(GetLocalStorageFolder(), Path.GetRandomFileName() + ".json.gz");
            string decompressedFileName = Path.Combine(GetLocalStorageFolder(), Path.GetRandomFileName() + ".json");

            try
            {
                OperationContext operationContext = new OperationContext();
                operationContext.RequestCompleted += (sender, e) => PrintHeaders(e);
                await retryPolicy.ExecuteAsync((() => blob.DownloadToFileAsync(fileName, FileMode.OpenOrCreate, null, null, operationContext)));
                List<UCDDHourly> list = new List<UCDDHourly>();
                var jsonSerializer = new JsonSerializer();

                FileInfo gzipFileName = new FileInfo(fileName);
                using (FileStream fileToDecompressAsStream = gzipFileName.OpenRead())
                {
                    using (FileStream decompressedStream = File.Create(decompressedFileName))
                    {
                        using (GZipStream decompressionStream = new GZipStream(fileToDecompressAsStream, CompressionMode.Decompress))
                        {
                            decompressionStream.CopyTo(decompressedStream);
                        }
                    }
                    using (var reader = new StreamReader(decompressedFileName))
                    using (var jsonReader = new JsonTextReader(reader))
                    {
                        jsonReader.SupportMultipleContent = true;
                        while (jsonReader.Read())
                        {
                            UCDDHourly ucddHourly = jsonSerializer.Deserialize<UCDDHourly>(jsonReader);
                            list.Add(ucddHourly);
                        }
                    }
                }

                IEnumerable<List<UCDDHourly>> chunkedList = list.Chunk(10000);
                foreach (List<UCDDHourly> chunk in chunkedList)
                {
                    var jsonList = JsonConvert.SerializeObject(chunk);
                    await oms.SendOMSApiIngestionFile(jsonList);
                    eventNumber += chunk.Count;
                    totalLogs += chunk.Count;
                }
            }
            catch (Exception e)
            {                
                log.Info($"Failed processing: {blob.Uri}. Reason: {e}");
                throw;
            }
            finally
            {
                try
                {
                    File.Delete(fileName);
                    File.Delete(decompressedFileName);
                }
                catch (Exception e)
                {                    
                    log.Info($"Was not able to delete file: {fileName}. Reason: {e.Message}");
                }
            }
            
            log.Info($"Done processing: {blob.Uri}");
            return eventNumber;
        }

        private static void SendLogsFromSubfolder(CloudBlobDirectory subfolder, databaseStateDictionary databaseState, OMSIngestionApi oms)
        {
            int nextEvent = 0;
            int eventNumber = 0;
            int datesCompareResult = -1;
            string currentDate = null;

            log.Info($"Processing sub folder: {subfolder.Prefix}");

            string subfolderName = new DirectoryInfo(subfolder.Prefix).Name;
            IEnumerable<CloudBlobDirectory> dateFolders = GetSubDirectories(subfolderName, subfolder, databaseState);
            var subfolderState = databaseState[subfolderName];
            string lastBlob = subfolderState.BlobName;
            DateTimeOffset? lastModified = subfolderState.LastModified;
            try
            {
                foreach (var dateFolder in dateFolders)
                {
                    currentDate = new DirectoryInfo(dateFolder.Prefix).Name;
                    datesCompareResult = string.Compare(currentDate, subfolderState.Date, StringComparison.OrdinalIgnoreCase);
                    //current folder is older than last state
                    if (datesCompareResult <= 0)
                    {
                        continue;
                    }

                    var tasks = new List<Task<int>>();

                    IEnumerable<CloudBlob> cloudBlobs = dateFolder.ListBlobs(useFlatBlobListing: true).OfType<CloudBlob>()
                        .Where(b => b.Name.EndsWith(".json.gz", StringComparison.OrdinalIgnoreCase)).ToList();

                    foreach (var blob in cloudBlobs)
                    {
                        string blobName = new FileInfo(blob.Name).Name;

                        if (datesCompareResult == 0)
                        {
                            int blobsCompareResult = string.Compare(blobName, subfolderState.BlobName, StringComparison.OrdinalIgnoreCase);
                            //blob is older than last state
                            if (blobsCompareResult < 0)
                            {
                                continue;
                            }

                            if (blobsCompareResult == 0)
                            {
                                if (blob.Properties.LastModified == subfolderState.LastModified)
                                {
                                    continue;
                                }
                                eventNumber = subfolderState.EventNumber;
                            }
                        }

                        tasks.Add(SendBlobToOMS(blob, eventNumber, oms));

                        lastBlob = blobName;
                        lastModified = blob.Properties.LastModified;
                        eventNumber = 0;
                    }

                    Task.WaitAll(tasks.ToArray());
                    if (tasks.Count > 0)
                    {
                        nextEvent = tasks.Last().Result;
                    }
                    subfolderState.BlobName = lastBlob;
                    subfolderState.LastModified = lastModified;
                    if (datesCompareResult >= 0)
                    {
                        subfolderState.Date = currentDate;
                    }

                    subfolderState.EventNumber = nextEvent;
                    log.Info($"Saving the state file after processing blobs{stateFileName}");
                    File.WriteAllText(stateFileName, JsonConvert.SerializeObject(statesList));                   
                }            
                log.Info($"Done processing sub folder: {subfolder.Prefix}");
            }
            catch (Exception e)
            {               
                log.Info($"Failed processing sub folder: {subfolder.Prefix}. Reason: {e}");
                UpdateFailuresLog(subfolder.Prefix, e);
            }
        }

        private static void SendLogsFromDatabase(CloudBlobDirectory databaseDirectory, serverStateDictionary serverState, OMSIngestionApi oms)
        {
            log.Info($"Processing audit logs for database: {databaseDirectory.Prefix}");

            try
            {
                string databaseName = new DirectoryInfo(databaseDirectory.Prefix).Name;
                IEnumerable<CloudBlobDirectory> subfolders = GetSubDirectories(databaseName, databaseDirectory, serverState);

                foreach (var subfolder in subfolders)
                {
                    SendLogsFromSubfolder(subfolder, serverState[databaseName], oms);
                }                
                log.Info($"Done processing audit logs for database: {0}");
            }
            catch (Exception e)
            {                
                log.Info($"Failed processing audit logs for database: {databaseDirectory.Prefix}. Reason: {e}");
                UpdateFailuresLog(databaseDirectory.Prefix, e);
            }

        }

        private static void SendLogsFromServer(CloudBlobDirectory serverDirectory, OMSIngestionApi oms)
        {
            log.Info($"Processing audit logs for server: {serverDirectory.Prefix}");
            try
            {
                string serverName = new DirectoryInfo(serverDirectory.Prefix).Name;
                IEnumerable<CloudBlobDirectory> databases = GetSubDirectories(serverName, serverDirectory, statesList);

                foreach (var database in databases)
                {
                    SendLogsFromDatabase(database, statesList[serverName], oms);
                }

                log.Info($"Done processing audit logs for server: {serverDirectory.Prefix}");
                
            }
            catch (Exception e)
            {
                log.Info("Failed processing audit logs for server: {serverDirectory}. Reason: {e}");

                UpdateFailuresLog(serverDirectory.Prefix, e);
            }
        }

        private static IEnumerable<CloudBlobDirectory> GetSubDirectories<T>(string directoryName, CloudBlobDirectory directory, IDictionary<string, T> dictionary) where T : new()
        {
            if (!dictionary.ContainsKey(directoryName))
            {
                dictionary.Add(directoryName, new T());
            }

            return directory.ListBlobs().OfType<CloudBlobDirectory>().ToList().OrderBy(p => p.Prefix);
        }

        private static StateDictionary GetStates(string fileName)
        {
            StateDictionary statesList;
            if (!File.Exists(fileName))
            {
                statesList = new StateDictionary();
            }
            else
            {
                using (StreamReader file = File.OpenText(fileName))
                {
                    JsonSerializer serializer = new JsonSerializer();
                    statesList = (StateDictionary)serializer.Deserialize(file, typeof(StateDictionary));
                }
            }

            return statesList;
        }

        private static void UpdateFailuresLog(string resource, Exception ex)
        {
            string failureMessage = string.Format("Failed processing audit logs for: {0}. Reason: {1}", resource, ex.Message);
            auditLogProcessingFailures.Add(failureMessage);
        }
    }
}
