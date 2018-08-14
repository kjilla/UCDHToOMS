using Microsoft.Azure.KeyVault;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace UCDDHourly2OMS
{
    public class Helper
    {

        private readonly static string _enrollmentNumber = "8099099";
        private readonly static bool _includeHeader = true;
        string baseurl = "https://consumption.azure.com/v2/enrollments";

        public int ProcessQuery(OMSIngestionApi oms, HttpClient client, String query, TraceWriter log)
        {
            try
            {
                JsonResult<UCDDHourly> jsonResult = default(JsonResult<UCDDHourly>);
                int count = 0;
                var tasks = new List<Task>();
                while (!string.IsNullOrEmpty(query))
                {
                    var response = client.GetAsync(new Uri(query)).Result;
                    if (response.IsSuccessStatusCode)
                    {
                        var result = response.Content.ReadAsStringAsync().Result;
                        jsonResult = JsonConvert.DeserializeObject<JsonResult<UCDDHourly>>(result);                        
                        var jsonList = JsonConvert.SerializeObject(jsonResult.data.ToList());
                        tasks.Add(oms.SendOMSApiIngestionFile(jsonList));
                        count = count + jsonResult.data.ToList().Count;
                        Console.WriteLine($"Count {count}");
                        query = jsonResult.nextLink;
                    }
                }
                Task.WaitAll(tasks.ToArray());
                log.Info($"Record Count {count}");
                return count;
            }
            catch (Exception e)
            {
                log.Info($"Failed processing. Reason: {e}");
                throw e;
            }            
        }

        public static AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();

        public async Task<String> GetAuthorizationHeader()
        {
            var token = await GetToken();
            //token.Dump();
            return $"Bearer {token}";
        }

        public async Task<string> GetToken()
        {
            const string SecretFormatString = "https://ccmsecrets.vault.azure.net/secrets/ccmaccesstoken/";
            var keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
            var result = await keyVaultClient.GetSecretAsync(SecretFormatString);
            return result.Value;
        }

        public string GetUsageQueryUrl()
        {
            //DateTime startDate = DateTime.Now;            
            //startDate = startDate.AddDays(-1);

            DateTime d1 = DateTime.Now;
            DateTime startDate = new DateTime(d1.Year, 08, 01, 0, 0, 0);
            DateTime endDate = new DateTime(d1.Year, 08, 31, 0, 0, 0);

            return $"{baseurl}/{_enrollmentNumber}/usagedetailsbycustomdate?startTime={startDate.ToShortDateString()}&endTime={endDate.ToShortDateString()}";
        }

        public WebRequestHandler SetHandler()
        {
            var handler = new WebRequestHandler();
            return handler;
        }

        public HttpClient SetClient(WebRequestHandler handler)
        {
            HttpClient httpClient = new HttpClient(handler);
            httpClient.DefaultRequestHeaders.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));
            var activityid = Guid.NewGuid().ToString();
            //activityid.Dump();
            if (_includeHeader)
            {
                httpClient.DefaultRequestHeaders.Add("x-ms-correlation-id", activityid);
                httpClient.DefaultRequestHeaders.Add("x-ms-tracking-id", Guid.NewGuid().ToString());
                string authHeader = GetAuthorizationHeader().Result;
                httpClient.DefaultRequestHeaders.Add("Authorization", authHeader);
            }


            return httpClient;
        }


        public HttpClient BuildClient()
        {
            var handler = SetHandler();
            var httpClient = SetClient(handler);
            return httpClient;
        }
    }

    public class JsonResult<T>
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("nextLink")]
        public string nextLink { get; set; }

        [JsonProperty("data")]
        public T[] data { get; set; }
    }
}
