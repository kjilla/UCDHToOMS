﻿using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Globalization;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

namespace UCDDHourly2OMS
{
    public class OMSIngestionApi
    {
        private readonly string m_CustomerId;
        private readonly string m_SharedKey;
        private readonly TraceWriter m_log;
        private readonly RetryPolicy m_Retry;

        public OMSIngestionApi(string customerId, string sharedKey, TraceWriter log)
        {
            // Check the shared key is of a valid format
            Convert.FromBase64String(sharedKey);
            m_CustomerId = customerId;
            m_SharedKey = sharedKey;            
            m_Retry = RetryPolicy.DefaultFixed;
            m_log = log;
        }

        private string GetOMSApiSignature(string date, int contentLength)
        {
            var xHeaders = string.Format("x-ms-date:{0}", date);
            var stringToHash = string.Format("POST\n{0}\napplication/json\n{1}\n/api/logs", contentLength, xHeaders);

            var bytesToHash = Encoding.UTF8.GetBytes(stringToHash);
            var keyBytes = Convert.FromBase64String(m_SharedKey);

            using (var sha256 = new System.Security.Cryptography.HMACSHA256(keyBytes))
            {
                var calculatedHash = sha256.ComputeHash(bytesToHash);
                var encodedHash = Convert.ToBase64String(calculatedHash);
                var authorization = string.Format("SharedKey {0}:{1}", m_CustomerId, encodedHash);
                return authorization;
            }
        }

        public async Task SendOMSApiIngestionFile(string requestBody)
        {
            var method = "POST";
            var contentType = "application/json";
            var date = DateTime.UtcNow.ToString("r");

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            string address = string.Format("https://{0}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01", m_CustomerId);
            Uri uriAddress = new Uri(address);

            byte[] payload = Encoding.UTF8.GetBytes(requestBody);

            var signature = GetOMSApiSignature(date, payload.Length);

            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uriAddress);

            request.Method = method;
            request.ContentType = contentType;
            request.ContentLength = payload.Length;
            request.Headers.Add(HttpRequestHeader.Authorization, signature);
            request.Headers.Add("Log-Type", "UCDDHourly_CL");
            request.Headers.Add("x-ms-date", date);
            request.Headers.Add("time-generated-field", "EventTime");
                        
            m_log.Info($"Sending chunk ({payload.Length} bytes) to: {uriAddress}");

            // send request over the network
            using (Stream dataStream = await m_Retry.ExecuteAsync(() => request.GetRequestStreamAsync()))
            {
                await m_Retry.ExecuteAsync(() => dataStream.WriteAsync(payload, 0, payload.Length));
            }

            RestResponse response = await m_Retry.ExecuteAsync(() => GetResponse(request, stopwatch, payload.Length));

            if (!(response.StatusCode >= HttpStatusCode.OK && response.StatusCode < HttpStatusCode.Ambiguous))
            {                
                m_log.Info($"{method} to {address} failed with error {response.StatusDescription}");
                throw new HttpException((int)response.StatusCode, string.Format("{0} to {1} failed", method, address));
            }
        }

        private async Task<RestResponse> GetResponse(HttpWebRequest request, Stopwatch stopwatch, int payloadLength, int timeoutMilliseconds = 0)
        {
            WebResponse response = null;
            // get the response
            try
            {
                if (timeoutMilliseconds == 0)
                {
                    response = await m_Retry.ExecuteAsync(() => request.GetResponseAsync());
                }
                else
                {
                    var responseTask = m_Retry.ExecuteAsync(() => request.GetResponseAsync());
                    bool requestCompleted = responseTask.Wait(timeoutMilliseconds);
                    if (!requestCompleted)
                    {                       
                        m_log.Info($"GET request to {request.RequestUri} had reached timeout of {timeoutMilliseconds} ms");
                        request.Abort();
                        throw new HttpException((int)HttpStatusCode.RequestTimeout, string.Format("GET to {0} failed on timeout", request.RequestUri));
                    }
                    response = responseTask.Result;
                }
                HttpWebResponse httpResponse = (HttpWebResponse)response;
                string responseFromServer = null;
                using (Stream dataStream = response.GetResponseStream())
                {
                    StreamReader reader = new StreamReader(dataStream);
                    responseFromServer = await m_Retry.ExecuteAsync(() => reader.ReadToEndAsync());
                }

                stopwatch.Stop();

                return new RestResponse()
                {
                    SentBytes = payloadLength,
                    StatusDescription = httpResponse.StatusDescription,
                    ResponseFromServer = responseFromServer,
                    StatusCode = httpResponse.StatusCode,
                    Duration = stopwatch.Elapsed
                };
            }
            finally
            {
                if (response != null)
                {
                    response.Dispose();
                }
            }
        }

        private class RestResponse
        {
            public int SentBytes { get; set; }

            public string StatusDescription { get; set; }

            public string ResponseFromServer { get; set; }

            public HttpStatusCode StatusCode { get; set; }

            public TimeSpan Duration { get; set; }

            public override string ToString()
            {
                string escapedResponse = ResponseFromServer.Replace("{", "{{").Replace("}", "}}");
                return string.Format(
                    CultureInfo.InvariantCulture,
                    "Sent {0} bytes, status = {1}, status code = {2}, duration = {3}, response = '{4}'",
                    SentBytes,
                    StatusDescription,
                    StatusCode,
                    Duration,
                    escapedResponse);
            }
        }

    }
}
