using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System.Diagnostics;
using Microsoft.Azure;
using System.Collections.Generic;
using UCDDHourly2OMS;

namespace UCDDToOMSFunc
{
    public static class UCDDHourlyToOMS
    {        
        static List<string> auditLogProcessingFailures = new List<string>();

        [FunctionName("UCDDHourlyToOMS")]
        //public static void Run([TimerTrigger("0 0 */1 * * *")]TimerInfo myTimer, TraceWriter log)
        public static void Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, TraceWriter log)
        {
            log.Info($"C# Timer trigger function executed at: {DateTime.Now}");            
            try
            {               
                string customerId = CryptoHelper.GetKeyVaultSecret("omsworkspaceid");
                string sharedKey = CryptoHelper.GetKeyVaultSecret("omsworkspacekey");              
                log.Info($"Processing started at {DateTime.UtcNow.ToString()}");                
                OMSIngestionProcessor.StartIngestion(customerId, sharedKey, log);
                log.Info($"Finished processing at  {DateTime.UtcNow.ToString()}");               
            }
            catch (Exception ex)
            {                
                log.Info($"Error: {ex}");
                UpdateFailuresLog("UCDDHourly2OMSConsole", ex);
            }
            finally
            {
                if (auditLogProcessingFailures.Count > 0)
                {
                    log.Info($"Processing ucddHourly failed during the operation:\n{string.Join(Environment.NewLine, auditLogProcessingFailures)}");                   
                }
            }
        }

        private static void UpdateFailuresLog(string resource, Exception ex)
        {
            string failureMessage = string.Format("Failed processing audit logs for: {0}. Reason: {1}", resource, ex.Message);
            auditLogProcessingFailures.Add(failureMessage);
        }
    }
}
