using Microsoft.Azure.KeyVault;
using Microsoft.Azure.Services.AppAuthentication;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Azure;

namespace UCDDToOMSFunc
{
    class CryptoHelper
    {
        static AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();
        public static string GetKeyVaultSecret(string secretNode)
        {
            var secretUri = string.Format("{0}{1}", CloudConfigurationManager.GetSetting("KeyVaultURL"), secretNode);

            var keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
            return keyVaultClient.GetSecretAsync(secretUri).Result.Value;
        } 
    }
}
