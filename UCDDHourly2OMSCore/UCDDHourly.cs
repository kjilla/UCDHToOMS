using Newtonsoft.Json;
using System;
using System.Collections.Generic;


namespace UCDDHourly2OMS
{
    [JsonObject]
    public class UCDDHourly
    {
        [JsonProperty]
        public string DepartmentName { get; private set; }

        [JsonProperty]
        public string CostCenter { get; private set; }

        [JsonProperty]
        public string UnitOfMeasure { get; private set; }

        [JsonProperty]
        public DateTime Date { get; private set; }

        [JsonProperty]
        public string AccountOwnerEmail { get; private set; }

        [JsonProperty]
        public string AccountName { get; private set; }

        [JsonProperty]
        public string ServiceAdministratorId { get; private set; }

        [JsonProperty]
        public string SubscriptionName { get; private set; }

        [JsonProperty]
        public string MeterCategory { get; private set; }

        [JsonProperty]
        public string MeterSubCategory { get; private set; }

        [JsonProperty]
        public string MeterRegion { get; private set; }

        [JsonProperty]
        public string MeterName { get; private set; }

        [JsonProperty]
        public string InstanceId { get; private set; }

        [JsonProperty]
        public string ServiceInfo1 { get; private set; }

        [JsonProperty]
        public string ServiceInfo2 { get; private set; }

        [JsonProperty]
        public long AccountId { get; private set; }

        [JsonProperty]
        public string AdditionalInfo { get; private set; }

        [JsonProperty]
        public long BillingMonth { get; private set; }

        [JsonProperty]
        public string ChannelType { get; private set; }

        [JsonProperty]
        public decimal ConsumedQuantity { get; private set; }

        [JsonProperty]
        public long ConsumedServiceId { get; private set; }

        [JsonProperty]
        public long DepartmentId { get; private set; }

        [JsonProperty]
        public long EnrollmentId { get; private set; }

        [JsonProperty]
        public decimal Cost { get; private set; }

        [JsonProperty]
        public string InstanceName { get; private set; }

        [JsonProperty]
        public bool IsMonetaryCommitmentService { get; private set; }

        [JsonProperty]
        public string MeterId { get; private set; }

        [JsonProperty]
        public long PartnerId { get; private set; }

        [JsonProperty]
        public long ProductId { get; private set; }

        [JsonProperty]
        public string Product { get; private set; }

        [JsonProperty]
        public string ResourceGroup { get; private set; }

        [JsonProperty]
        public string ResourceType { get; private set; }

        [JsonProperty]
        public long ResourceLocationId { get; private set; }

        [JsonProperty]
        public decimal ResourceRate { get; private set; }

        [JsonProperty]
        public string StoreServiceIdentifier { get; private set; }

        [JsonProperty]
        public long SubscriptionId { get; private set; }

        [JsonProperty]
        public string Tags { get; private set; }

        [JsonProperty]
        public long UsageChargeDetailDailyId { get; private set; }

        [JsonProperty]
        public long UsageDateId { get; private set; }

        [JsonProperty]
        public string SourceLocation { get; private set; }

        [JsonProperty]
        public string EnrollmentNumber { get; private set; }

        [JsonProperty]
        public string SubscriptionGuid { get; private set; }

        [JsonProperty]
        public string ConsumedService { get; private set; }

        [JsonProperty]
        public string ResourceLocation { get; private set; }

        [JsonProperty]
        public long Version { get; private set; }

        [JsonProperty]
        public decimal ConsumedQuantityScaled { get; private set; }

        [JsonProperty]
        public string PartNumber { get; private set; }

        [JsonProperty]
        public string ResourceGuid { get; private set; }

        [JsonProperty]
        public string OfferId { get; private set; }

        [JsonProperty]
        public long HourId { get; private set; }

        [JsonProperty]
        public DateTime TimeGenerated
        {
            get
            {
                return dateCreated.HasValue ? this.dateCreated.Value : DateTime.Now;
            }

            set { this.dateCreated = DateTime.Now; }
        }

        private DateTime? dateCreated = null;
    }
}
