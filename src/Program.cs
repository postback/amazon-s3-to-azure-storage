using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace ExportFromAmazonS3ImportIntoAzureBlobStorage
{
    class Program
    {
        private static string azureStorageAccountName = ConfigurationManager.AppSettings["azureStorageAccountName"];
        private static string azureStorageAccountKey = ConfigurationManager.AppSettings["azureStorageAccountKey"];
        private static string azureBlobContainerName = ConfigurationManager.AppSettings["azureBlobContainerName"];
        private static string awsServiceUrl = ConfigurationManager.AppSettings["AWSServiceUrl"];
        private static string s3Bucket = ConfigurationManager.AppSettings["S3Bucket"];
        private static string awsAccessKey = ConfigurationManager.AppSettings["AWSAccessKey"];
        private static string awsSecretKey = ConfigurationManager.AppSettings["AWSSecretKey"];

        static void Main(string[] args)
        {
            var s3 = AWSClientFactory.CreateAmazonS3Client(
              awsAccessKey,
              awsSecretKey,
              new AmazonS3Config(){
                CommunicationProtocol = Protocol.HTTPS,
                ServiceURL = awsServiceUrl,
              }
              );

            var storageAccount = new Microsoft.WindowsAzure.Storage.CloudStorageAccount(
                   new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(
                       azureStorageAccountName
                       , azureStorageAccountKey)
                       , true);
            
            var blobClient = storageAccount.CreateCloudBlobClient();
            var blobContainer = blobClient.GetContainerReference(azureBlobContainerName);

            ExportAndImport(string.Empty,blobContainer,s3);

            Console.WriteLine("Press any key to exit");
            Console.ReadLine();
        }

        private static void ExportAndImport(string folder,CloudBlobContainer container, AmazonS3 s3)
        {
            var listRequest = new ListObjectsRequest{
                BucketName = ConfigurationManager.AppSettings["S3Bucket"],
            }.WithPrefix(folder);

            Console.WriteLine("Fetching all S3 object in " + folder);
            var s3response = s3.ListObjects(listRequest);
            
            //Checking if container exists, and creating it if not
            if (container.CreateIfNotExists()) {
                Console.WriteLine("Creating the blob container");
            }

            foreach (var s3Item in s3response.S3Objects)
            {
                if (s3Item.Key == folder) {
                    continue;
                }

                if (s3Item.Key.EndsWith("/"))
                {
                    ExportAndImport(s3Item.Key, container, s3);
                    continue;
                }

                Console.WriteLine("---------------------------------------------------");
                var blockBlob = container.GetBlockBlobReference(s3Item.Key);
                Console.WriteLine("Blob: " + blockBlob.Uri.AbsoluteUri);

                var id = blockBlob.StartCopyFromBlob(new Uri("http://" + awsServiceUrl + "/" + s3Bucket + "/" + HttpUtility.UrlEncode(s3Item.Key)), null, null, null);

                bool continueLoop = true;
                while (continueLoop && id == string.Empty)
                {
                    var copyState = blockBlob.CopyState;
                    if (copyState != null)
                    {
                        var percentComplete = copyState.BytesCopied / copyState.TotalBytes;
                        Console.WriteLine("Status of blob copy...." + copyState.Status + " " + copyState.TotalBytes + " of " + copyState.BytesCopied + "bytes copied. " + string.Format("{0:0.0%}", percentComplete));
                        if (copyState.Status != CopyStatus.Pending)
                        {
                            continueLoop = false;
                        }
                    }
                    System.Threading.Thread.Sleep(1000);
                }
            }
        }
    }
}
