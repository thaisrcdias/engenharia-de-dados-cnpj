using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Threading.Tasks;
using Amazon;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Transfer;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace LambdaS3
{
    public class Function
    {
        public async Task<MaisInformacoes> LambdaTest(ILambdaContext context)
        {
            var arquivo = new Arquivos();

            //empresas
            foreach (var empresa in arquivo.Empresas)
            {
                await arquivo.BaixarUpload(empresa);
            }

            //estabelecimento
            foreach (var estabelecimento in arquivo.Estabelecimento)
            {
                await arquivo.BaixarUpload(estabelecimento);
            }

            //simples
            await arquivo.BaixarUpload(arquivo.SimplesNacional);

            //cnae
            await arquivo.BaixarUpload(arquivo.Cnae);

            //municipio
            await arquivo.BaixarUpload(arquivo.Municipio);

            //naturezajuridica
            await arquivo.BaixarUpload(arquivo.NaturezaJuridica);


            var obj = new MaisInformacoes()
            {
                NameFunc = context.FunctionName
            };

            return obj;
        }
    }

    public class MaisInformacoes
    {
        public string NameFunc { get; set; }
        public DateTime Date { get; set; } = DateTime.Now;
    }

    public class Arquivos
    {
        public List<string> Empresas { get; set; } = new List<string>();
        public List<string> Estabelecimento { get; set; } = new List<string>();
        public string SimplesNacional { get; set; } = "http://200.152.38.155/CNPJ/F.K03200$W.SIMPLES.CSV.D10911.zip";
        public string Cnae { get; set; } = "http://200.152.38.155/CNPJ/F.K03200$Z.D10911.CNAECSV.zip";
        public string Municipio { get; set; } = "http://200.152.38.155/CNPJ/F.K03200$Z.D10911.MUNICCSV.zip";
        public string NaturezaJuridica { get; set; } = "http://200.152.38.155/CNPJ/F.K03200$Z.D10911.NATJUCSV.zip";

        public string Path { get; set; } = "D:\\DowaloadReceita\\";

        public Arquivos()
        {
            Empresas.Add("http://200.152.38.155/CNPJ/K3241.K03200Y0.D10911.EMPRECSV.zip");
            Empresas.Add("http://200.152.38.155/CNPJ/K3241.K03200Y1.D10911.EMPRECSV.zip");
            Empresas.Add("http://200.152.38.155/CNPJ/K3241.K03200Y2.D10911.EMPRECSV.zip");
            Empresas.Add("http://200.152.38.155/CNPJ/K3241.K03200Y3.D10911.EMPRECSV.zip");
            Empresas.Add("http://200.152.38.155/CNPJ/K3241.K03200Y4.D10911.EMPRECSV.zip");
            Empresas.Add("http://200.152.38.155/CNPJ/K3241.K03200Y5.D10911.EMPRECSV.zip");
            Empresas.Add("http://200.152.38.155/CNPJ/K3241.K03200Y6.D10911.EMPRECSV.zip");
            Empresas.Add("http://200.152.38.155/CNPJ/K3241.K03200Y7.D10911.EMPRECSV.zip");
            Empresas.Add("http://200.152.38.155/CNPJ/K3241.K03200Y8.D10911.EMPRECSV.zip");
            Empresas.Add("http://200.152.38.155/CNPJ/K3241.K03200Y9.D10911.EMPRECSV.zip");

            Estabelecimento.Add("http://200.152.38.155/CNPJ/K3241.K03200Y0.D10911.ESTABELE.zip");
            Estabelecimento.Add("http://200.152.38.155/CNPJ/K3241.K03200Y1.D10911.ESTABELE.zip");
            Estabelecimento.Add("http://200.152.38.155/CNPJ/K3241.K03200Y2.D10911.ESTABELE.zip");
            Estabelecimento.Add("http://200.152.38.155/CNPJ/K3241.K03200Y3.D10911.ESTABELE.zip");
            Estabelecimento.Add("http://200.152.38.155/CNPJ/K3241.K03200Y4.D10911.ESTABELE.zip");
            Estabelecimento.Add("http://200.152.38.155/CNPJ/K3241.K03200Y5.D10911.ESTABELE.zip");
            Estabelecimento.Add("http://200.152.38.155/CNPJ/K3241.K03200Y6.D10911.ESTABELE.zip");
            Estabelecimento.Add("http://200.152.38.155/CNPJ/K3241.K03200Y7.D10911.ESTABELE.zip");
            Estabelecimento.Add("http://200.152.38.155/CNPJ/K3241.K03200Y8.D10911.ESTABELE.zip");
            Estabelecimento.Add("http://200.152.38.155/CNPJ/K3241.K03200Y9.D10911.ESTABELE.zip");
        }

        public async Task BaixarUpload(string path)
        {
            using (WebClient webClient = new WebClient())
            {
                webClient.Headers.Add("Accept: text/html, application/xhtml+xml, */*");
                webClient.Headers.Add("User-Agent: Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)");

                var nome = string.Empty;
                var file = webClient.DownloadData(path);

                using (var client = new AmazonS3Client("", "", RegionEndpoint.USEast1))
                {
                    using (var newMemoryStream = new MemoryStream(file))
                    {
                        try
                        {
                            Stream unzippedEntryStream; // Unzipped data from a file in the archive
                            var outraStream = new MemoryStream();

                            ZipArchive archive = new ZipArchive(newMemoryStream);
                            foreach (ZipArchiveEntry entry in archive.Entries)
                            {
                                unzippedEntryStream = entry.Open();
                                nome = entry.FullName;
                                unzippedEntryStream.CopyTo(outraStream);
                                unzippedEntryStream.Dispose();
                            }

                            var uploadRequest = new TransferUtilityUploadRequest
                            {
                                InputStream = outraStream,
                                Key = nome,
                                BucketName = "a3datadesafio",
                                CannedACL = S3CannedACL.PublicRead
                            };

                            var fileTransferUtility = new TransferUtility(client);
                            await fileTransferUtility.UploadAsync(uploadRequest);

                            outraStream.Dispose();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }
                }
            }
        }

        public void BaixarArquivos()
        {
            using (WebClient webClient = new WebClient())
            {
                webClient.Headers.Add("Accept: text/html, application/xhtml+xml, */*");
                webClient.Headers.Add("User-Agent: Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)");

                foreach (var arquivo in Empresas)
                {
                    var nome = arquivo.Split('/');
                    webClient.DownloadFile(new Uri(arquivo), $"{Path}{nome[nome.Length - 1]}");
                }

                foreach (var arquivo in Estabelecimento)
                {
                    var nome = arquivo.Split('/');
                    webClient.DownloadFile(new Uri(arquivo), $"{Path}{nome[nome.Length - 1]}");
                }

                var simplesNacional = SimplesNacional.Split('/');
                webClient.DownloadFile(new Uri(SimplesNacional), $"{Path}{simplesNacional[simplesNacional.Length - 1]}");

                var nomeCnae = Cnae.Split('/');
                webClient.DownloadFile(new Uri(Cnae), $"{Path}{nomeCnae[nomeCnae.Length - 1]}");

                var municipio = Municipio.Split('/');
                webClient.DownloadFile(new Uri(Municipio), $"{Path}{municipio[municipio.Length - 1]}");

                var naturezaJuridica = NaturezaJuridica.Split('/');
                webClient.DownloadFile(new Uri(NaturezaJuridica), $"{Path}{naturezaJuridica[naturezaJuridica.Length - 1]}");


            }
        }

        public async Task UploadDirAsync()
        {
            try
            {
                string existingBucketName = "*** bucket name ***";
                string directoryPath = Path;

                string wildCard = "*.*";

                RegionEndpoint bucketRegion = RegionEndpoint.USWest2;
                IAmazonS3 s3Client = new AmazonS3Client(bucketRegion);

                var directoryTransferUtility = new TransferUtility(s3Client);

                await directoryTransferUtility.UploadDirectoryAsync(directoryPath, existingBucketName);
                Console.WriteLine("Upload parametro 1 completo");

                await directoryTransferUtility.UploadDirectoryAsync(
                                               directoryPath,
                                               existingBucketName,
                                               wildCard,
                                               SearchOption.AllDirectories);
                Console.WriteLine("Upload parametro 2 completo");


                var request = new TransferUtilityUploadDirectoryRequest
                {
                    BucketName = existingBucketName,
                    Directory = directoryPath,
                    SearchOption = SearchOption.AllDirectories,
                    SearchPattern = wildCard
                };

                await directoryTransferUtility.UploadDirectoryAsync(request);
                Console.WriteLine("Upload statement 3 completed");
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Erros encontrados. Mensagem:'{0}' quando foi escrever um objeto", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Erro desconhecido no servidor. Mensagem:'{0}' quando foi escrever um objeto", e.Message);
            }
        }
    }
}