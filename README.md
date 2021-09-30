# Hackathon de Engenharia de Dados - A3 Data Challenge Women


Implementação de um pipeline de extração, transformação e disponibilização de dados da base pública de cnpj.
Após extração, limpeza, organização e estruturação dos dados, as perguntas chave do desafio devem ser respondidas de maneira visual.

Fonte de dados: https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj

Perguntas Chave
1. Número de indústrias ativas por mês/ano entre 2010 - 2021, discriminado por MEI ou Simples, em cada município brasileiro.
2. Número de comércios que fecharam por mês/ano entre 2010 - 2021, discriminado por MEI ou Simples, em cada município brasileiro.
3. Número de CNPJ novos por mês/ano entre 2010 - 2021, discriminado por MEI ou Simples, em cada município brasileiro
4. Qual o número de CNPJ que surgiram do grupo de educação superior, entre 2015 e 2021, discriminado por ano, em cada estado brasileiro?
5. Qual a classe de CNAE com o maior capital social médio no Brasil no último ano?
6. Qual a média do capital social das empresas de pequeno porte por natureza  jurídica no último ano?

Relatórios Diponibilizados em: https://datastudio.google.com/s/iLCAUxfXSjw

<hr size=1>

<b>Arquitetura do Projeto</b>

![image](https://user-images.githubusercontent.com/69485358/134758443-3449806f-935c-43c2-b749-52521ca1cf07.png)

1. Extração da fonte de dados. Para isso foi criada uma lambda function na AWS, que possui agendamento pelo airflow. A fonte de dados pública do CNPJ possui periodicidade de atualização mensal, com isso a carga de dados será feita mensalmente.
Essa lambda function faz download dos arquivos e salva em um bucket no S3.<br>
   <ul>Observações: 
       <li> Antes da extração dos dados, foi analisada as perguntas chaves e layout dos campos dos arquivos, para o download apenas dos arquivos necessários. </li>
       <li>  Inicialmente seriam usados recursos da cloud da AWS no projeto, mas com o intuito de minimização de custos, os arquivos baixados foram migrados para a Cloud Storage da GCP (Google Cloud Plataform).</li>
   </ul>

2. Para a migração dos arquivos foi utilizado o serviço Transfer da GCP, onde é feito o espelhamento dos dados.
3. Foi criada uma cloud function da GCP, que será acionada pelo evento, de sempre que um novo arquivo for salvo no Storage, ela será acionada e a partir dela são acionadas as DAGs no Airflow, que foi escolhida como nossa ferramenta de orquestração do pipeline de dados.
4. A partir disso será realizada a ingestão de dados no Big Query, que será nosso lakehouse, que é uma junção de data lake com DW. No Big Query foram criadas as seguintes camadas: Raw (onde o dado "bruto" é salvo),  Trusted (onde o dado é salvo, após as devidas limpezas) e Semantic, que é a camada onde possui as Views, para serem consumidas no relatório final.

<br>
<b>Pitch da Solução:</b> https://www.youtube.com/watch?v=7vvRjtoIS_Q
<br>
<hr>
<br>
<b> Linguagens Utilizadas</b>
<ul>
   <li>Python (criação das DAGs e cloud function) </li>
   <li>SQL (Views e transformação dos dados)</li>
   <li>C# (lambda function)</li>
   <li>Javascript para criação das UDF (User Defined Function) que são funções definidas pelo usuário para manipulação dos dados no Dataflow </li>
</ul>
<br>
<b>Tratamento dos Dados</b>
<br>
<br>
<b>Características da Solução: </b>
<br>
<br>
<ul> <b>Escalabilidade:</b>
   <li>Google Cloud Storage: Altamente escalável para grande volume de dados e distribuído entre zonas.</li>
   <li> Uso do Big Query</li>
   <li> Airflow executando com três nós no Kubernates, ou seja, com três workers no ambiente </li>
   <li> Dataflow, que proporciona escalonamento automático horizontal de recursos de workers para maximizar a utilização de recursos </li>
</ul>
<ul> <b>Confiabilidade</b>
   <li> Google Cloud Storage: Altamente escalável para grande volume de dados e distribuído entre zonas.</li>
   <li> Dag Retry: Em caso de falha em algum dos passos do pipeline no Airflow, o retry está habilitado, para o job ser executado novamente.</li>
   <li> Observabilidade: Todo pipeline pode ser monitorado via interface do Airflow.</li>
</ul>
<ul> <b>Facilidade de Integração</b>
   <li> Versionamento no GIT (branch de master e develop)</li>
   <li> CI/CD para deploy em ambiente de produção (segunda fase projeto) </li>
</ul>
<ul> <b>Eficiência Operacional</b>
   <li> Big Query: Permite consultas de gtande volume de dados usando SQL, em segundos</li>
   <li> Aquisição de Dados (camada Landing) desacoplada da camada de ingestão de dados (Lakehouse) </li>
</ul>
<ul> <b>Otimização de Custos</b>
   <li> Armazenamento da fonte de dados na Cloud Storage da GCP</li>
   <li> Utilização do cluster do Dataflow, que é desligado quando não está sendo utilizado e escalabilidade máxima configurada para 4 nós (para arquivo de empresa e estabelecimento) e 1 nó para ao restante</li>
   <li> Particionamento das tabelas, por dia, no Big Query</li>
</ul>

<br>
<b> Estrutura de Pastas</b>
     <ul><li>lambdaS3: Código da lambda function para obtenção dos dados no site da receita federal, que descompacta o afrquivo zip e salva no bucket do S3. </li>
         <li>cloud_function: Código da cloud function para acionamento do airflow.</li>
         <li>pipeline: Script para criação das tabelas, udf(user defined function). As udfs auxiliam na manipulação dos dados no Dataflow.</li>
         <li>script_criacao_views: Scripts SQL para criação das views que respondem as perguntas chaves do desafio.</li>
     </ul>  

