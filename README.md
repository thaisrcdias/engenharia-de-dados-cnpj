# engenharia-de-dados-cnpj
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
<br> Estrutura de Pastas</b>
     <ul><li>lambdaS3: Código da lambda function para obtenção dos dados no site da receita federal, que descompacta o afrquivo zip e salva no bucket do S3. </li>
         <li>cloud_function: Código da cloud function para acionamento do airflow.</li>
         <li>pipeline: Script para criação das tabelas, udf(user defined function). As udfs auxiliam na manipulação dos dados no Dataflow.</li>
         <li>script_criacao_views: Scripts SQL para criação das views que respondem as perguntas chaves do desafio.</li>
     </ul>  
<b>Relatórios
   Diponibilizados em: https://datastudio.google.com/s/iLCAUxfXSjw
