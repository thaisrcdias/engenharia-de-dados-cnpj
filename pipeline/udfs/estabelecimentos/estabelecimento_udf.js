function transform_store_analysis(line) {

    var values = line.split('";"');
    var obj = new Object();
    var today = new Date().toISOString();
     
    obj.cnpj_basico = values[0].replace('"\"','');
    obj.cnpj_ordem = values[1];
    obj.cnpj_dv = values[2];
    obj.identificador_matriz = values[3];
    obj.nome_fantasia = values[4];
    obj.situacao_cadastral = values[5];
    obj.data_situacao_cadastral = values[6];
    obj.cod_mtv_sit_cadastral = values[7];
	obj.nome_cidade_exterior = values[8];
    obj.cod_pais = values[9];
    obj.data_inicio_atividade = values[10];
    obj.cnae_fiscal_principal = values[11];
    obj.cnae_fiscal_secundaria = values[12];
    obj.tipo_logradouro = values[13];
	obj.logradouro = values[14];
    obj.numero = values[15];
    obj.complemento = values[16];
    obj.bairro = values[17];
    obj.cep = values[18];
    obj.uf = values[19];
    obj.cod_municipio = values[20];
    obj.ddd_um = values[21];
    obj.telefone_um = values[22];
    obj.ddd_dois = values[23];
    obj.telefone_dois = values[24];
    obj.ddd_fax = values[25];
    obj.fax = values[26];
    obj.correrio_eletronico = values[27];
    obj.situacao_especial = values[28];
    obj.data_situ_especial = values[29].replace('"\"','');
	
    obj.data_cadastro = today;   
    
    var jsonString = JSON.stringify(obj);

    return jsonString;
}