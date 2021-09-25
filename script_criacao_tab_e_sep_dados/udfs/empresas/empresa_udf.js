function transform_store_analysis(line) {

    var values = line.split(';');
    var obj = new Object();
	 var today = new Date().toISOString();
     
    obj.cnpj_basico = values[0];
    obj.razao_social = values[1];
	obj.natureza_juridica = values[2];
    obj.qualificacao_responsavel = values[3];
    obj.capital_social = values[4];
    obj.porte_empresa = values[5];
    obj.ente_federativo = values[6];
	
	obj.data_cadastro = today;   
    
    var jsonString = JSON.stringify(obj);

    return jsonString;
}