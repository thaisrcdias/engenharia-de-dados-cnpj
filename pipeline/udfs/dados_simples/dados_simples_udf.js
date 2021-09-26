function transform_store_analysis(line) {

    var values = line.split(';');
    var obj = new Object();
	 var today = new Date().toISOString();
     
    obj.cnpj_basico = values[0];
    obj.opcao_simples = values[1];
	obj.data_opcao_simples = values[2];
    obj.data_exclusao_simples = values[3];
    obj.opcao_mei = values[4];
    obj.data_opcao_mei = values[5];
    obj.data_exclusao_mei = values[6];
	
	obj.data_cadastro = today;   
    
    var jsonString = JSON.stringify(obj);

    return jsonString;
}