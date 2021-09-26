function transform_store_analysis(line) {

    var values = line.split(';');
    var obj = new Object();
	 var today = new Date().toISOString();
     
    obj.codigo = values[0];
    obj.descricao = values[1];
	obj.data_cadastro = today;   
    
    var jsonString = JSON.stringify(obj);

    return jsonString;
}