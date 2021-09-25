merge trusted.estabelecimento as T
using
(
    select * except(rowNumber)
    from
    (
        select 
        coalesce(replace(cnpj_basico, '"', ''), '-1') as cnpj_basico, 
        coalesce(replace(cnpj_ordem, '"',''), 'Não Informado') as cnpj_ordem,
		coalesce(replace(cnpj_dv, '"',''), 'Não Informado') as cnpj_dv,
		coalesce(replace(identificador_matriz, '"',''), 'Não Informado') as identificador_matriz,
		coalesce(replace(replace(nome_fantasia, '"',''), '\'', ''), 'Não Informado') as nome_fantasia,
		coalesce(replace(situacao_cadastral, '"',''), 'Não Informado') as situacao_cadastral,
		
		case
			when replace(data_situacao_cadastral, '"','') = '0' or length(replace(data_situacao_cadastral, '"','')) < 8
				then null
			else
				PARSE_DATE("%Y%m%d",replace(data_situacao_cadastral, '"',''))
	    end as data_situacao_cadastral,

		coalesce(replace(cod_mtv_sit_cadastral, '"',''), '-1') as cod_mtv_sit_cadastral,
		coalesce(replace(nome_cidade_exterior, '"',''), 'Não Informado') as nome_cidade_exterior,
		coalesce(replace(cod_pais, '"',''), '-1') as cod_pais,
		
		case
			when replace(data_inicio_atividade, '"','') = '0' or length(replace(data_inicio_atividade, '"','')) < 8
				then null
			else
				PARSE_DATE("%Y%m%d",replace(data_inicio_atividade, '"',''))
	    end as data_inicio_atividade,
		
		coalesce(replace(cnae_fiscal_principal, '"',''), 'Não Informado') as cnae_fiscal_principal,
		coalesce(replace(cnae_fiscal_secundaria, '"',''), 'Não Informado') as cnae_fiscal_secundaria,
		coalesce(replace(tipo_logradouro, '"',''), 'Não Informado') as tipo_logradouro,
		coalesce(replace(logradouro, '"',''), 'Não Informado') as logradouro,
		coalesce(replace(numero, '"',''), 'Não Informado') as numero,
		coalesce(replace(complemento, '"',''), 'Não Informado') as complemento,
		coalesce(replace(bairro, '"',''), 'Não Informado') as bairro,
		coalesce(replace(cep, '"',''), 'Não Informado') as cep,
		coalesce(replace(uf, '"',''), 'Não Informado') as uf,
		coalesce(replace(cod_municipio, '"',''), '-1') as cod_municipio,
		coalesce(replace(ddd_um, '"',''), 'Não Informado') as ddd_um,
		coalesce(replace(telefone_um, '"',''), 'Não Informado') as telefone_um,
		coalesce(replace(ddd_dois, '"',''), 'Não Informado') as ddd_dois,
		coalesce(replace(telefone_dois, '"',''), 'Não Informado') as telefone_dois,
		coalesce(replace(ddd_fax, '"',''), 'Não Informado') as ddd_fax,
        coalesce(replace(fax, '"',''), 'Não Informado') as fax,
		coalesce(replace(correrio_eletronico, '"',''), 'Não Informado') as correrio_eletronico,
		coalesce(replace(situacao_especial, '"',''), 'Não Informado') as situacao_especial,
		case
			when replace(data_situ_especial, '"','') = '0' or length(replace(data_situ_especial, '"','')) < 8
				then null
			else
				PARSE_DATE("%Y%m%d",replace(data_situ_especial, '"',''))
	    end as data_situ_especial,
        row_number()over(partition by cnpj_basico order by data_cadastro desc) as rowNumber
        from `raw.estabelecimento`
    ) where rowNumber = 1
) as R 
on R.cnpj_basico = T.cnpj_basico
when matched 
then 
update set 
    T.cnpj_ordem = R.cnpj_ordem,
	T.cnpj_dv = R.cnpj_dv,
	T.identificador_matriz = R.identificador_matriz,
	T.nome_fantasia = R.nome_fantasia,
	T.situacao_cadastral = R.situacao_cadastral,
	T.data_situacao_cadastral = R.data_situacao_cadastral,
	T.cod_mtv_sit_cadastral = R.cod_mtv_sit_cadastral,
	T.nome_cidade_exterior = R.nome_cidade_exterior,
	T.cod_pais = R.cod_pais,
	T.data_inicio_atividade = R.data_inicio_atividade,
	T.cnae_fiscal_principal  = R.cnae_fiscal_principal,
	T.cnae_fiscal_secundaria = R.cnae_fiscal_secundaria,
	T.tipo_logradouro = R.tipo_logradouro,
	T.logradouro = R.logradouro,
	T.numero = R.numero,
	T.complemento = R.complemento,
	T.bairro = R.bairro,
	T.cep = R.cep,
	T.uf = R.uf,
	T.cod_municipio = R.cod_municipio,
	T.ddd_um = R.ddd_um,
	T.telefone_um = R.telefone_um,
	T.ddd_dois = R.ddd_dois,
	T.telefone_dois = R.telefone_dois,
	T.ddd_fax = R.ddd_fax,
	T.fax = R.fax,
	T.correrio_eletronico = R.correrio_eletronico,
	T.situacao_especial = R.situacao_especial,
	T.data_situ_especial = R.data_situ_especial,
  T.data_cadastro = current_timestamp() 
 when not matched 
 then
 insert 
 (
     cnpj_basico,
     cnpj_ordem,
	 cnpj_dv,
	 identificador_matriz,
	 nome_fantasia,
	 situacao_cadastral,
	 data_situacao_cadastral,
	 cod_mtv_sit_cadastral,
	 nome_cidade_exterior,
	 cod_pais,
	 data_inicio_atividade,
	 cnae_fiscal_principal,
	 cnae_fiscal_secundaria,
	 tipo_logradouro,
	 logradouro,
     numero,
	 complemento,
	 bairro,
	 cep,
	 uf,
	 cod_municipio,
	 ddd_um,
	 telefone_um,
	 ddd_dois,
	 telefone_dois,
	 ddd_fax,
	 fax,
	 correrio_eletronico,
	 situacao_especial,
	 data_situ_especial,
     data_cadastro

 )  
 values 
 (
	 R.cnpj_basico,
	 R.cnpj_ordem,
	 R.cnpj_dv,
	 R.identificador_matriz,
	 R.nome_fantasia,
	 R.situacao_cadastral,
	 R.data_situacao_cadastral,
	 R.cod_mtv_sit_cadastral,
	 R.nome_cidade_exterior,
	 R.cod_pais,
	 R.data_inicio_atividade,
	 R.cnae_fiscal_principal,
	 R.cnae_fiscal_secundaria,
	 R.tipo_logradouro,
	 R.logradouro,
	 R.numero,
	 R.complemento,
	 R.bairro,
	 R.cep,
	 R.uf,
	 R.cod_municipio,
	 R.ddd_um,
	 R.telefone_um,
	 R.ddd_dois,
	 R.telefone_dois,
	 R.ddd_fax,
	 R.fax,
	 R.correrio_eletronico,
	 R.situacao_especial,
	 R.data_situ_especial,
   current_timestamp()

 ) 