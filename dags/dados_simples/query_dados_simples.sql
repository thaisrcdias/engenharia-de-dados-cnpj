merge trusted.dados_simples as T
using
(
    select * except(rowNumber)
    from
    (
        select 
        coalesce(replace(cnpj_basico, '"', ''), '-1') as cnpj_basico, 
        coalesce(replace(opcao_simples, '"',''), 'Não Informado') as opcao_simples,
		case
			when replace(data_opcao_simples, '"','') = '00000000'
				then null
			else
				PARSE_DATE("%Y%m%d",replace(data_opcao_simples, '"',''))
	    end as data_opcao_simples,
		
	    case
			when replace(data_exclusao_simples, '"','') = '00000000' or LENGTH(replace(data_exclusao_simples, '"','')) < 8
				then null
			else
				PARSE_DATE("%Y%m%d",replace(data_exclusao_simples, '"',''))
	    end as data_exclusao_simples,
		
	
		coalesce(replace(opcao_mei, '"',''), 'Não Informado') as opcao_mei,
		
		case
			when replace(data_opcao_mei, '"','') = '00000000'
				then null
			else
				PARSE_DATE("%Y%m%d",replace(data_opcao_mei, '"',''))
	    end as data_opcao_mei,
		
		case
			when replace(data_exclusao_mei, '"','') = '00000000'
				then null
			else
				PARSE_DATE("%Y%m%d",replace(data_exclusao_mei, '"',''))
	    end as data_exclusao_mei,
		
        row_number()over(partition by cnpj_basico order by data_cadastro desc) as rowNumber
        from `raw.dados_simples`
    ) where rowNumber = 1
) as R 
on R.cnpj_basico = T.cnpj_basico
when matched 
then 
update set 
    T.opcao_simples = R.opcao_simples,
	T.data_opcao_simples = R.data_opcao_simples,
	T.data_exclusao_simples = R.data_exclusao_simples,
	T.opcao_mei = R.opcao_mei,
	T.data_opcao_mei = R.data_opcao_mei,
	T.data_exclusao_mei = R.data_exclusao_mei,
    T.data_insercao = current_timestamp() 
 when not matched 
 then
 insert 
 (
     cnpj_basico,
     opcao_simples,
	 data_opcao_simples,
	 data_exclusao_simples,
	 opcao_mei,
	 data_opcao_mei,
	 data_exclusao_mei,
     data_insercao

 )  
 values 
 (
     R.cnpj_basico,
     R.opcao_simples,
	 R.data_opcao_simples,
	 R.data_exclusao_simples,
	 R.opcao_mei,
	 R.data_opcao_mei,
	 R.data_exclusao_mei,
     current_timestamp()

 ) 