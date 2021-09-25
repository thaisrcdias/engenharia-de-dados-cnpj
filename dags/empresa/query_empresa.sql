merge trusted.empresa as T
using
(
    select * except(rowNumber)
    from
    (
        select 
        coalesce(replace(cnpj_basico, '"', ''), '-1') as cnpj_basico, 
		coalesce(replace(replace(razao_social, '"',''), '\'', ''), 'Não Informado') as razao_social
		coalesce(replace(natureza_juridica	, '"',''), '-1') as natureza_juridica,
		coalesce(replace(qualificacao_responsavel, '"',''), 'Não Informado') as qualificacao_responsavel,
		cast(replace(replace(capital_social, '"',''), ',', '.') as float64) as capital_social,
		coalesce(replace(porte_empresa, '"',''), 'Não Informado') as porte_empresa,
		coalesce(replace(ente_federativo, '"',''), 'Não Informado') as ente_federativo,
        row_number()over(partition by cnpj_basico order by data_cadastro desc) as rowNumber
        from `raw.empresa`
    ) where rowNumber = 1
) as R 
on R.cnpj_basico = T.cnpj_basico
when matched 
then 
update set 
    T.razao_social = R.razao_social,
	T.qualificacao_responsavel = R.qualificacao_responsavel,
	T.capital_social = R.capital_social,
	T.porte_empresa = R.porte_empresa,
	T.ente_federativo = R.ente_federativo,
  T.data_cadastro = current_timestamp() 
 when not matched 
 then
 insert 
 (
     cnpj_basico,
     razao_social,
	 natureza_juridica,
	 qualificacao_responsavel,
	 capital_social,
	 porte_empresa,
	 ente_federativo,
     data_cadastro

 )  
 values 
 (
     R.cnpj_basico,
     R.razao_social,
	 R.natureza_juridica,
	 R.qualificacao_responsavel,
	 R.capital_social,
	 R.porte_empresa,
	 R.ente_federativo,
     current_timestamp()

 ) 