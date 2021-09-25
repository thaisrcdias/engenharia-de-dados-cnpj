create or replace view semantic.media_capital_social
(
select j.codigo, j.descricao,  extract(year from CURRENT_DATE()) - 1 as ultimo_ano , avg(e.capital_social) as media_capital_social
       from trusted.empresa e
       inner join trusted.natureza_juridica j on j.codigo = e.natureza_juridica
       inner join trusted.estabelecimento l on l.cnpj_basico = e.cnpj_basico

where e.porte_empresa = '03' and extract(year from l.data_situacao_cadastral) = extract(year from CURRENT_DATE()) - 1
group by j.codigo, j.descricao, extract(year from CURRENT_DATE()) - 1

)