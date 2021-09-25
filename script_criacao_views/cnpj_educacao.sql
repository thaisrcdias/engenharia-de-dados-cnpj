create or replace view semantic.cnpj_educacao as 
(
select m.codigo, m.descricao, EXTRACT(year FROM es.data_situacao_cadastral) as ano, count(es.cnpj_basico) num_cnpj
       from trusted.estabelecimento es
       inner join trusted.cnae cn on cn.codigo = es.cnae_fiscal_principal
       inner join trusted.municipio m on m.codigo = es.cod_municipio
       where lower(cn.descricao) like '%superior%'
             and  es.data_situacao_cadastral between date('2015-01-01') and date('2021-12-31')
       group by m.codigo, m.descricao, EXTRACT(year FROM es.data_situacao_cadastral)

)