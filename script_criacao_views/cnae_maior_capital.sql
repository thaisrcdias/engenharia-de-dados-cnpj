create or replace view semantic.cnae_maior_capital as (

select * from (
select cn.codigo,
       cn.descricao, 
       round(avg(ep.capital_social), 2) as capital_social_medio
       from trusted.empresa ep 
       inner join trusted.estabelecimento es on es.cnpj_basico = ep.cnpj_basico
       inner join trusted.cnae cn on cn.codigo = es.cnae_fiscal_principal
       where  extract(year from es.data_situacao_cadastral) = extract(year from CURRENT_DATE() - 1)
      group by cn.codigo, cn.descricao) order by capital_social_medio desc limit 1 
)
