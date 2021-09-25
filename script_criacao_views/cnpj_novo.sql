create or replace view semantic.cnpj_educacao as 
(

select m.codigo, m.descricao, EXTRACT(year FROM f.data_situacao_cadastral) as ano, EXTRACT(month FROM f.data_situacao_cadastral) as mes, count(e.cnpj_basico) as qte_cnpjs
        from trusted.empresa e
        inner join trusted.dados_simples d on e.cnpj_basico = d.cnpj_basico
        inner join trusted.estabelecimento f on f.cnpj_basico = d.cnpj_basico
        inner join trusted.municipio m on m.codigo = f.cod_municipio
    where (d.opcao_mei = 'S' or d.opcao_simples = 'S')
        and f.data_situacao_cadastral between date('2010-01-01') and date('2021-12-31')
   group by m.codigo, m.descricao, EXTRACT(year FROM f.data_situacao_cadastral), EXTRACT(month FROM f.data_situacao_cadastral)
)   
          
      





