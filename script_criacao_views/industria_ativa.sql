create or replace view semantic.industria_ativa as 
(

select  m.codigo, m.descricao, EXTRACT(year FROM e.data_situacao_cadastral) as ano, EXTRACT(month FROM e.data_situacao_cadastral) as mes, count(e.cnpj_basico) as qte_industrias
          from trusted.estabelecimento e
          inner join trusted.dados_simples d on d.cnpj_basico = e.cnpj_basico
          inner join trusted.municipio m on m.codigo = e.cod_municipio
        where e.situacao_cadastral = '02' and (UPPER(d.opcao_mei) = 'S' or UPPER(d.opcao_simples) = 'S')
          and e.data_situacao_cadastral between date('2010-01-01') and date('2021-12-31')
        group by m.codigo, m.descricao, EXTRACT(year FROM e.data_situacao_cadastral), EXTRACT(month FROM e.data_situacao_cadastral) 
		
)