merge trusted.pais as T
using
(
    select * except(rowNumber)
    from
    (
        select 
        coalesce(replace(codigo, '"', ''), '-1') as codigo, 
        coalesce(replace(descricao, '"',''), 'Não Informado') as descricao,
        row_number()over(partition by codigo order by data_cadastro desc) as rowNumber
        from `raw.pais`
    ) where rowNumber = 1
) as R 
on R.codigo = T.codigo
when matched 
then 
update set 
    T.descricao = R.descricao,
    T.data_insercao = current_timestamp() 
 when not matched 
 then
 insert 
 (
     codigo,
     descricao,
     data_insercao

 )  
 values 
 (
     R.codigo,
     R.descricao,
     current_timestamp()

 ) 