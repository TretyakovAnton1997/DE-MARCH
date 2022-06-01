--drop table annual_rr ;
create table annual_rr(adress varchar2(4000),rooms number,surface1 number,surface2 number,cost number,rent number,annual_rr number);
insert into annual_rr(adress,rooms,surface1 ,surface2 ,cost ,rent ,annual_rr) 
select adress, rooms, surface1, surface2, cost,rent,
round((sum(rent/surface2) over (partition by adress,rooms))/(count(rent) over (partition by adress,rooms))*12/(sum(cost/surface1) over (partition by adress,rooms))/(count(cost) over (partition by adress,rooms))*100,2) as annual_rr
from
(select '('|| substr(student00.realty_rent_data.f3,'1', length(student00.realty_rent_data.f3)-instr(reverse(student00.realty_rent_data.f3),' '))|| ')' || student00.realty_sale_data.f2 as adress, 
student00.realty_sale_data.f4 as rooms, student00.realty_rent_data.f4 as rooms2, cast(replace(rtrim(substr(student00.realty_sale_data.f5 ,0, instr(student00.realty_sale_data.f5,'/')), '/'),'.',',') as number)  as surface1,
cast(rtrim(substr(student00.realty_rent_data.f5,0,instr(student00.realty_rent_data.f5,'/')),'/') as number)  as surface2, cast(replace(student00.realty_sale_data.f7, ' ') as number) as cost ,
cast(replace(student00.realty_rent_data.f7,' ') as number)  as rent, student00.realty_sale_data.f3 as metro
from  student00.realty_sale_data left join student00.realty_rent_data on student00.realty_rent_data.f2 = student00.realty_sale_data.f2 and substr(student00.realty_rent_data.f3,'1', length(student00.realty_rent_data.f3)-instr(reverse(student00.realty_rent_data.f3),' ')) like substr(student00.realty_sale_data.f3,'1', length(student00.realty_sale_data.f3)-instr(reverse(student00.realty_rent_data.f3),' '))
where student00.realty_sale_data.f4 =  student00.realty_rent_data.f4  and student00.realty_rent_data.f5 is not null and rtrim(substr(student00.realty_rent_data.f5,0,instr(student00.realty_rent_data.f5,'/')),'/') !='-')
order by annual_rr desc;
commit;
-- На данном этапе после объединения таблиц и вычисления доходности в отдельную таблицу выгрузили данные о доходности от сдачи в аренду недвижимости. 
select * from annual_rr fetch first 10 rows only; -- Chart 1
select  distinct(rooms), avg(annual_rr) over (partition by rooms) from annual_rr order by 2 desc ;--Chart2 -- Результаты данных запросов при помощи встроенного в sql developer export wizard-a выгружаем в файл xls для визуалиции результатов инструментами excel.


-- Вставляем в таблицу ddm данные об изменении индекса MCFTR за последние 12 лет в современных ценах с учётом инфляции. 
--drop table ddm;
create table ddm (num_years number,start_dt DATE, end_dt DATE, mcftr number);
insert into ddm
with const as
 (select num_years, add_months(end_dt, -12 * num_years) start_dt, end_dt
    from (select 13 num_years, to_date('01.01.2022', 'dd.mm.yyyy') end_dt
            from dual)),
cld as
 (select end_dt - level + 1 dt,
         student00.st_get_inf_period('RUB', end_dt - level + 1, end_dt) coeff_infl
    from const
  connect by level <= end_dt - start_dt + 1),
m as
 (select to_date(f1, 'dd.mm.yyyy') dt,
         to_number(f2, '99999999999999999999D99999999999999999999', 'NLS_NUMERIC_CHARACTERS='', ''') mcftr
    from student00.mcftr),
i as
 (select cld.dt,
         last_value(m.mcftr ignore nulls) over(order by cld.dt) mcftr,
         round(last_value(m.mcftr ignore nulls) over(order by cld.dt) * cld.coeff_infl, 2) mcftr_with_infl
    from cld
    left join m
      on cld.dt = m.dt
   order by cld.dt),
pre as
 (select /*+ materialize*/
   t.num_year, t.start_dt, t.end_dt,
   min(i.dt) min_dt,
   max(i.mcftr_with_infl) keep(dense_rank first order by i.dt) start_mcftr_with_infl,
   max(i.mcftr_with_infl) keep(dense_rank last order by i.dt) end_mcftr_with_infl
    from i,
         (select level num_year,
                 add_months(end_dt, -12 * level) start_dt,
                 add_months(end_dt, -12 * (level - 1)) end_dt
            from const
          connect by level <= num_years) t
   where i.dt between t.start_dt and t.end_dt
   group by t.num_year, t.start_dt, t.end_dt)
select  num_year,start_dt, end_dt, --start_mcftr_with_infl, end_mcftr_with_infl,
       round(avg(end_mcftr_with_infl / start_mcftr_with_infl), 6) mcftr
  from pre
 where min_dt = start_dt
 group by num_year, start_dt, end_dt, start_mcftr_with_infl, end_mcftr_with_infl
 order by num_year fetch first 12 rows only;
commit;
select * from ddm;--chart3 --Результат данного запроса при помощи встроенного в sql developer export wizard-a выгружаем в файл xls для визуалиции результатов инструментами excel.


--drop table ami;
create table ami (num_year number, start_dt DATE, end_dt DATE, q_stock number, avg_k_grow number);  
insert into ami 
with const as
 (select num_years, add_months(end_dt, -12 * num_years) start_dt, end_dt
    from (select 15 num_years, to_date('01.01.2022', 'dd.mm.yyyy') end_dt
            from dual)),
pre as
 (select /*+ materialize*/
   stock_invest_results.stock_name, t.num_year, t.start_dt, t.end_dt,
   min(stock_invest_results.dt) min_dt,
   max(stock_invest_results.amt_minus_infl) keep(dense_rank first order by stock_invest_results.dt) start_amt_minus_infl,
   max(stock_invest_results.amt_minus_infl) keep(dense_rank last order by stock_invest_results.dt) end_amt_minus_infl
    from student00.stock_invest_results,
         (select level num_year, add_months(end_dt, -12 * level) start_dt, add_months(end_dt, -12 * (level - 1)) end_dt
            from const
          connect by level <= num_years) t
   where stock_invest_results.dt between t.start_dt and t.end_dt
   group by stock_invest_results.stock_name, t.num_year, t.start_dt, t.end_dt)
select  num_year, start_dt, end_dt,
       count(1) q_stock,
       round(avg(end_amt_minus_infl / start_amt_minus_infl), 6) avg_k_grow
  from pre
 where min_dt = start_dt
 group by num_year, start_dt, end_dt
 order by num_year;
commit;
-- Заполняем таблицу ami значениями демонстрирующими количество акций которые продавались на бирже в тот или иной год а так же коэффицент роста с учетом инфляции.
select * from ami  order by num_year desc; -- chart4


--Хит парад акций с самой высокой средней доходностью за последние 10 лет.
create table hp (stock_name varchar2(4000), min_dt date, end_dt date, infl_coeff number, start_stock_price number, 
end_stock_price number, stock_price_incr_wo_div number, sum_div_amt number, num_stocks_incr_due2_reinvest number, 
total_incr_over_infl number, yearly_avg_incr_over_infl number );
insert into hp
with const as (select num_years, add_months(end_dt, -12 * num_years) start_dt, end_dt
                 from (select 10 num_years, to_date('01.01.2022', 'dd.mm.yyyy') end_dt from dual)),
pre as (select /*+ materialize*/ stock_name, const.num_years, const.start_dt, const.end_dt,
               min(dt) min_dt,
               nvl(sum(div_amt), 0) sum_div_amt,
               max(stock_price) keep(dense_rank first order by dt) start_stock_price,
               max(stock_price) keep(dense_rank last order by dt) end_stock_price,
               max(num_stocks) keep(dense_rank first order by dt) start_num_stocks,
               max(num_stocks) keep(dense_rank last order by dt) end_num_stocks,
               max(amt_minus_infl) keep(dense_rank first order by dt) start_amt_minus_infl,
               max(amt_minus_infl) keep(dense_rank last order by dt) end_amt_minus_infl
        from student00.stock_invest_results, const
         where stock_invest_results.dt between const.start_dt and const.end_dt
         group by stock_name, const.num_years, const.start_dt, const.end_dt)
select stock_name, min_dt, end_dt, round(student00.st_get_inf_period('RUB', min_dt, end_dt), 6) infl_coeff,
       start_stock_price, end_stock_price, round(end_stock_price / start_stock_price, 6) stock_price_incr_wo_div,
       sum_div_amt,
       round(end_num_stocks / start_num_stocks, 6) num_stocks_incr_due2_reinvest,
       round(end_amt_minus_infl / start_amt_minus_infl, 6) total_incr_over_infl,
       round(power(end_amt_minus_infl / start_amt_minus_infl, 12 / months_between(end_dt, min_dt)), 6) yearly_avg_incr_over_infl
  from pre
 order by 11 desc;
 commit;
 select stock_name,min_dt,end_dt,end_stock_price,sum_div_amt,yearly_avg_incr_over_infl from hp order by 6 desc fetch first 20 rows only; --Chart5
 
 
--В результате выполнения данного запроса объединяем ежегодный прирост в процентах от акций и от недвижимости.
--drop table hp_re_profitability;
create table hp_re_profitability(name varchar2(4000), profitability number);
insert into hp_re_profitability
select stock_name as name , (yearly_avg_incr_over_infl-1)*100 as profitability  from ( select * from hp order by 11)
union all
select 'real estate '||cast(r as varchar2(4000))||' rooms' as name, round(a,4) as profitability  from (select distinct(rooms) r , avg(annual_rr)  over (partition by rooms) a from annual_rr order by rooms)
order by profitability desc;
commit;
select * from hp_re_profitability order by profitability desc; --Chart6
--