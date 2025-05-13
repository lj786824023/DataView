DELIMITER |

CREATE DEFINER="gbase"@"%" FUNCTION "etl_func_getsql_dm"(
in_schema_name varchar(30), -- 库名
in_table_name varchar(30), -- 表名
in_column_name varchar(30), -- 列名
in_tx_date varchar(8), -- 跑批日期
in_days integer, -- 保留的天数
in_months integer -- 保留的月数
) RETURNS varchar(2000) CHARSET gbk
BEGIN
declare etl_out_res_msg varchar(2000);
set
etl_out_res_msg = concat( 'delete from ', in_schema_name, '.', in_table_name, ' where ', in_column_name, ' < (', in_tx_date, ' - interval ', in_days, ' day)', ' and ', in_column_name, ' not in (select calendar_date from etl.etl_sys_calendar where calendar_date <= ', in_tx_date, ' and calendar_date >= date_sub(', in_tx_date, ',interval case when ', in_tx_date, '=etl.etl_func_get_month_end_date(', in_tx_date, ') then ', in_months - 1, ' else ', in_months, ' end month) and end_of_month_ind = 1)' );

return etl_out_res_msg;
end |