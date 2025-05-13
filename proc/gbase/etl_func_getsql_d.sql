DELIMITER |

CREATE DEFINER="gbase"@"%" FUNCTION "etl_func_getsql_d"(
in_schema_name varchar(30), -- 库名
in_table_name varchar(30), -- 表名
in_column_name varchar(30), -- 列名
in_tx_date varchar(8), -- 跑批日期
in_days integer -- 保留的天数
) RETURNS varchar(2000) CHARSET gbk
BEGIN
	declare etl_out_res_msg varchar(2000);
	set etl_out_res_msg = concat('delete from ',in_schema_name,'.',in_table_name,
	' where ',in_column_name,' < (',in_tx_date,' - interval ',in_days,' day)');
	return etl_out_res_msg;
END |