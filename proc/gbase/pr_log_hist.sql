DELIMITER |

CREATE DEFINER="gbase"@"%" PROCEDURE "pr_log_hist"(
	in p_owner varchar(50), 
	in p_table_name varchar(100),       
	in p_data_date varchar(30),
	out p_o_result varchar(10)
)
begin
	declare v_cnt   int DEFAULT 0;
	declare v_proc_name  varchar(20) default 'pr_log_hist';
	declare v_sql1  varchar(2000);
	declare v_sql2  varchar(2000);
	declare v_start_time varchar(30) default date_format(now(), '%Y-%m-%d %H:%i:%s');
	declare v_end_time varchar(30) default '';

	select count(1) into v_cnt from etl.etl_hist_log
		where owner = p_owner
		and table_name = p_table_name
		and data_date = p_data_date;
	
	if v_cnt>0 then
		update etl.etl_hist_log set hist_num = hist_num + 1
			where owner = p_owner
			and table_name = p_table_name
			and data_date = p_data_date;
	else
		insert into etl.etl_hist_log(owner,table_name,data_date,hist_num)
			values(p_owner,p_table_name,p_data_date,1);
	end if;
	/*
	set p_o_result = '0';
	set  v_end_time = date_format(now(), '%Y-%m-%d %H:%i:%s');  
	call etl.pr_log_trace(v_proc_name, v_start_time, v_end_time, p_o_result);
	*/
end |