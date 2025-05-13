DELIMITER |

CREATE DEFINER="gbase"@"%" PROCEDURE "pr_log_trace"(
	IN p_proc_name varchar(100),
	IN p_start_time varchar(100),
	IN p_end_time varchar(100),
	IN p_o_result varchar(100)
)
begin
	declare v_proc_name varchar(100) default p_proc_name;
	declare v_start_time varchar(100) default p_start_time;
	declare v_end_time varchar(100) default p_end_time;
	declare v_o_result varchar(100) default p_o_result;

	insert into etl.etl_trace_log(proc_name, start_time, end_time, o_result)
	values (v_proc_name, v_start_time, v_end_time, v_o_result);

end |