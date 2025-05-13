DELIMITER |

CREATE DEFINER="gbase"@"%" PROCEDURE "pr_log_error"(IN p_data_date varchar(8),
	IN p_step varchar(10),
	IN p_error_msg long,
	IN p_proc_name varchar(100),
	IN p_o_result varchar(10)
	)
begin
	declare v_data_date varchar(8) default p_data_date;
	declare v_step varchar(10) default p_step;
	declare v_error_msg long default p_error_msg;
	declare v_proc_name varchar(100) default p_proc_name;
	declare v_o_result varchar(10) default p_o_result;

	insert into etl.etl_error_log(data_date, step, error_msg, proc_name, p_o_result, system_time)
	values (v_data_date, v_step, v_error_msg, v_proc_name, v_o_result, now());
END |