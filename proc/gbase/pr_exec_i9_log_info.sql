DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_exec_i9_log_info"(
	OUT	OUT_CODE		INTEGER,	
	IN	IN_PRC_NAME	VARCHAR(60),
	IN	IN_STEP_NO		INTEGER,
	IN	IN_STEP_SQL		TEXT,
	in  IN_ETL_DATE VARCHAR(8)
)
lable:BEGIN
	delete from almbrd.t_amlbrd_i9_log_info 
	where ETL_DATE = IN_ETL_DATE
	and PRC_NAME = IN_PRC_NAME
	and STEP_NO = IN_STEP_NO;
	
	insert into almbrd.t_amlbrd_i9_log_info VALUES(IN_ETL_DATE,IN_PRC_NAME,IN_STEP_NO,IN_ETL_DATE);
END |