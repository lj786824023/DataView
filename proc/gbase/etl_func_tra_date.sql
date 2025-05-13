DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" FUNCTION "etl_func_tra_date"(
	P_DATE varchar(100)
	) RETURNS varchar(20) CHARSET gbk
begin
	
	/*
	 * 作者：ljz
	 * 创建日期：20220314
	 * 修改记录：
	 *   20220314：初版:格式化输出日期（YYYYMMDD）
	 * 
	 */
	declare V_YEAR varchar(10) default '';
	declare V_MONTH varchar(10) default '';
	declare V_DAY varchar(10) default '';
	
	if length(P_DATE) = 8 and regexp_like(P_DATE,'[0-9]{8,8}') then
		return P_DATE;
	end if;
	
	set V_YEAR = regexp_substr(P_DATE,'[^/-][0-9]*',1,1);
	set V_MONTH = lpad(regexp_substr(P_DATE,'[^/-][0-9]*',1,2),2,'0');
	set V_DAY = lpad(regexp_substr(P_DATE,'[^/-][0-9]*',1,3),2,'0');
	
	return V_YEAR || V_MONTH || V_DAY;
END |