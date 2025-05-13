DELIMITER |

CREATE DEFINER="gbase"@"%" FUNCTION "etl_func_ccs_date"(
P_DATE varchar(20) -- 库名
) RETURNS varchar(20) CHARSET gbk
BEGIN
	declare V_YEAR varchar(10);
	declare V_MONTH varchar(10);
	declare V_DAY varchar(10);
	
	SET V_YEAR = '20'||substr(P_DATE,-2);
	SET V_MONTH = CASE substr(P_DATE,3,3)
                    WHEN 'JUN' THEN '01'
                    WHEN 'FEB' THEN '02'
                    WHEN 'MAR' THEN '03'
                    WHEN 'APR' THEN '04'
                    WHEN 'MAY' THEN '05'
                    WHEN 'JUN' THEN '06'
                    WHEN 'JUL' THEN '07'
                    WHEN 'AUG' THEN '08'
                    WHEN 'SEP' THEN '09'
                    WHEN 'OCT' THEN '10'
                    WHEN 'NOV' THEN '11'
                    WHEN 'DEC' THEN '12'
                    ELSE ''
				  END;
	SET V_DAY = substr(P_DATE,1,2);
	
	SET @V_DATE = V_YEAR || V_MONTH || V_DAY;

	return DECODE(V_MONTH,'','0001-01-01',@V_DATE);
	-- return V_YEAR || V_MONTH || V_DAY;
END |