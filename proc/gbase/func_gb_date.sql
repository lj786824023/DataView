DELIMITER |

CREATE DEFINER="cqbank_sj"@"%" FUNCTION "func_gb_date"(
	P_DATE varchar(100)
	) RETURNS varchar(20) CHARSET gbk
begin
	
	/*
	 * 作者：ljz
	 * 创建日期：20230301
	 * 修改记录：
	 *   20230301：初版:格式化输出日期（YYYYMMDD）
	 * 
	 */
	-- 空值返回 0001-01-01
	IF COALESCE(P_DATE,'') = '' THEN
	  RETURN CAST('0001-01-01' AS date);
	END IF;
	
	-- 截取年月日
	set @V_YEAR = cast(regexp_substr(P_DATE,'[0-9]+',1,1) AS int);
	set @V_MONTH = cast(regexp_substr(P_DATE,'[0-9]+',1,2) AS int);
	set @V_DAY = cast(regexp_substr(P_DATE,'[0-9]+',1,3) AS int);
	
	IF length(@V_YEAR)=8 THEN
	  set @V_MONTH = substr(@V_YEAR,5,2);
	  set @V_DAY = substr(@V_YEAR,7,2);
	  set @V_YEAR = substr(@V_YEAR,1,4);
	END IF;
	
	-- 计算2月份天数
	IF !(@V_YEAR BETWEEN 0001 AND 9999) THEN
	  RETURN CAST('0001-01-02' AS date);
	ELSE
	  SET @FEB_DAYS = IF((@V_YEAR%400=0) OR (@V_YEAR%100<>0 AND @V_YEAR%4=0),29,28);	  
	END IF;

    -- 判断月日是否正常	
	IF (@V_MONTH IN (1,3,5,7,8,10,12) AND @V_DAY BETWEEN 1 AND 31) OR (@V_MONTH IN (4,6,9,11) AND @V_DAY BETWEEN 1 AND 30) OR (@V_MONTH = 2 AND @V_DAY BETWEEN 1 AND @FEB_DAYS) THEN
	  SET @V_DATE = COALESCE(@V_YEAR || lpad(@V_MONTH,2,'0') || lpad(@V_DAY,2,'0'),'0001-01-02');
    ELSE
      RETURN CAST('0001-01-02' AS date);
	END IF;
	
	RETURN @V_DATE;
	
END |