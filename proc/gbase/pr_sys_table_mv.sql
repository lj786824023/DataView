DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_sys_table_mv"(
	)
begin
   
	/*
	 * 作者：ljz
	 * 创建日期：20231227
	 * 修改记录：
	 *   20231227：初版 迁移源表数据到目标表，主要用于迁移不同引擎的表
	 *             1.建表etl.bbb，字段数据量与源表一致，建议全为varchar
	 *             2.修改相关表、游标sql 
	 *
	 */
	
    DECLARE V1,V2,V3,V4,V5,V6,V7,V8,V9,V10,V11,V12,V13,V14,V15,V16,V17,V18,V19,V20 VARCHAR(2000); -- 游标接收字段
    DECLARE V_HEAD_SQL VARCHAR(2000) DEFAULT 'insert into etl.bbb values'; -- 头部SQL
    DECLARE V_DATA_LIST LONGTEXT DEFAULT ''; -- 数据元组
    DECLARE V_COMMIT_CNT INT DEFAULT 0; -- 控制循环次数
    DECLARE V_NULL_STR VARCHAR(10) DEFAULT ''; -- 空字符串
    
	DECLARE DONE INT DEFAULT(0);
	DECLARE cur REF CURSOR;
	DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET done = 1;

	-- 游标数据
	OPEN cur FOR
	SELECT * FROM information_schema.columns WHERE table_schema='pdm';
    
	-- 清空目标表
	TRUNCATE TABLE etl.bbb;
	
	SET @V_DATA_LIST = '';
	
	REPEAT
	FETCH cur INTO V1,V2,V3,V4,V5,V6,V7,V8,V9,V10,V11,V12,V13,V14,V15,V16,V17,V18,V19,V20;
	IF NOT DONE THEN
	    SET V_COMMIT_CNT = V_COMMIT_CNT + 1;
        -- 拼接数据('a','b','c','d')
	    SET @V_DATA=CONCAT('(\'',
                           concat_ws('\',\'',NVL(V1,V_NULL_STR),NVL(V2,V_NULL_STR),NVL(V3,V_NULL_STR),NVL(V4,V_NULL_STR),NVL(V5,V_NULL_STR),NVL(V6,V_NULL_STR),NVL(V7,V_NULL_STR),NVL(V8,V_NULL_STR),NVL(V9,V_NULL_STR),NVL(V10,V_NULL_STR),NVL(V11,V_NULL_STR),NVL(V12,V_NULL_STR),NVL(V13,V_NULL_STR),NVL(V14,V_NULL_STR),NVL(V15,V_NULL_STR),NVL(V16,V_NULL_STR),NVL(V17,V_NULL_STR),NVL(V18,V_NULL_STR),NVL(V19,V_NULL_STR),NVL(V20,V_NULL_STR)),
                           '\')');
        -- 凭借元组数据('a','b','c','d'),('a','b','c','d')
	    SET @V_DATA_LIST = @V_DATA_LIST||@V_DATA||',';
	    -- 每1000行提交1次
	    IF V_COMMIT_CNT >= 1000 THEN
	      -- 拼接表头insert
	      SET @V_SQL = V_HEAD_SQL||SUBSTR(@V_DATA_LIST,1,CHAR_LENGTH(@V_DATA_LIST)-1);
	      prepare V_STMT from @V_SQL;
	      execute V_STMT;
	      SET V_COMMIT_CNT = 0;
	      SET @V_DATA_LIST = ''; 
	    END IF;

	END IF;
	UNTIL DONE END REPEAT;
	CLOSE cur;
	
	IF V_COMMIT_CNT >= 1 THEN
	      -- 拼接表头insert
	      SET @V_SQL = V_HEAD_SQL||SUBSTR(@V_DATA_LIST,1,CHAR_LENGTH(@V_DATA_LIST)-1);
	      prepare V_STMT from @V_SQL;
	      execute V_STMT;
	    END IF;

END |