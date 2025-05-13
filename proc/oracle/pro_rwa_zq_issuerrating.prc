CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZQ_ISSUERRATING(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ZQ_ISSUERRATING
    实现功能:财务系统-债券-市场风险-发行人评级信息(从数据源补录表中将债券相关信息全量导入RWA市场风险债券接口表发行人评级信息表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-28
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA.RWA_WS_BONDTRADE|债券投资补录信息表
    源  表2 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表
    源  表3 :RWA_DEV.FNS_BND_INFO_B|财务系统债券信息表
    目标表  :RWA_DEV.RWA_ZQ_ISSUERRATING|财务系统债券类发行人评级信息表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZQ_ISSUERRATING';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZQ_ISSUERRATING';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 财务系统-债券投资-非国债且非外币主权
    INSERT INTO RWA_DEV.RWA_ZQ_ISSUERRATING(
                DATADATE                               --数据日期
                ,ISSUERID                           	 --发行人ID
                ,ISSUERNAME                    	 	 		 --发行人名称
                ,RATINGORG                     	 	 		 --评级机构
                ,RATINGRESULT                  	 	 		 --评级结果
                ,RATINGDATE                    	 	 		 --评级日期
                ,FETCHFLAG                     	 	 		 --取数标识
    )
    WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
		, TEMP_BND_CUST AS (
												SELECT DISTINCT
															 T4.BONDPUBLISHID			AS CUSTOMERID
													FROM RWA_DEV.FNS_BND_INFO_B T1
										INNER JOIN TEMP_BND_BOOK T2
														ON T1.BOND_ID = T2.BOND_ID
										INNER JOIN RWA_DEV.NCM_BUSINESS_DUEBILL T3														--信贷借据表
														ON 'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
													 AND T3.DATANO = p_data_dt_str
										INNER JOIN RWA_DEV.NCM_BOND_INFO T4																		--信贷债券信息表
	  												ON T3.RELATIVESERIALNO2 = T4.OBJECTNO
													 AND T4.OBJECTTYPE = 'BusinessContract'
													 AND NVL(T4.ISCOUNTTR,'2') <> '1'                               --人民币债券，非国债
                           AND NVL(T4.BONDFLAG04,'2') <> '1'                              --外币债券，非主权类
													 AND T4.DATANO = p_data_dt_str
												 WHERE T1.ASSET_CLASS = '10'																			--仅交易性账户进入市场风险
													 AND T1.DATANO = p_data_dt_str
													 AND T1.BOND_CODE IS NOT NULL																		--排除无效的债券数据
		)
		SELECT 			DATADATE
    						,ISSUERID
    						,ISSUERNAME
    						,RATINGORG
    						,RATINGRESULT
    						,RATINGDATE
    						,FETCHFLAG
   	FROM
   	(
    SELECT
    						TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,T1.CUSTOMERID													     AS ISSUERID               	 --发行人ID
                ,T4.CUSTOMERNAME	           								 AS ISSUERNAME          	 	 --发行人名称
                ,T2.EVALUTEORG					  									 AS RATINGORG        	 	 	 	 --评级机构
                ,T3.DESCRATING															 AS RATINGRESULT        	 	 --评级结果
                ,T2.EVALUTEDATE															 AS RATINGDATE          	 	 --评级日期
                ,''                         				 				 AS FETCHFLAG           	 	 --取数标识
                ,RANK() OVER(PARTITION BY T1.CUSTOMERID,T2.EVALUTEORG ORDER BY T2.EVALUTEDATE DESC) AS RK
    						,ROW_NUMBER() OVER(PARTITION BY T1.CUSTOMERID,T2.EVALUTEORG,T2.EVALUTEDATE ORDER BY T3.DESCRATING) AS RM
    						,COUNT(1) OVER(PARTITION BY T1.CUSTOMERID,T2.EVALUTEORG,T2.EVALUTEDATE) AS RN

    FROM				TEMP_BND_CUST T1
    INNER JOIN	RWA_DEV.NCM_CUSTOMER_RATING T2
    ON					T1.CUSTOMERID = T2.CUSTOMERID
    AND					T2.DATANO = p_data_dt_str
	  INNER JOIN	RWA_DEV.RWA_CD_RATING_MAPPING T3
	  ON					T2.EVALUTEORG = T3.SRCRATINGORG
	  AND					T2.EVALUTELEVEL = T3.SRCRATINGNAME
	  AND					T3.MAPPINGTYPE = '01'																			--全量类型
	  AND					T3.SRCRATINGTYPE = '01'																		--长期评级
	  LEFT JOIN		RWA_DEV.NCM_CUSTOMER_INFO T4															--统一客户信息表
	  ON					T1.CUSTOMERID = T4.CUSTOMERID
	  AND					T4.DATANO = p_data_dt_str
	  )
	  WHERE				RK = 1
		AND					RM = (CASE WHEN RN = 1 THEN 1 ELSE 2 END)									--存在同一客户在同一天被同一家机构评级多次，取第二好的评级结果
		;

    COMMIT;


    --2.2 财务系统-债券投资-国债
    INSERT INTO RWA_DEV.RWA_ZQ_ISSUERRATING(
                DATADATE                               --数据日期
                ,ISSUERID                           	 --发行人ID
                ,ISSUERNAME                    	 	 		 --发行人名称
                ,RATINGORG                     	 	 		 --评级机构
                ,RATINGRESULT                  	 	 		 --评级结果
                ,RATINGDATE                    	 	 		 --评级日期
                ,FETCHFLAG                     	 	 		 --取数标识
    )
    WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
    SELECT      DISTINCT
    						TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,'ZGZYZF'																     AS ISSUERID               	 --发行人ID
                ,'中国中央政府'		           								 AS ISSUERNAME          	 	 --发行人名称
                ,'01'										  									 AS RATINGORG        	 	 	 	 --评级机构
                ,(SELECT RATINGRESULT FROM RWA.RWA_WP_COUNTRYRATING WHERE COUNTRYCODE = 'CHN' AND ISINUSE = '1')
                																						 AS RATINGRESULT        	 	 --评级结果                 待转换为标普
                ,(SELECT REPLACE(RATINGSTARTDATE,'/','') FROM RWA.RWA_WP_COUNTRYRATING WHERE COUNTRYCODE = 'CHN' AND ISINUSE = '1')
                																						 AS RATINGDATE          	 	 --评级日期
                ,''                         				 				 AS FETCHFLAG           	 	 --取数标识

    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T2
		ON					T1.BOND_ID = T2.BOND_ID
		INNER JOIN	RWA_DEV.NCM_BUSINESS_DUEBILL T3														--信贷借据表
		ON					'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
		AND					T3.BUSINESSTYPE = '1040102040'														--人民币债券投资
		AND					T3.DATANO = p_data_dt_str
		INNER JOIN	RWA_DEV.NCM_BOND_INFO T4																	--信贷债券信息表
	  ON					T3.RELATIVESERIALNO2 = T4.OBJECTNO
		AND					T4.OBJECTTYPE = 'BusinessContract'
		AND					(T4.ISCOUNTTR = '1' OR T4.BONDNAME LIKE '%国债%')					--国债
		AND					T4.DATANO = p_data_dt_str
		WHERE 			T1.ASSET_CLASS = '10'																			--仅交易性账户进入市场风险
		AND					T1.DATANO = p_data_dt_str
		AND 				T1.BOND_CODE IS NOT NULL																	--排除无效的债券数据
		;

    COMMIT;


    --2.3 财务系统-债券投资-外币主权
    INSERT INTO RWA_DEV.RWA_ZQ_ISSUERRATING(
                DATADATE                               --数据日期
                ,ISSUERID                           	 --发行人ID
                ,ISSUERNAME                    	 	 		 --发行人名称
                ,RATINGORG                     	 	 		 --评级机构
                ,RATINGRESULT                  	 	 		 --评级结果
                ,RATINGDATE                    	 	 		 --评级日期
                ,FETCHFLAG                     	 	 		 --取数标识
    )
    WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
    SELECT      DISTINCT
    						TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,CASE WHEN T4.MARKETSCATEGORY = '01' THEN T4.BONDPUBLISHCOUNTRY || 'ZYZF'													--境外主权国家或经济实体区域的中央政府
                			WHEN T4.MARKETSCATEGORY = '02' THEN T4.BONDPUBLISHCOUNTRY || 'ZYYH'													--境外中央银行
                			ELSE T4.BONDPUBLISHCOUNTRY || 'BMST'																												--境外国家或地区注册的公共部门实体
                 END																		     AS ISSUERID               	 --发行人ID
                ,CASE WHEN T4.MARKETSCATEGORY = '01' THEN T4.BONDPUBLISHCOUNTRY || '中央政府'											--境外主权国家或经济实体区域的中央政府
                			WHEN T4.MARKETSCATEGORY = '02' THEN T4.BONDPUBLISHCOUNTRY || '中央银行'											--境外中央银行
                			ELSE T4.BONDPUBLISHCOUNTRY || '公共部门实体'																								--境外国家或地区注册的公共部门实体
                 END							           								 AS ISSUERNAME          	 	 --发行人名称
                ,'01'										  									 AS RATINGORG        	 	 	 	 --评级机构
                ,T5.RATINGRESULT														 AS RATINGRESULT        	 	 --评级结果                 待转换为标普
                ,REPLACE(T5.RATINGSTARTDATE,'/','')					 AS RATINGDATE          	 	 --评级日期
                ,''                         				 				 AS FETCHFLAG           	 	 --取数标识

    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T2
		ON					T1.BOND_ID = T2.BOND_ID
		INNER JOIN	RWA_DEV.NCM_BUSINESS_DUEBILL T3														--信贷借据表
		ON					'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
		AND					T3.BUSINESSTYPE = '1040202011'														--外币债券投资
		AND					T3.DATANO = p_data_dt_str
		INNER JOIN	RWA_DEV.NCM_BOND_INFO T4																	--信贷债券信息表
	  ON					T3.RELATIVESERIALNO2 = T4.OBJECTNO
		AND					T4.OBJECTTYPE = 'BusinessContract'
		AND					T4.BONDFLAG04 = '1'																				--主权类
		AND					T4.DATANO = p_data_dt_str
		INNER JOIN	RWA.RWA_WP_COUNTRYRATING T5
		ON					T4.BONDPUBLISHCOUNTRY = T5.COUNTRYCODE
		AND					T5.ISINUSE = '1'
		WHERE 			T1.ASSET_CLASS = '10'																			--仅交易性账户进入市场风险
		AND					T1.DATANO = p_data_dt_str
		AND 				T1.BOND_CODE IS NOT NULL																	--排除无效的债券数据
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZQ_ISSUERRATING',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZQ_ISSUERRATING;
    --Dbms_output.Put_line('RWA_DEV.RWA_ZQ_ISSUERRATING表当前插入的财务系统-债券(市场风险)-发行人评级信息数据记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '发行人评级信息('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZQ_ISSUERRATING;
/

