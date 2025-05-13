CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_ISSUERRATING(
														p_data_dt_str IN  VARCHAR2, --数据日期
                            p_po_rtncode  OUT VARCHAR2, --返回编号
                            p_po_rtnmsg   OUT VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_LC_ISSUERRATING
    实现功能:理财系统-债券理财投资-市场风险-发行人评级信息(从数据源补录表中将债券理财投资相关信息全量导入RWA市场风险理财接口表发行人评级信息表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :QHJIANG
    编写时间:2016-04-14
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA.RWA_WS_FCII_BOND|债券理财投资补录表
    源  表2 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表
    目标表1 :RWA_DEV.RWA_LC_ISSUERRATING|发行人评级信息表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
    */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_ISSUERRATING';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --清除目标表中的原有记录
    --DELETE FROM RWA_LC_ISSUERRATING WHERE DATADATE=TO_DATE(p_data_dt_str,'yyyyMMdd');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_ISSUERRATING';


    --DBMS_OUTPUT.PUT_LINE('开始：导入【发行人评级信息表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    INSERT INTO RWA_DEV.RWA_LC_ISSUERRATING(
        				DATADATE                               --数据日期
                ,ISSUERID                           	 --发行人ID
                ,ISSUERNAME                    	 	 		 --发行人名称
                ,RATINGORG                     	 	 		 --评级机构
                ,RATINGRESULT                  	 	 		 --评级结果
                ,RATINGDATE                    	 	 		 --评级日期
                ,FETCHFLAG                     	 	 		 --取数标识
    )
    WITH TEMP_INVESTASSETDETAIL AS (
        SELECT DISTINCT
        			 T3.FLD_ASSET_CODE						 AS FLD_ASSET_CODE
          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
           AND T4.FLD_INCOME_TYPE <> '3'																		--3：排除非保本类型
           AND T4.DATANO = p_data_dt_str
         WHERE T3.FLD_ASSET_TYPE = '2'																			-- 2：债券，24：资产管理计划
           AND T3.FLD_ASSET_STATUS = '1' 																		--1：状态正常
           AND T3.FLD_ASSET_FLAG = '1'   																		--1：理财产品
           AND T3.C_ACC_TYPE = 'D'																					--D：交易类，该部分数据作为市场风险
           AND T3.FLD_DATE = p_data_dt_str																	--有效的理财产品其估值日期每日更新
           AND T3.DATANO = p_data_dt_str
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
    						DATADATE
    						,ISSUERID
    						,ISSUERNAME
    						,RATINGORG
    						,RATINGRESULT
    						,RATINGDATE
    						,FETCHFLAG
    						,RANK() OVER(PARTITION BY ISSUERID,RATINGORG ORDER BY RATINGDATE DESC)  AS RK
    						,ROW_NUMBER() OVER(PARTITION BY ISSUERID,RATINGORG,RATINGDATE ORDER BY RATINGRESULT) AS RM
    						,COUNT(1) OVER(PARTITION BY ISSUERID,RATINGORG,RATINGDATE) AS RN
    FROM
    (
    SELECT
    						TO_DATE(p_data_dt_str,'YYYYMMDD')						 AS DATADATE       --RWA系统赋值
        				,'LC' || T1.C_ISSUER_IDENTIFICATION_TYPE || T1.C_ISSUER_IDENTIFICATION_NO
        										                         				 AS ISSUERID       --债券发行人
        				,NVL(T1.C_RWA_PUBLISHNAME,T4.C_ORG_NAME)		 AS ISSUERNAME     --债券发行人
        				,T5.DITEMNO							               			 AS RATINGORG      --主体评级机构
        				,T3.DESCRATING															 AS RATINGRESULT   --主体信用评级
        				,T1.C_ISSUER_RELEASE_DATE										 AS RATINGDATE     --默认 空
        				,''                             						 AS FETCHFLAG      --取数标识
    FROM 				RWA_DEV.ZGS_ATBOND T1
    INNER JOIN	TEMP_INVESTASSETDETAIL T2
    ON					T1.C_BOND_CODE = T2.FLD_ASSET_CODE
    INNER JOIN	RWA_DEV.RWA_CD_RATING_MAPPING T3
    ON					T1.C_SCORE_TYPE = T3.SRCRATINGORG
    AND					T1.C_BODY_SCORE = T3.SRCRATING
    AND					T3.MAPPINGTYPE = 'LCI'
    LEFT JOIN		RWA_DEV.ZGS_ATTYORG T4
    ON					T1.C_PUBLISHER = T4.C_ORG_ID
    AND					T4.DATANO = p_data_dt_str
    LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T5
    ON					T1.C_SCORE_TYPE = T5.SITEMNO
    AND					T5.SYSID = 'LC'
    AND					T5.SCODENO = 'ERAgency'
   	WHERE 			T1.DATANO = p_data_dt_str
		AND					T1.C_BODY_SCORE IS NOT NULL
		AND					T1.C_ISSUER_IDENTIFICATION_NO IS NOT NULL
   	UNION
   	SELECT
    						TO_DATE(p_data_dt_str,'YYYYMMDD')						 AS DATADATE       --RWA系统赋值
        				,'LC' || T1.C_ISSUER_IDENTIFICATION_TYPE || T1.C_ISSUER_IDENTIFICATION_NO
        										                         				 AS ISSUERID       --债券发行人
        				,NVL(T1.C_RWA_PUBLISHNAME,T4.C_ORG_NAME)		 AS ISSUERNAME     --债券发行人
        				,T5.DITEMNO							               			 AS RATINGORG      --主体评级机构
        				,T3.DESCRATING															 AS RATINGRESULT   --主体信用评级
        				,T1.C_ISSUER_RELEASE_DATE2									 AS RATINGDATE     --默认 空
        				,''                             						 AS FETCHFLAG      --取数标识
    FROM 				RWA_DEV.ZGS_ATBOND T1
    INNER JOIN	TEMP_INVESTASSETDETAIL T2
    ON					T1.C_BOND_CODE = T2.FLD_ASSET_CODE
    INNER JOIN	RWA_DEV.RWA_CD_RATING_MAPPING T3
    ON					T1.C_SCORE_TYPE_2 = T3.SRCRATINGORG
    AND					T1.C_BODY_SCORE_2 = T3.SRCRATING
    AND					T3.MAPPINGTYPE = 'LCI'
    LEFT JOIN		RWA_DEV.ZGS_ATTYORG T4
    ON					T1.C_PUBLISHER = T4.C_ORG_ID
    AND					T4.DATANO = p_data_dt_str
    LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T5
    ON					T1.C_SCORE_TYPE_2 = T5.SITEMNO
    AND					T5.SYSID = 'LC'
   	WHERE 			T1.DATANO = p_data_dt_str
		AND					T1.C_BODY_SCORE_2 IS NOT NULL
		AND					T1.C_ISSUER_IDENTIFICATION_NO IS NOT NULL
		UNION
   	SELECT
    						TO_DATE(p_data_dt_str,'YYYYMMDD')						 AS DATADATE       --RWA系统赋值
        				,'ZGZYZF'		                         				 AS ISSUERID       --债券发行人(目前都是 空) 【需补录数据】
        				,'中国中央政府'															 AS ISSUERNAME     --债券发行人(目前都是 空)
        				,'01'										               			 AS RATINGORG      --主体评级机构
        				,(SELECT RATINGRESULT FROM RWA.RWA_WP_COUNTRYRATING WHERE COUNTRYCODE = 'CHN' AND ISINUSE = '1')
        																										 AS RATINGRESULT   --主体信用评级
        				,(SELECT REPLACE(RATINGSTARTDATE,'/','') FROM RWA.RWA_WP_COUNTRYRATING WHERE COUNTRYCODE = 'CHN' AND ISINUSE = '1')
        																										 AS RATINGDATE     --默认 空
        				,''                             						 AS FETCHFLAG      --取数标识
    FROM 				RWA_DEV.ZGS_ATBOND T1
    INNER JOIN	TEMP_INVESTASSETDETAIL T2
    ON					T1.C_BOND_CODE = T2.FLD_ASSET_CODE
   	WHERE 			T1.C_BOND_TYPE IN ('01','17','19')							--国债，默认发行人信息
		AND					T1.DATANO = p_data_dt_str
		)
		)
		WHERE				RK = 1
		AND					RM = (CASE WHEN RN = 1 THEN 1 ELSE 2 END)				--存在同一客户在同一天被同一家机构评级多次，取第二好的评级结果
   	;

		COMMIT;

		dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_ISSUERRATING',cascade => true);

    --DBMS_OUTPUT.PUT_LINE('结束：导入【发行人评级信息表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_ISSUERRATING;
    --Dbms_output.Put_line('RWA_DEV.RWA_LC_ISSUERRATING表当前插入的理财系统-债券理财投资(市场风险)-发行人评级信息数据记录为: ' || v_count || ' 条');

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count;

    --定义异常
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '发行人评级信息('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_LC_ISSUERRATING;
/

