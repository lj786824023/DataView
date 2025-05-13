CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LG_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_LG_WSIB
    实现功能:区分融资类包含和非融资类包含的数据铺底，取到科目
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-11-07
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.NCM_BUSINESS_CONTRACT|授信业务合同表
    源  表2 :RWA_DEV.NCM_BUSINESS_TYPE|业务品种信息表
    源  表3 :RWA.ORG_INFO|机构信息表
    源  表4 :RWA.RWA_WS_LG|信贷保函补录表
    源  表5 :RWA.RWA_WP_SUPPTASKORG|补录任务机构分发配置表
    源  表6 :RWA.RWA_WP_SUPPTASK|补录任务发布表
    目标表1 :RWA.RWA_WSIB_LG|信贷保函铺底表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LG_WSIB';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    --清空拆借与存放同业铺底表
    DELETE FROM RWA.RWA_WSIB_LG WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 信贷系统-对外保函和对外担保
    INSERT INTO RWA.RWA_WSIB_LG(
                 DATADATE                              --数据日期
                ,ORGID                             	 	 --机构ID
                ,CONTRACTNO                        	   --合同流水号
                ,BELONGORGCODE                         --业务所属机构
                ,CUSTOMERNAME                          --客户名称
                ,BUSINESSTYPE													 --业务类型
                ,LGTYPE                                --保函类型
                ,BEGINDATE                             --合同起始日
                ,ENDDATE                         	     --合同到期日
                ,CURRENCY                   	         --币种
                ,BALANCE                        	     --合同余额
                ,FINANCEFLAG                         	 --是否融资性业务
    )
    WITH TMP_SUPPORG AS (
								SELECT T1.ORGID AS ORGID
										   ,CASE WHEN T3.ORGLEVEL > 2 THEN T4.SORTNO ELSE T3.SORTNO END AS SORTNO
								  FROM RWA.RWA_WP_SUPPTASKORG T1
						INNER JOIN RWA.RWA_WP_SUPPTASK T2
								    ON T1.SUPPTASKID = T2.SUPPTASKID
								   AND T2.ENABLEFLAG = '01'
						 LEFT JOIN RWA.ORG_INFO T3
								    ON T1.ORGID = T3.ORGID
						 LEFT JOIN RWA.ORG_INFO T4
	                	ON T3.BELONGORGID = T4.ORGID
								 WHERE T1.SUPPTMPLID = 'M-0200'
							ORDER BY T3.SORTNO
		)
		SELECT
								 DATADATE                              --数据日期
                ,ORGID                             	 	 --机构ID
                ,CONTRACTNO                        	   --合同流水号
                ,BELONGORGCODE                         --业务所属机构
                ,CUSTOMERNAME                          --客户名称
                ,BUSINESSTYPE													 --业务类型
                ,LGTYPE                                --保函类型
                ,BEGINDATE                             --合同起始日
                ,ENDDATE                         	     --合同到期日
                ,CURRENCY                   	         --币种
                ,BALANCE                        	     --合同余额
                ,FINANCEFLAG                         	 --是否融资性业务
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,NVL(T7.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --机构ID                     		按照补录任务分配情况，分配给金融市场部及其分部
                ,RANK() OVER(PARTITION BY T1.SERIALNO ORDER BY LENGTH(NVL(T7.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --数据序号
                ,T1.SERIALNO		                             AS CONTRACTNO               --合同号
                ,T1.OPERATEORGID														 AS BELONGORGCODE						 --业务所属机构
                ,T1.CUSTOMERNAME													   AS CUSTOMERNAME             --客户名称
                ,T1.BUSINESSTYPE                             AS BUSINESSTYPE             --业务类型
                ,T1.SAFEGUARDTYPE			                       AS LGTYPE                   --保函类型
                ,TO_CHAR(TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'),'YYYY-MM-DD')
                                                             AS BEGINDATE                --合同起始日
                ,TO_CHAR(TO_DATE(T1.MATURITY,'YYYYMMDD'),'YYYY-MM-DD')
                                                             AS ENDDATE                  --合同到期日
                ,T1.BUSINESSCURRENCY                         AS CURRENCY                 --币种
                ,T1.BALANCE                                  AS BALANCE                  --合同余额
                ,T4.FINANCEFLAG                              AS FINANCEFLAG              --是否融资性业务
    FROM        RWA_DEV.NCM_BUSINESS_CONTRACT T1
		LEFT	JOIN	RWA.ORG_INFO T3																						--机构信息表
	  ON					T1.OPERATEORGID = T3.ORGID
	  LEFT JOIN   (SELECT CONTRACTNO
	  									 ,FINANCEFLAG
	  							 FROM RWA.RWA_WS_LG
	  							WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_LG WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
	  						) T4																											--取最近一期补录数据铺底
    ON          T1.SERIALNO = T4.CONTRACTNO
    LEFT	JOIN  TMP_SUPPORG T7
    ON          T3.SORTNO LIKE T7.SORTNO || '%'
		WHERE       T1.DATANO = P_DATA_DT_STR
    AND         T1.BUSINESSTYPE IN ('105120','102050')       							--国内保函直接默认融资类保函，国内保函不用补录
    AND         T1.BALANCE > 0
    AND         SUBSTR(T1.SERIALNO,3,8) <= P_DATA_DT_STR
    ORDER BY		T1.SERIALNO
    )
    WHERE RECORDNO = 1
		;

    COMMIT;

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_LG WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');



    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '信贷保函补录数据铺底('|| v_pro_name ||')处理失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_LG_WSIB;
/

