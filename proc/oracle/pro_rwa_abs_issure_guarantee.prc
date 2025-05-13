CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSURE_GUARANTEE(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ABS_ISSURE_GUARANTEE
    实现功能:信息管理系统-保证,表结构为保证表
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2015-05-26
    单  位	:上海安硕信息技术股份有限公司
    源  表1	:NCM_GUARANTY_INFO|担保物信息表
    源  表2	:NCM_BUSINESS_DUEBILL|授信业务借据信息表
    源  表3	:NCM_BUSINESS_CONTRACT|授信业务合同表
    源  表4	:NCM_GUARANTY_CONTRACT|担保合同信息表
    源  表5	:NCM_CONTRACT_RELATIVE|合同关联表
    源  表6	:NCM_GUARANTY_RELATIVE|担保合同与担保物关联表
    源  表7	:NCM_CUSTOMER_INFO|客户基本信息表
    源  表8 :NCM_GUARANTY_INFO|担保物信息表
    目标表	:RWA_XD_GUARANTEE|信贷系统-保证表
    变更记录(修改人|修改时间|修改内容):
  */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_ISSURE_GUARANTEE';
  --定义判断值变量
  v_count1 INTEGER;
  --定义异常变量
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ABS_ISSURE_GUARANTEE';

    --将有效借据下对应合同的保证插入到目标表中
    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_GUARANTEE(
         				 DATADATE          												  --数据日期
								,DATANO                                     --数据流水号
								,GUARANTEEID                                --保证ID
								,SSYSID                                     --源系统ID
								,GUARANTEECONID                             --保证合同ID
								,GUARANTORID                                --保证人ID
								,CREDITRISKDATATYPE                         --信用风险数据类型
								,GUARANTEEWAY                            		--担保方式
								,QUALFLAGSTD                            		--权重法合格标识
								,QUALFLAGFIRB                               --内评初级法合格标识
								,GUARANTEETYPESTD                           --权重法保证类型
								,GUARANTORSDVSSTD                           --权重法保证人细分
								,GUARANTEETYPEIRB                           --内评法保证类型
								,GUARANTEEAMOUNT                            --保证总额
								,CURRENCY                                   --币种
								,STARTDATE                                  --起始日期
								,DUEDATE                                    --到期日期
								,ORIGINALMATURITY                           --原始期限
								,RESIDUALM                                  --剩余期限
								,GUARANTORIRATING                           --保证人内部评级
								,GUARANTORPD                                --保证人违约概率
								,GROUPID                                    --分组编号
    )WITH TEMP_GUARANTEE1 AS(SELECT T3.SERIALNO AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
                                    ,MAX(NVL(T1.ACTUALMATURITY,T3.MATURITY)) AS MATURITY, MIN(T2.ATTRIBUTE1) AS ATTRIBUTE1
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '3%'  --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA.RWA_WS_ABS_ISSUE_UNDERASSET RWAIU
                               ON T1.SERIALNO=RWAIU.HTBH
                               AND RWAIU.DATADATE = TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
                               INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT RWD
                               ON          RWAIU.SUPPORGID=RWD.ORGID
                               AND         RWD.DATADATE=TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
                               AND         RWD.SUPPTMPLID='M-0133'
                               AND         RWD.SUBMITFLAG='1'
                               INNER JOIN 	RWA_DEV.RWA_ABS_ISSURE_CONTRACT T4
    													 ON					'ABS'||T3.SERIALNO = T4.CONTRACTID
                               WHERE T1.DATANO=P_DATA_DT_STR
                               GROUP BY T3.SERIALNO
		                         )
		,TEMP_RELATIVE AS (SELECT T3.SERIALNO,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(ATTRIBUTE1) AS ATTRIBUTE1,MIN(T2.QUALIFYFLAG) AS QUALIFYFLAG
                       FROM TEMP_GUARANTEE1 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO,MIN(QUALIFYFLAG) AS QUALIFYFLAG
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      GROUP BY T3.SERIALNO
                    )
    SELECT
         				 TO_DATE(T1.DATANO,'YYYYMMDD')										AS  DATADATE          		 --数据日期
         				,T1.DATANO																 		    AS	DATANO               --数据流水号
         				,'ABS'||T1.SERIALNO													      AS	GUARANTEEID          --保证ID
								,'ABS'																						AS	SSYSID               --源系统ID
								,'ABS'||T1.SERIALNO																AS	GUARANTEECONID       --保证合同ID
								,T1.GUARANTORID																		AS	GUARANTORID          --保证人ID
								,CASE WHEN T2.ATTRIBUTE1='1' THEN '01'
                      ELSE '02'
                 END																							AS	CREDITRISKDATATYPE   --信用风险数据类型
								,T1.GUARANTYTYPE																	AS	GUARANTEEWAY       	 --担保方式
								,''																								AS	QUALFLAGSTD          --权重法合格标识
								,CASE WHEN T2.QUALIFYFLAG	= '01' THEN '1'
											WHEN T2.QUALIFYFLAG	= '02' THEN '0'
								 			ELSE ''
								 END																						  AS	QUALFLAGFIRB         --内评初级法合格标识
								,''																								AS	GUARANTEETYPESTD     --权重法保证类型
								,''																								AS	GUARANTORSDVSSTD     --权重法保证人细分
								,CASE WHEN T2.QUALIFYFLAG = '01' THEN '020201'
								      ELSE ''
								 END																							AS	GUARANTEETYPEIRB     --内评法保证类型
								,T1.GUARANTYVALUE																	AS	GUARANTEEAMOUNT      --保证总额
								,NVL(T1.GUARANTYCURRENCY,'CNY')										AS	CURRENCY             --币种
								,T2.PUTOUTDATE																	  AS	STARTDATE            --起始日期
								,T2.MATURITY																			AS	DUEDATE              --到期日期
								,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365
								END                                               AS  ORIGINALMATURITY   	 --原始期限
								,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
								END      											                    AS	RESIDUALM            --剩余期限
								,T6.PDADJLEVEL																		AS	GUARANTORIRATING     --保证人内部评级
								,T6.PD																						AS	GUARANTORPD          --保证人违约概率
								,''																								AS	GROUPID              --分组编号
    FROM RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.SERIALNO=T2.SERIALNO
    LEFT JOIN RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON T1.GUARANTORID=T6.CUSTID
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.GUARANTYTYPE='010'
    ;
    COMMIT;

    --将有效借据下对应合同的保证插入到目标表中（抵质押品类型为信用证，备用信用证都归为保证）
    INSERT INTO RWA_DEV.RWA_XD_GUARANTEE(
         				 DATADATE          												  --数据日期
								,DATANO                                     --数据流水号
								,GUARANTEEID                                --保证ID
								,SSYSID                                     --源系统ID
								,GUARANTEECONID                             --保证合同ID
								,GUARANTORID                                --保证人ID
								,CREDITRISKDATATYPE                         --信用风险数据类型
								,GUARANTEEWAY                            		--担保方式
								,QUALFLAGSTD                            		--权重法合格标识
								,QUALFLAGFIRB                               --内评初级法合格标识
								,GUARANTEETYPESTD                           --权重法保证类型
								,GUARANTORSDVSSTD                           --权重法保证人细分
								,GUARANTEETYPEIRB                           --内评法保证类型
								,GUARANTEEAMOUNT                            --保证总额
								,CURRENCY                                   --币种
								,STARTDATE                                  --起始日期
								,DUEDATE                                    --到期日期
								,ORIGINALMATURITY                           --原始期限
								,RESIDUALM                                  --剩余期限
								,GUARANTORIRATING                           --保证人内部评级
								,GUARANTORPD                                --保证人违约概率
								,GROUPID                                    --分组编号
    )WITH TEMP_GUARANTEE1 AS(SELECT T3.SERIALNO AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
                                    ,MAX(NVL(T1.ACTUALMATURITY,T3.MATURITY)) AS MATURITY, MIN(T2.ATTRIBUTE1) AS ATTRIBUTE1
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '3%'  --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA.RWA_WS_ABS_ISSUE_UNDERASSET RWAIU
                               ON T1.SERIALNO=RWAIU.HTBH
                               AND RWAIU.DATADATE = TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
                               INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT RWD
                               ON          RWAIU.SUPPORGID=RWD.ORGID
                               AND         RWD.DATADATE=TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
                               AND         RWD.SUPPTMPLID='M-0133'
                               AND         RWD.SUBMITFLAG='1'
                               INNER JOIN 	RWA_DEV.RWA_ABS_ISSURE_CONTRACT T4
    													 ON					'ABS'||T3.SERIALNO = T4.CONTRACTID
                               WHERE T1.DATANO=P_DATA_DT_STR
                               GROUP BY T3.SERIALNO
		                         )
		,TEMP_RELATIVE AS (SELECT T5.GUARANTYID,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(ATTRIBUTE1) AS ATTRIBUTE1
                       FROM TEMP_GUARANTEE1 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      INNER JOIN (SELECT CONTRACTNO, GUARANTYID
                                  FROM RWA_DEV.NCM_GUARANTY_RELATIVE
                                  WHERE DATANO=P_DATA_DT_STR
                                  GROUP BY CONTRACTNO, GUARANTYID
                                  ) T4
                      ON T3.SERIALNO=T4.CONTRACTNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5
                      ON T4.GUARANTYID=T5.GUARANTYID
                      AND T5.DATANO=P_DATA_DT_STR
                      GROUP BY T5.GUARANTYID
                   )
		 , TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID, CERTTYPE, CERTID
                      FROM (SELECT T1.CUSTOMERID,
                                   T1.CERTTYPE,
                                   T1.CERTID,
                                   ROW_NUMBER() OVER(PARTITION BY T1.CERTTYPE, T1.CERTID ORDER BY T1.CUSTOMERID) AS RM
                              FROM RWA_DEV.NCM_CUSTOMER_INFO T1
                             WHERE EXISTS
                             (SELECT 1
                                      FROM RWA_DEV.NCM_GUARANTY_INFO T2
                                     WHERE T1.CERTID = T2.OBLIGEEIDNUMBER
                                       AND T2.DATANO = p_data_dt_str
                                       AND T2.GUARANTYTYPEID IN
                                           ('004001004001',
                                            '004001005001',
                                            '004001006001',
                                            '004001006002',
                                            '001001003001')
                                       AND T2.AFFIRMVALUE0 > 0)
                               AND T1.DATANO = p_data_dt_str)
                     WHERE RM = 1
    )
    SELECT
         				 TO_DATE(T1.DATANO,'YYYYMMDD')										AS  DATADATE          		 --数据日期
         				,T1.DATANO																 		    AS	DATANO               --数据流水号
         				,'ABS'||T1.GUARANTYID												      AS	GUARANTEEID          --保证ID
								,'ABS'																						AS	SSYSID               --源系统ID
								,''																		            AS	GUARANTEECONID       --保证合同ID
								,NVL(T7.CUSTOMERID,'XN-YBGS')									    AS	GUARANTORID          --保证人ID
								,CASE WHEN T2.ATTRIBUTE1='1' THEN '01'
                      ELSE '02'
                 END																							AS	CREDITRISKDATATYPE   --信用风险数据类型
								,T1.GUARANTYTYPE																	AS	GUARANTEEWAY       	 --担保方式
								,''																								AS	QUALFLAGSTD          --权重法合格标识
								,CASE WHEN T1.QUALIFYFLAG03='01' THEN '1'
                	    WHEN T1.QUALIFYFLAG03='02' THEN '0'
                      ELSE ''
                 END																							AS	QUALFLAGFIRB         --内评初级法合格标识
								,''																								AS	GUARANTEETYPESTD     --权重法保证类型
								,''																								AS	GUARANTORSDVSSTD     --权重法保证人细分
								,CASE WHEN T1.QUALIFYFLAG03='01' THEN '020201'
								      ELSE ''
								 END																							AS	GUARANTEETYPEIRB     --内评法保证类型
								,T1.AFFIRMVALUE0																	AS	GUARANTEEAMOUNT      --保证总额
								,NVL(T1.AFFIRMCURRENCY,'CNY')										  AS	CURRENCY             --币种
								,T2.PUTOUTDATE																	  AS	STARTDATE            --起始日期
								,T2.MATURITY																			AS	DUEDATE              --到期日期
								,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365
								END                                               AS  ORIGINALMATURITY   	 --原始期限
								,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
								END      											                    AS	RESIDUALM            --剩余期限
								,T6.PDADJLEVEL																		AS	GUARANTORIRATING     --保证人内部评级
								,T6.PD																						AS	GUARANTORPD          --保证人违约概率
								,''																								AS	GROUPID              --分组编号
    FROM RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID = T2.GUARANTYID
    LEFT JOIN RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON T1.CLRERID=T6.CUSTID
    LEFT JOIN		TEMP_CUST_INFO T7
    ON					T1.OBLIGEEIDTYPE = T7.CERTTYPE
    AND 				T1.OBLIGEEIDNUMBER = T7.CERTID
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.GUARANTYTYPE  IN('004001004001','004001005001','004001006001','004001006002')     --信用证，备用信用证,融资性保函，非融资性保函 都归为保证
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ABS_ISSURE_GUARANTEE',cascade => true);

    /*目标表数据统计*/
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ABS_ISSURE_GUARANTEE;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count1;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '资产证券化-保证(PRO_RWA_ABS_ISSURE_GUARANTEE)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ABS_ISSURE_GUARANTEE;
/

