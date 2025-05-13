CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSURE_CMRELEVENCE(
			 											P_DATA_DT_STR	IN	VARCHAR2,		--数据日期
       											P_PO_RTNCODE	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														P_PO_RTNMSG		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ABS_ISSURE_CMRELEVENCE
    实现功能:信贷系统-合同与缓释物关联,表结构为合同缓释物关联表
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2016-04-28
    单  位	:上海安硕信息技术股份有限公司
    源  表1	:CMS_GUARANTY_INFO|担保物信息表
    源  表2	:CMS_BUSINESS_DUEBILL|授信业务借据信息表
    源  表3	:CMS_BUSINESS_CONTRACT|授信业务合同表
    源  表4	:CMS_GUARANTY_CONTRACT|担保合同信息表
    源  表5	:CMS_CONTRACT_RELATIVE|合同关联表
    源  表6	:CMS_GUARANTY_RELATIVE|担保合同与担保物关联表
    目标表	:RWA_XD_CMRELEVENCE|信贷系统-合同与缓释物关联
    辅助表	:无
    变更记录(修改人|修改时间|修改内容):
  */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_ISSURE_CMRELEVENCE';
  --定义判断值变量
  v_count1 INTEGER;
  --定义异常变量
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE';

    /*插入有效借据下合同对应的抵质押品数据*/
    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE(
         				 DATADATE       									--数据日期
         				,DATANO                           --数据流水号
         				,CONTRACTID                       --合同代号
         				,MITIGATIONID                     --缓释物代号
         				,MITIGCATEGORY                    --缓释物类型
         				,SGUARCONTRACTID                  --源担保合同代号
         				,GROUPID                          --分组编号
    )WITH TEMP_RELATIVE AS (SELECT DISTINCT T5.GUARANTYID,T1.CONTRACTID,T3.GUARANTYTYPE
                       FROM RWA_DEV.RWA_ABS_ISSURE_CONTRACT T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTID = 'ABS' || T2.SERIALNO
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
                   )

     SELECT
         				TO_DATE(P_DATA_DT_STR,'YYYYMMDD')										          AS	datadate       									--数据日期
         				,T1.DATANO																			              AS	datano              						--数据流水号
         				,T2.CONTRACTID																						    AS	contractid          						--合同代号  (关联主合同，判断主合同是否有效)
         				,'ABS'||T1.GUARANTYID                         							  AS 	mitigationid        						--缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
         				,'03' 						  																          AS	mitigcategory       						--缓释物类型   全是抵质押品
         				,''      																	                    AS	sguarcontractid     						--源担保合同代号(担保编号)
         				,''																									          AS	groupid             						--分组编号
    FROM   			RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN 	TEMP_RELATIVE T2
    ON 					T1.GUARANTYID = T2.GUARANTYID
    WHERE 			T1.DATANO=P_DATA_DT_STR
    AND 				T1.GUARANTYTYPE NOT IN ('020080','020090')     --信用证，备用信用证都归为保证
    ;
    COMMIT;

    /*插入有效借据下合同对应的抵质押品数据(信用证归为保证)*/
    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE(
         				 DATADATE       									--数据日期
         				,DATANO                           --数据流水号
         				,CONTRACTID                       --合同代号
         				,MITIGATIONID                     --缓释物代号
         				,MITIGCATEGORY                    --缓释物类型
         				,SGUARCONTRACTID                  --源担保合同代号
         				,GROUPID                          --分组编号
    )WITH
    TEMP_RELATIVE AS (SELECT DISTINCT T5.GUARANTYID,T1.CONTRACTID,T3.GUARANTYTYPE
                       FROM RWA_DEV.RWA_ABS_ISSURE_CONTRACT T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTID = 'ABS' || T2.SERIALNO
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
                   )

     SELECT
         				TO_DATE(P_DATA_DT_STR,'YYYYMMDD')										          AS	datadate       									--数据日期
         				,T1.DATANO																			              AS	datano              						--数据流水号
         				,T2.CONTRACTID																						    AS	contractid          						--合同代号  (关联主合同，判断主合同是否有效)
         				,'ABS'||T1.GUARANTYID                         							  AS 	mitigationid        						--缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
         				,'02' 						  																          AS	mitigcategory       						--缓释物类型   全是抵质押品
         				,''      																	                    AS	sguarcontractid     						--源担保合同代号(担保编号)
         				,''																									          AS	groupid             						--分组编号
    FROM   			RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN 	TEMP_RELATIVE T2
    ON 					T1.GUARANTYID = T2.GUARANTYID
    WHERE 			T1.DATANO = P_DATA_DT_STR
    AND 				T1.GUARANTYTYPE IN ('020080','020090')     --信用证，备用信用证都归为保证
    ;
    COMMIT;


    /*插入有效借据下合同有保证金的数据*/
    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE(
         				 DATADATE       									--数据日期
         				,DATANO                           --数据流水号
         				,CONTRACTID                       --合同代号
         				,MITIGATIONID                     --缓释物代号
         				,MITIGCATEGORY                    --缓释物类型
         				,SGUARCONTRACTID                  --源担保合同代号
         				,GROUPID                          --分组编号
    )
    SELECT
         				TO_DATE(P_DATA_DT_STR,'YYYYMMDD')										         AS	datadate       									--数据日期
         				,P_DATA_DT_STR																	             AS	datano              						--数据流水号
         				,'ABS'||T1.CONTRACTNO																		     AS	contractid          						--合同代号  (关联主合同，判断主合同是否有效)
         				,'ABS'||T1.CONTRACTNO                        								 AS mitigationid        						--缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
         				,'03'								  																       AS	mitigcategory       						--缓释物类型   保证金是金融质押品
         				,T1.CONTRACTNO     																	         AS	sguarcontractid     						--源担保合同代号(担保编号)
         				,''																									         AS	groupid             						--分组编号

		FROM  			RWA_DEV.RWA_TEMP_BAIL2 T1															--信贷合同表
    INNER JOIN	RWA_DEV.RWA_ABS_ISSURE_CONTRACT T2										--信贷借据表
    ON					'ABS' || T1.CONTRACTNO = T2.CONTRACTID
		WHERE 			T1.ISMAX = '1'																				--取相同合同下最大的一笔作为结果
    ;
		COMMIT;


		--插入保证关联信息表
		INSERT INTO RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE(
         				 DATADATE       									--数据日期
         				,DATANO                           --数据流水号
         				,CONTRACTID                       --合同代号
         				,MITIGATIONID                     --缓释物代号
         				,MITIGCATEGORY                    --缓释物类型
         				,SGUARCONTRACTID                  --源担保合同代号
         				,GROUPID                          --分组编号
    )
    WITH TEMP_RELATIVE AS (SELECT DISTINCT T1.CONTRACTID,T3.SERIALNO
                       FROM RWA_DEV.RWA_ABS_ISSURE_CONTRACT T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTID = 'ABS' || T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                    )
    SELECT
         				TO_DATE(P_DATA_DT_STR,'YYYYMMDD')										         AS	datadate       									--数据日期
         				,T1.DATANO																			             AS	datano              						--数据流水号
         				,T2.CONTRACTID																						   AS	contractid          						--合同代号  (关联主合同，判断主合同是否有效)
         				,'ABS'||T1.SERIALNO                         							   AS mitigationid        						--缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
         				,'02'								  																       AS	mitigcategory       						--缓释物类型   02-保证
         				,T2.CONTRACTID     																	         AS	sguarcontractid     						--源担保合同代号(担保编号)
         				,''																									         AS	groupid             						--分组编号

		FROM 				RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN 	TEMP_RELATIVE T2
    ON 					T1.SERIALNO = T2.SERIALNO
    WHERE 			T1.DATANO = P_DATA_DT_STR
    AND 				T1.GUARANTYTYPE IN ('010010','010020','010030')
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ABS_ISSURE_CMRELEVENCE',cascade => true);

    /*目标表数据统计*/
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE;

		P_PO_RTNCODE := '1';
	  P_PO_RTNMSG  := '成功'||'-'||v_count1;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 P_PO_RTNCODE := sqlcode;
   			 P_PO_RTNMSG  := '资产证券化-合同与缓释物关联('||v_pro_name||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ABS_ISSURE_CMRELEVENCE;
/

