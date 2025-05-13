CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XD_CMRELEVENCE(
			 											P_DATA_DT_STR	IN	VARCHAR2,		--数据日期
       											P_PO_RTNCODE	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														P_PO_RTNMSG		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_XD_CMRELEVENCE
    实现功能:信贷系统-合同与缓释物关联,表结构为合同缓释物关联表
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2016-04-28
    单  位	:上海安硕信息技术股份有限公司
    源  表1	:NCM_GUARANTY_INFO|担保物信息表
    源  表2	:NCM_BUSINESS_DUEBILL|授信业务借据信息表
    源  表3	:NCM_BUSINESS_CONTRACT|授信业务合同表
    源  表4	:NCM_GUARANTY_CONTRACT|担保合同信息表
    源  表5	:NCM_CONTRACT_RELATIVE|合同关联表
    源  表6	:NCM_GUARANTY_RELATIVE|担保合同与担保物关联表
    源  表7	:RD_LOAN_NOR|正常贷款
    目标表	:RWA_XD_CMRELEVENCE|信贷系统-合同与缓释物关联
    辅助表	:无
    变更记录(修改人|修改时间|修改内容):
    xlp  20190412 调整一期核心数据关联条件
    
    
    
  */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  --v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XD_CMRELEVENCE';
  --定义判断值变量
  v_count1 INTEGER;
  --定义异常变量
  v_raise EXCEPTION;
    --定义临时表名
  --v_tabname VARCHAR2(200);
  --定义创建语句
  --v_create VARCHAR2(1000);

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_XD_CMRELEVENCE';

    /*1.1 插入有效借据下合同对应的抵质押品数据(普通)*/
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
         				 DATADATE       									--数据日期
         				,DATANO                           --数据流水号
         				,CONTRACTID                       --合同代号
         				,MITIGATIONID                     --缓释物代号
         				,MITIGCATEGORY                    --缓释物类型
         				,SGUARCONTRACTID                  --源担保合同代号
         				,GROUPID                          --分组编号
                ,FLAG
    )WITH TEMP_CMRELEVENCE1 AS(SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --有效判断条件
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --排除同业，回购，投资，委托贷款业务,贴现和转帖也排除，用票据信息作为缓释
                             )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T5.GUARANTYID,T1.CONTRACTNO
                       FROM TEMP_CMRELEVENCE1 T1
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
                   )

     SELECT
         				TO_DATE(P_DATA_DT_STR,'YYYYMMDD')										          AS	datadate       									--数据日期
         				,T1.DATANO																			              AS	datano              						--数据流水号
         				,T2.CONTRACTNO																			          AS	contractid          						--合同代号  (关联主合同，判断主合同是否有效)
         				,'YP'||T1.GUARANTYID                         								  AS 	mitigationid        						--缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
         				,'03' 						  																          AS	mitigcategory       						--缓释物类型   全是抵质押品
         				,''      																	                    AS	sguarcontractid     						--源担保合同代号(担保编号)
         				,''																									          AS	groupid             						--分组编号
                ,'DZY|PT'
    FROM   RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID=T2.GUARANTYID
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'                  --modify by yushuangjiang
    --AND T1.CLRGNTSTATUS IN ('03','10')     --二期经过跟信贷陈康确定把这两个状态限定条件去掉
    AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')     --信用证，备用信用证,融资性保函，非融资性保函 都归为保证
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;

    /*1.2 插入有效借据下合同对应的抵质押品数据(逾期贷款-微粒贷)*/
   /* INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --数据日期
                ,DATANO                           --数据流水号
                ,CONTRACTID                       --合同代号
                ,MITIGATIONID                     --缓释物代号
                ,MITIGCATEGORY                    --缓释物类型
                ,SGUARCONTRACTID                  --源担保合同代号
                ,GROUPID                          --分组编号
                ,flag
    )WITH TEMP_CMRELEVENCE1 AS(SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN \*RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND (T31.OVERDUEBALANCE+T31.DULLBALANCE+T31.BADBALANCE)>0   --取到逾期的记录*\
                                          rwa_dev.brd_loan_nor t31
                               ON  T1.SERIALNO = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.sbjt_cd = '13100001' --逾期微粒贷
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               --AND T1.BUSINESSTYPE='11103030'  --只取微粒贷业务
                               )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T5.GUARANTYID,T1.CONTRACTNO
                       FROM TEMP_CMRELEVENCE1 T1
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
                   )

     SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                             AS  datadate                        --数据日期
                ,T1.DATANO                                                    AS  datano                          --数据流水号
                ,T2.CONTRACTNO                                                AS  contractid                      --合同代号  (关联主合同，判断主合同是否有效)
                ,'YP'||T1.GUARANTYID                                          AS  mitigationid                    --缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
                ,'03'                                                         AS  mitigcategory                   --缓释物类型   全是抵质押品
                ,''                                                           AS  sguarcontractid                 --源担保合同代号(担保编号)
                ,''                                                           AS  groupid                         --分组编号
                ,'DZY|YQWLD'
    FROM   RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID=T2.GUARANTYID
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'                  --modify by yushuangjiang
    --AND T1.CLRGNTSTATUS IN ('03','10')     --二期经过跟信贷陈康确定把这两个状态限定条件去掉
    AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')     --信用证，备用信用证,融资性保函，非融资性保函 都归为保证
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T7 WHERE 'YP'||T2.GUARANTYID=T7.mitigationid AND T2.CONTRACTNO=T7.contractid AND T7.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;*/
    
    /*1.3 插入有效借据下合同对应的抵质押品数据(逾期贷款-其余业务)*/
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --数据日期
                ,DATANO                           --数据流水号
                ,CONTRACTID                       --合同代号
                ,MITIGATIONID                     --缓释物代号
                ,MITIGCATEGORY                    --缓释物类型
                ,SGUARCONTRACTID                  --源担保合同代号
                ,GROUPID                          --分组编号
                ,flag
    )WITH TEMP_CMRELEVENCE1 AS(SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN BRD_LOAN_NOR T4 
                               ON  T4.DATANO = P_DATA_DT_STR
                               AND substr(T4.SBJT_CD,1,4) = '1310' --科目编号 逾期贷款
                               and T4.SBJT_CD != '13100001' --所有不含微粒贷的逾期贷款
                               AND T4.CRDT_ACCT_NO = T1.SERIALNO
                                       WHERE  T1.DATANO=P_DATA_DT_STR
    
                               )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T5.GUARANTYID,T1.CONTRACTNO
                       FROM TEMP_CMRELEVENCE1 T1
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
                   )

     SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                             AS  datadate                        --数据日期
                ,T1.DATANO                                                    AS  datano                          --数据流水号
                ,T2.CONTRACTNO                                                AS  contractid                      --合同代号  (关联主合同，判断主合同是否有效)
                ,'YP'||T1.GUARANTYID                                          AS  mitigationid                    --缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
                ,'03'                                                         AS  mitigcategory                   --缓释物类型   全是抵质押品
                ,''                                                           AS  sguarcontractid                 --源担保合同代号(担保编号)
                ,''                                                           AS  groupid                         --分组编号
                ,'DZY|YQ'
    FROM   RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID=T2.GUARANTYID
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'                  --modify by yushuangjiang
    --AND T1.CLRGNTSTATUS IN ('03','10')     --二期经过跟信贷陈康确定把这两个状态限定条件去掉
    AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')     --信用证，备用信用证,融资性保函，非融资性保函 都归为保证
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T7 WHERE 'YP'||T2.GUARANTYID=T7.mitigationid AND T2.CONTRACTNO=T7.contractid AND T7.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    /*1.4 插入有效借据下合同对应的抵质押品数据(追加到PUTOUT表 上的抵质押品信息)*/
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --数据日期
                ,DATANO                           --数据流水号
                ,CONTRACTID                       --合同代号
                ,MITIGATIONID                     --缓释物代号
                ,MITIGCATEGORY                    --缓释物类型
                ,SGUARCONTRACTID                  --源担保合同代号
                ,GROUPID                          --分组编号
                ,flag
    )WITH TEMP_CMRELEVENCE1 AS(SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO,T4.SERIALNO AS BPSERIALNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T4
                               ON T3.SERIALNO=T4.CONTRACTSERIALNO
                               AND T4.DATANO=P_DATA_DT_STR
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          /*rwa_dev.brd_loan_nor t31
                               ON   t1.serialno=t31.crdt_acct_no
                               AND  t31.datano = P_DATA_DT_STR
                               AND  t31.cur_bal > 0*/
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --有效判断条件
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --排除同业，回购，投资，委托贷款业务,贴现和转帖也排除，用票据信息作为缓释
                             )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T5.GUARANTYID,T1.CONTRACTNO
                       FROM TEMP_CMRELEVENCE1 T1
                      INNER JOIN (SELECT OBJECTNO, GUARANTYID
                                  FROM RWA_DEV.NCM_GUARANTY_RELATIVE
                                  WHERE DATANO=P_DATA_DT_STR
                                  AND OBJECTTYPE='PutOutApply'
                                  GROUP BY OBJECTNO, GUARANTYID
                                  ) T4
                      ON T1.BPSERIALNO=T4.OBJECTNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5
                      ON T4.GUARANTYID=T5.GUARANTYID
                      AND T5.DATANO=P_DATA_DT_STR
                   )

     SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                             AS  datadate                        --数据日期
                ,T1.DATANO                                                    AS  datano                          --数据流水号
                ,T2.CONTRACTNO                                                AS  contractid                      --合同代号  (关联主合同，判断主合同是否有效)
                ,'YP'||T1.GUARANTYID                                          AS  mitigationid                    --缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
                ,'03'                                                         AS  mitigcategory                   --缓释物类型   全是抵质押品
                ,''                                                           AS  sguarcontractid                 --源担保合同代号(担保编号)
                ,''                                                           AS  groupid                         --分组编号
                ,'DZY|PUTOUT'
    FROM   RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID=T2.GUARANTYID
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'                  --modify by yushuangjiang
    --AND T1.CLRGNTSTATUS IN ('03','10')     --二期经过跟信贷陈康确定把这两个状态限定条件去掉
    AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')     --信用证，备用信用证,融资性保函，非融资性保函 都归为保证
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T7 WHERE 'YP'||T2.GUARANTYID=T7.mitigationid AND T2.CONTRACTNO=T7.contractid AND T7.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND C.DATANO=P_DATA_DT_STR) 
   ;
    COMMIT;
    
    /*1.5 插入有效借据下合同对应的抵质押品数据(票据贴现，转帖现_外转)*/
   /* INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --数据日期
                ,DATANO                           --数据流水号
                ,CONTRACTID                       --合同代号
                ,MITIGATIONID                     --缓释物代号
                ,MITIGCATEGORY                    --缓释物类型
                ,SGUARCONTRACTID                  --源担保合同代号
                ,GROUPID                          --分组编号
                ,flag
    )\*WITH TEMP_CMRELEVENCE1 AS(SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               AND T3.DATANO=P_DATA_DT_STR
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'    --排除外部转帖现
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          \*rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND    t31.datano = P_DATA_DT_STR
                               AND    t31.cur_bal > 0*\
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --有效判断条件
                               AND T1.BUSINESSTYPE IN ('10302010','10302015','10302020')  --贴现和转帖，用票据信息作为缓释
                             )*\
     SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                             AS  datadate                        --数据日期
                ,T1.DATANO                                                    AS  datano                          --数据流水号
                ,T2.CONTRACTNO                                                AS  contractid                      --合同代号  (关联主合同，判断主合同是否有效)
                ,'PJ'||T1.SERIALNO                                            AS  mitigationid                    --缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
                ,'03'                                                         AS  mitigcategory                   --缓释物类型   全是抵质押品
                ,''                                                           AS  sguarcontractid                 --源担保合同代号(担保编号)
                ,''                                                           AS  groupid                         --分组编号
                ,'DZY|TXZT'
    FROM RWA_DEV.NCM_BILL_INFO T1
    INNER JOIN (SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               AND T3.DATANO=P_DATA_DT_STR
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'    --排除外部转帖现
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          \*rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND    t31.datano = P_DATA_DT_STR
                               AND    t31.cur_bal > 0*\
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --有效判断条件
                               AND T1.BUSINESSTYPE IN ('10302010','10302015','10302020')  --贴现和转帖，用票据信息作为缓释
                           ) T2
    ON T1.OBJECTNO = T2.CONTRACTNO
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.OBJECTTYPE='BusinessContract'
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid and C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;*/
    
    /*2.1 插入有效借据下合同有保证金的数据*/
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --数据日期
                ,DATANO                           --数据流水号
                ,CONTRACTID                       --合同代号
                ,MITIGATIONID                     --缓释物代号
                ,MITIGCATEGORY                    --缓释物类型
                ,SGUARCONTRACTID                  --源担保合同代号
                ,GROUPID                          --分组编号
                ,flag
    )WITH TEMP_CMRELEVENCE3 AS(SELECT DISTINCT  T1.RELATIVESERIALNO2 AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                         /* rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND   t31.datano = P_DATA_DT_STR
                               AND   t31.cur_bal > 0*/
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --有效判断条件
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --排除同业，回购，投资，委托贷款业务,贴现和转帖也排除，用票据信息作为缓释
                             )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --数据日期
                ,T1.DATANO                                                   AS datano                          --数据流水号
                ,T1.SERIALNO                                                 AS contractid                      --合同代号  (关联主合同，判断主合同是否有效)
                ,'HT'|| T2.CONTRACTNO || T3.BAILCURRENCY                     AS mitigationid                    --缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
                ,'03'                                                        AS mitigcategory                   --缓释物类型   保证金是金融质押品
                ,T1.SERIALNO                                                 AS sguarcontractid                 --源担保合同代号(担保编号)
                ,''                                                          AS groupid                         --分组编号
                ,'BZJ|PT'
    FROM  RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_CMRELEVENCE3 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.RWA_TEMP_BAIL1 T3
    ON T1.SERIALNO=T3.CONTRACTNO
    AND T3.BAILBALANCE<9999999999999    --剔除脏数据
    --AND T3.ISMAX='1'            --这个是一期的逻辑，这里关联的BAIL2表，改为关联BAIL1表不需要加这个标志  modify by yushuangjiang
    WHERE T1.DATANO=P_DATA_DT_STR
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T1.SERIALNO = c.contractid and C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    /*2.2 插入有效借据下合同有保证金的数据（逾期贷款-微粒贷）*/
    /*INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --数据日期
                ,DATANO                           --数据流水号
                ,CONTRACTID                       --合同代号
                ,MITIGATIONID                     --缓释物代号
                ,MITIGCATEGORY                    --缓释物类型
                ,SGUARCONTRACTID                  --源担保合同代号
                ,GROUPID                          --分组编号
                ,flag
    )WITH TEMP_CMRELEVENCE3 AS(SELECT DISTINCT  T1.RELATIVESERIALNO2 AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN \*RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND (T31.OVERDUEBALANCE+T31.DULLBALANCE+T31.BADBALANCE)>0   --取到逾期的记录*\
                                          rwa_dev.brd_loan_nor t31
                               ON     t1.serialno = t31.crdt_acct_no
                               AND    t31.datano = P_DATA_DT_STR
                               AND    t31.sbjt_cd = '13100001' --逾期微粒贷
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               --AND T1.BUSINESSTYPE='11103030'  --只取微粒贷业务
                               )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --数据日期
                ,T1.DATANO                                                   AS datano                          --数据流水号
                ,T1.SERIALNO                                                 AS contractid                      --合同代号  (关联主合同，判断主合同是否有效)
                ,'HT' || T2.CONTRACTNO || T3.BAILCURRENCY                    AS mitigationid                    --缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
                ,'03'                                                        AS mitigcategory                   --缓释物类型   保证金是金融质押品
                ,T1.SERIALNO                                                 AS sguarcontractid                 --源担保合同代号(担保编号)
                ,''                                                          AS groupid                         --分组编号
                ,'BZJ|YQWLD'
    FROM  RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_CMRELEVENCE3 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.RWA_TEMP_BAIL1 T3
    ON T1.SERIALNO=T3.CONTRACTNO
    --AND T3.ISMAX='1'       --这个是一期的逻辑，这里关联的BAIL2表，改为关联BAIL1表不需要加这个标志  modify by yushuangjiang
    WHERE T1.DATANO=P_DATA_DT_STR
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T3 WHERE 'HT'||T1.SERIALNO||T3.BAILCURRENCY=T3.MITIGATIONID AND T1.SERIALNO=T3.CONTRACTID AND T3.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T1.SERIALNO = c.contractid and C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;*/
    
    /*2.3 插入有效借据下合同有保证金的数据（逾期贷款-其余业务）*/
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --数据日期
                ,DATANO                           --数据流水号
                ,CONTRACTID                       --合同代号
                ,MITIGATIONID                     --缓释物代号
                ,MITIGCATEGORY                    --缓释物类型
                ,SGUARCONTRACTID                  --源担保合同代号
                ,GROUPID                          --分组编号
                ,flag
    )WITH TEMP_CMRELEVENCE3 AS(
                               SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN BRD_LOAN_NOR T4 
                               ON  T4.DATANO = P_DATA_DT_STR
                               AND substr(T4.SBJT_CD,1,4) = '1310' --科目编号
                               AND t4.sbjt_cd != '13100001' --所有不含微粒贷的逾期贷款
                               AND T4.CRDT_ACCT_NO = T1.SERIALNO
                                       WHERE  T1.DATANO=P_DATA_DT_STR
                               )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --数据日期
                ,T1.DATANO                                                   AS datano                          --数据流水号
                ,T1.SERIALNO                                                 AS contractid                      --合同代号  (关联主合同，判断主合同是否有效)
                ,'HT' || T2.CONTRACTNO || T3.BAILCURRENCY                    AS mitigationid                    --缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
                ,'03'                                                        AS mitigcategory                   --缓释物类型   保证金是金融质押品
                ,T1.SERIALNO                                                 AS sguarcontractid                 --源担保合同代号(担保编号)
                ,''                                                          AS groupid                         --分组编号
                ,'BZJ|YQ'
    FROM  RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_CMRELEVENCE3 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.RWA_TEMP_BAIL1 T3
    ON T1.SERIALNO=T3.CONTRACTNO
    AND T3.BAILBALANCE<9999999999999    --剔除脏数据
    --AND T3.ISMAX='1'         --这个是一期的逻辑，这里关联的BAIL2表，改为关联BAIL1表不需要加这个标志  modify by yushuangjiang
    WHERE T1.DATANO=P_DATA_DT_STR
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T3 WHERE 'HT'||T1.SERIALNO||T3.BAILCURRENCY=T3.MITIGATIONID AND T1.SERIALNO=T3.CONTRACTID AND T3.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T1.SERIALNO = c.contractid and C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    --3.1 插入保证关联信息表-普通保证
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --数据日期
                ,DATANO                           --数据流水号
                ,CONTRACTID                       --合同代号
                ,MITIGATIONID                     --缓释物代号
                ,MITIGCATEGORY                    --缓释物类型
                ,SGUARCONTRACTID                  --源担保合同代号
                ,GROUPID                          --分组编号
                ,flag
    )WITH TEMP_CMRELEVENCE4 AS(SELECT DISTINCT  T1.RELATIVESERIALNO2 AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          /*rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND   t31.datano = P_DATA_DT_STR
                               AND   t31.cur_bal > 0*/
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --有效判断条件
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010','10201060','10202080','10201080','1020301010','1020301020'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --排除同业，回购，投资，委托贷款业务,贴现和转帖也排除，用票据信息作为缓释
                             )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T1.CONTRACTNO,T3.SERIALNO
                       FROM TEMP_CMRELEVENCE4 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON  T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      AND T3.GUARANTYTYPE='010'
                    )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --数据日期
                ,T1.DATANO                                                   AS datano                          --数据流水号
                ,T2.CONTRACTNO                                               AS contractid                      --合同代号  (关联主合同，判断主合同是否有效)
                ,'BZ'||T1.SERIALNO                                           AS mitigationid                    --缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
                ,'02'                                                        AS mitigcategory                   --缓释物类型   02-保证
                ,T1.SERIALNO                                                 AS sguarcontractid                 --源担保合同代号(担保编号)
                ,''                                                          AS groupid                         --分组编号
                ,'BZ|PT'
    FROM RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.SERIALNO=T2.SERIALNO
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.GUARANTYTYPE='010' --010保证
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid and C.DATANO=P_DATA_DT_STR);
    COMMIT;
    
    --3.2 插入保证关联信息表-保证（逾期贷款-微粒贷）
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --数据日期
                ,DATANO                           --数据流水号
                ,CONTRACTID                       --合同代号
                ,MITIGATIONID                     --缓释物代号
                ,MITIGCATEGORY                    --缓释物类型
                ,SGUARCONTRACTID                  --源担保合同代号
                ,GROUPID                          --分组编号
                ,flag
    )WITH TEMP_CMRELEVENCE4 AS(SELECT DISTINCT  T1.RELATIVESERIALNO2 AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN /*RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND (T31.OVERDUEBALANCE+T31.DULLBALANCE+T31.BADBALANCE)>0   --取到逾期的记录*/
                                          rwa_dev.brd_loan_nor t31
                               ON     t1.serialno = t31.crdt_acct_no
                               AND    t31.datano = P_DATA_DT_STR
                               AND    t31.sbjt_cd = '13100001'--逾期微粒贷
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               --AND T1.BUSINESSTYPE='11103030'  --只取微粒贷业务
                               )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T1.CONTRACTNO,T3.SERIALNO
                       FROM TEMP_CMRELEVENCE4 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON  T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      AND T3.GUARANTYTYPE='010'
                    )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --数据日期
                ,T1.DATANO                                                   AS datano                          --数据流水号
                ,T2.CONTRACTNO                                               AS contractid                      --合同代号  (关联主合同，判断主合同是否有效)
                ,'BZ'||T1.SERIALNO                                           AS mitigationid                    --缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
                ,'02'                                                        AS mitigcategory                   --缓释物类型   02-保证
                ,T1.SERIALNO                                                 AS sguarcontractid                 --源担保合同代号(担保编号)
                ,''                                                          AS groupid                         --分组编号
                ,'BZ|YQWLD'
    FROM RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.SERIALNO=T2.SERIALNO
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.GUARANTYTYPE='010'
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T3 WHERE T2.CONTRACTNO=T3.contractid AND 'BZ'||T2.SERIALNO=T3.mitigationid AND T3.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid and C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    --3.3 插入保证关联信息表-保证（逾期贷款-其余业务）
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --数据日期
                ,DATANO                           --数据流水号
                ,CONTRACTID                       --合同代号
                ,MITIGATIONID                     --缓释物代号
                ,MITIGCATEGORY                    --缓释物类型
                ,SGUARCONTRACTID                  --源担保合同代号
                ,GROUPID                          --分组编号
                ,flag
    )WITH TEMP_CMRELEVENCE4 AS(
                               SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN BRD_LOAN_NOR T4 
                               ON  T4.DATANO = P_DATA_DT_STR
                               AND T4.CRDT_ACCT_NO = T1.SERIALNO
                               AND substr(T4.SBJT_CD,1,4) = '1310' --科目编号
                               AND T4.SBJT_CD != '13100001' --所有不含微粒贷的逾期贷款
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T1.CONTRACTNO,T3.SERIALNO
                       FROM TEMP_CMRELEVENCE4 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON  T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      AND T3.GUARANTYTYPE='010'
                    )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --数据日期
                ,T1.DATANO                                                   AS datano                          --数据流水号
                ,T2.CONTRACTNO                                               AS contractid                      --合同代号  (关联主合同，判断主合同是否有效)
                ,'BZ'||T1.SERIALNO                                           AS mitigationid                    --缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
                ,'02'                                                        AS mitigcategory                   --缓释物类型   02-保证
                ,T1.SERIALNO                                                 AS sguarcontractid                 --源担保合同代号(担保编号)
                ,''                                                          AS groupid                         --分组编号
                ,'BZ|YQ'
    FROM RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.SERIALNO=T2.SERIALNO
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.GUARANTYTYPE='010'
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T3 WHERE T2.CONTRACTNO=T3.contractid AND 'BZ'||T2.SERIALNO=T3.mitigationid AND T3.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND  C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    --3.4 插入保证关联信息表-追加到PUTOUT表上的保证
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --数据日期
                ,DATANO                           --数据流水号
                ,CONTRACTID                       --合同代号
                ,MITIGATIONID                     --缓释物代号
                ,MITIGCATEGORY                    --缓释物类型
                ,SGUARCONTRACTID                  --源担保合同代号
                ,GROUPID                          --分组编号
                ,flag
    )WITH TEMP_CMRELEVENCE4 AS(SELECT DISTINCT  T1.RELATIVESERIALNO2 AS CONTRACTNO,T3.PUTOUTDATE,T3.MATURITY,T2.ATTRIBUTE1,T4.SERIALNO AS BPSERLANO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T4
                               ON T3.SERIALNO=T4.CONTRACTSERIALNO
                               AND T4.DATANO=P_DATA_DT_STR
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          /*rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND   t31.datano = P_DATA_DT_STR
                               AND   t31.cur_bal > 0*/
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --有效判断条件
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010','10201060','10202080','10201080','1020301010','1020301020'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --排除同业，回购，投资，委托贷款业务,贴现和转帖也排除，用票据信息作为缓释
                             )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T1.CONTRACTNO,T3.SERIALNO
                       FROM TEMP_CMRELEVENCE4 T1
                       INNER JOIN (SELECT OBJECTNO, CONTRACTNO
                                  FROM RWA_DEV.NCM_GUARANTY_RELATIVE
                                  WHERE DATANO=P_DATA_DT_STR
                                  AND OBJECTTYPE='PutOutApply'
                                  GROUP BY OBJECTNO, CONTRACTNO
                                  ) T2
                      ON T1.BPSERLANO=T2.OBJECTNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON T2.CONTRACTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      AND T3.GUARANTYTYPE='010'
                    )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --数据日期
                ,T1.DATANO                                                   AS datano                          --数据流水号
                ,T2.CONTRACTNO                                               AS contractid                      --合同代号  (关联主合同，判断主合同是否有效)
                ,'BZ'||T1.SERIALNO                                                 AS mitigationid                    --缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
                ,'02'                                                        AS mitigcategory                   --缓释物类型   02-保证
                ,T1.SERIALNO                                                 AS sguarcontractid                 --源担保合同代号(担保编号)
                ,''                                                          AS groupid                         --分组编号
                ,'BZ|PUTOUT'
    FROM RWA_DEV.NCM_GUARANTY_CONTRACT T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.SERIALNO=T2.SERIALNO
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.GUARANTYTYPE='010'
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_XD_CMRELEVENCE T3 WHERE T2.CONTRACTNO=T3.contractid AND 'BZ'||T2.SERIALNO=T3.mitigationid AND T3.DATANO=P_DATA_DT_STR)
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid and C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    --3.5 插入保证关联信息表 出口押汇、卖方押汇、福费廷【承兑行不为空-以业务信息作为担保合同信息，担保形式是保证】
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --数据日期
                ,DATANO                           --数据流水号
                ,CONTRACTID                       --合同代号
                ,MITIGATIONID                     --缓释物代号
                ,MITIGCATEGORY                    --缓释物类型
                ,SGUARCONTRACTID                  --源担保合同代号
                ,GROUPID                          --分组编号
                ,flag
    )WITH TEMP_CMRELEVENCE4 AS(SELECT DISTINCT  T1.RELATIVESERIALNO2 AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                         /* rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND   t31.datano = P_DATA_DT_STR
                               AND   t31.cur_bal > 0*/
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --有效判断条件
                               --AND T1.SERIALNO NOT LIKE 'BD%'
                               AND T1.BUSINESSTYPE
                                   IN ('10201060','10202080','10201080') --出口押汇、卖方押汇、福费廷
                             )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --数据日期
                ,T1.DATANO                                                   AS datano                          --数据流水号
                ,T2.CONTRACTNO                                               AS contractid                      --合同代号  (关联主合同，判断主合同是否有效)
                ,'BZ'||T1.SERIALNO                                           AS mitigationid                    --缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
                ,'02'                                                        AS mitigcategory                   --缓释物类型   02-保证
                ,T1.SERIALNO                                                 AS sguarcontractid                 --源担保合同代号(担保编号)
                ,''                                                          AS groupid                         --分组编号
                ,'BZ|YH'
    FROM RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_CMRELEVENCE4 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T3
    ON T1.SERIALNO=T3.CONTRACTSERIALNO
    AND T1.DATANO=T3.DATANO
    AND T3.ACCEPTORBANKID IS NOT NULL          --承兑行不为空才能做保证 20190805 ACCEPTORBANKID不为空的只有5条记录
    WHERE T1.DATANO=P_DATA_DT_STR
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
      
    --3.6 插入保证关联信息表 -有追索权卖方保理、无追索权卖方保理【保理商不为空-以业务信息作为担保合同信息，担保形式是保证】
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --数据日期
                ,DATANO                           --数据流水号
                ,CONTRACTID                       --合同代号
                ,MITIGATIONID                     --缓释物代号
                ,MITIGCATEGORY                    --缓释物类型
                ,SGUARCONTRACTID                  --源担保合同代号
                ,GROUPID                          --分组编号
                ,flag
    )WITH TEMP_CMRELEVENCE4 AS(SELECT DISTINCT  T1.RELATIVESERIALNO2 AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                /*          rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND   t31.datano = P_DATA_DT_STR
                               AND   t31.cur_bal > 0*/
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --有效判断条件
                               --AND T1.SERIALNO NOT LIKE 'BD%'
                               AND T1.BUSINESSTYPE
                                   IN ('1020301010','1020301020'） --有追索权卖方保理、无追索权卖方保理
                             )
    SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                            AS datadate                        --数据日期
                ,T1.DATANO                                                   AS datano                          --数据流水号
                ,T2.CONTRACTNO                                               AS contractid                      --合同代号  (关联主合同，判断主合同是否有效)
                ,'BZ'||T1.SERIALNO                                           AS mitigationid                    --缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
                ,'02'                                                        AS mitigcategory                   --缓释物类型   02-保证
                ,T1.SERIALNO                                                 AS sguarcontractid                 --源担保合同代号(担保编号)
                ,''                                                          AS groupid                         --分组编号
                ,'BZ|BL'
    FROM RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_CMRELEVENCE4 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T3
    ON T1.SERIALNO=T3.CONTRACTSERIALNO
    AND T1.DATANO=T3.DATANO
    AND T3.FACTORID IS NOT NULL               --保理商不为空作为保证 20190805FACTORID不为空的记录为0
    WHERE T1.DATANO=P_DATA_DT_STR
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    --3.7 抵质押品类型为信用证，备用信用证都归为保证
    INSERT INTO RWA_DEV.RWA_XD_CMRELEVENCE(
                 DATADATE                         --数据日期
                ,DATANO                           --数据流水号
                ,CONTRACTID                       --合同代号
                ,MITIGATIONID                     --缓释物代号
                ,MITIGCATEGORY                    --缓释物类型
                ,SGUARCONTRACTID                  --源担保合同代号
                ,GROUPID                          --分组编号
                ,flag
    )WITH TEMP_CMRELEVENCE1 AS(SELECT DISTINCT  T3.SERIALNO AS CONTRACTNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          /*rwa_dev.brd_loan_nor t31
                               ON    t1.serialno = t31.crdt_acct_no
                               AND   t31.datano = P_DATA_DT_STR
                               AND   t31.cur_bal > 0       */
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --有效判断条件
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --排除同业，回购，投资，委托贷款业务,贴现和转帖也排除，用票据信息作为缓释
                             )
    ,TEMP_RELATIVE AS (SELECT DISTINCT T5.GUARANTYID,T1.CONTRACTNO
                       FROM TEMP_CMRELEVENCE1 T1
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
                   )

     SELECT
                TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                             AS  datadate                        --数据日期
                ,T1.DATANO                                                    AS  datano                          --数据流水号
                ,T2.CONTRACTNO                                                AS  contractid                      --合同代号  (关联主合同，判断主合同是否有效)
                ,'BZ'||T1.GUARANTYID                                          AS  mitigationid                    --缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
                ,'02'                                                         AS  mitigcategory                   --缓释物类型   全是抵质押品
                ,''                                                           AS  sguarcontractid                 --源担保合同代号(担保编号)
                ,''                                                           AS  groupid                         --分组编号
                ,'BZ|XYZ'
    FROM   RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID=T2.GUARANTYID
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'                  --modify by yushuangjiang
    --AND T1.CLRGNTSTATUS IN ('03','10')     --二期经过跟信贷陈康确定把这两个状态限定条件去掉
    AND T1.GUARANTYTYPEID IN('004001004001','004001005001','004001006001','004001006002')     --信用证，备用信用证,融资性保函，非融资性保函 都归为保证
    and exists (select 1 from rwa_dev.rwa_xd_contract c where T2.CONTRACTNO = c.contractid AND C.DATANO=P_DATA_DT_STR)
    ;
    COMMIT;
    
    
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_XD_CMRELEVENCE',cascade => true);

    /*目标表数据统计*/
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_XD_CMRELEVENCE;
    --Dbms_output.Put_line('RWA_XD_CMRELEVENCE表当前插入的数据记录为:' || (v_count3-v_count2) || '条');
		--Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

		P_PO_RTNCODE := '1';
	  P_PO_RTNMSG  := '成功'||'-'||v_count1;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 P_PO_RTNCODE := sqlcode;
   			 P_PO_RTNMSG  := '信贷系统-合同与缓释物关联(pro_rwa_xd_cmrelevence)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_XD_CMRELEVENCE;
/

