CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ACCOUNT_ARTICULATION(p_data_dt_str in VARCHAR2, p_po_rtncode OUT VARCHAR2, p_po_rtnmsg OUT VARCHAR2)
  /*
    存储过程名称:RWA_DEV.pro_rwa_account_articulation
    实现功能:按照二级科目、机构和币种三个维度，计算RWA的风险暴露余额是否与总账中相对应的期末余额能够勾稽。
           表结构为风险暴露表RWA_DEV.RWA_EI_EXPOSURE
    数据口径:全量
    跑批频率:月末
    版  本  :V1.0.0
    编写人  :qpzhong
    编写时间:20161016
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.FNS_GL_BALANCE                   |总账表
    源  表2 :RWA_DEV.NSS_PA_QUOTEPRICE                |汇率转换表
    源  表3 :RWA_DEV.RWA_EI_EXPOSURE                  |汇总-信用风险暴露表
    源  表4 :RWA_DEV.RWA_ARTICULATION_PARAM           |总账勾稽参数表
    源  表5 :RWA.CODE_LIBRARY                         |代码表
    源  表6 :RWA.ORG_INFO OI                          |机构表
    源  表7 :RWA_DEV.RWA_ARTICULATION_TOLERANCE       |总账勾稽容忍度配置表
    源  表8 :RWA_DEV.RWA_TMP_DERIVATION_SUBJECT       |总账勾稽衍生科目临时表
    源  表9 :RWA_DEV.RWA_TMP_GLBALANCE02              |总账勾稽总账余额临时表二
    源  表10:RWA_DEV.RWA_TMP_GLBALANCE                |总账勾稽科目余额临时表
    源  表11:RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2      |总账勾稽衍生科目临时表二
    源  表12:RWA_DEV.RWA_TMP_DERIVATION_BALANCE01     |总账勾稽衍生科目余额临时表一
    源  表13:RWA_DEV.RWA_TMP_DERIVATION_BALANCE02     |总账勾稽衍生科目余额临时表二
    源  表14:RWA_DEV.RWA_TMP_DERIVATION_BALANCE03     |总账勾稽衍生科目余额临时表三
    源  表15:RWA_DEV.RWA_TMP_EXPOBALANCE              |总账勾稽暴露余额临时表
    目标表1 :RWA_DEV.RWA_ARTICULATION_RESULT          |总账勾稽结果表
    目标表2 :RWA_DEV.RWA_EI_CLIENT                    |参与主体表
    目标表3 :RWA_DEV.RWA_EI_CONTRACT                  |合同表
    目标表4 :RWA_DEV.RWA_EI_EXPOSURE                  |暴露表
    目标表5 :RWA_DEV.RWA_TMP_GLBALANCE                |总账勾稽科目余额临时表
    目标表6 :RWA_DEV.RWA_TMP_GLBALANCE02              |总账勾稽总账余额临时表二
    目标表7 :RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2      |总账勾稽衍生科目临时表二
    目标表8 :RWA_DEV.RWA_TMP_DERIVATION_BALANCE01     |总账勾稽衍生科目余额临时表一
    目标表9 :RWA_DEV.RWA_TMP_DERIVATION_BALANCE02     |总账勾稽衍生科目余额临时表二
    目标表10:RWA_DEV.RWA_TMP_DERIVATION_BALANCE03     |总账勾稽衍生科目余额临时表三
    目标表11:RWA_DEV.RWA_TMP_EXPOBALANCE              |总账勾稽暴露余额临时表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  V_BWXTZBJ NUMBER(24,6); ----系统表外从I9获取的所有减值
  V_BWZZZBJ NUMBER(24,6); ----总账按照科目获取的所有 预计负债-表外资产减值准备
  V_BWXHZBJ NUMBER(24,6); ----总账按照科目获取的所有 预计负债-表外资产减值准备
  V_BWCHA   NUMBER(24,6); ----表外总账和系统获取I9的减值的差异，即信用卡表外差异，分摊到表外信用卡每一项暴露
  V_BNXTZBJ NUMBER(24,6); ----系统表内贷款及信用卡从I9获取的所有减值
  V_BNZZZBJ NUMBER(24,6); ----总账按照科目获取的系统所有 贷款损失准备
  V_BNCHA   NUMBER(24,6); ----表内贷款损失准备总账和系统取的减值的差异，即信用卡的差异，分摊至010803
  
  v_pro_name VARCHAR2(200) := 'RWA_DEV.pro_rwa_account_articulation';
  v_datadate date := TO_DATE(p_data_dt_str,'yyyy/mm/dd');   --数据日期
  v_datano VARCHAR2(8) := TO_CHAR(v_datadate, 'yyyymmdd');  --数据流水号
  v_startdate VARCHAR2(10) := TO_CHAR(v_datadate,'yyyy-mm-dd'); --起始日期

  v_count NUMBER := 0;

  v_intolerance NUMBER(24,6) := 0.001; --表内容忍度 默认0.1%
  v_outtolerance NUMBER(24,6) := 0.01; --表外容忍度 默认1%

  --当前衍生科目的基础科目，格式 ''''a'',''b'',''c''''
  --v_subject_str VARCHAR2(1000) := '';

  --无形资产扣减项
  V_ILDDEBT number(24,6):=0;

  --定义遍历总账勾稽容忍度配置信息游标
  CURSOR cursor_type_tolerance IS
    SELECT tolerance_type, tolerance FROM RWA_DEV.rwa_articulation_tolerance;

  cursor_tolerance cursor_type_tolerance%ROWTYPE;

  --定义存储衍生科目的table
  TYPE table_derivation_subject
  IS TABLE OF RWA_DEV.rwa_tmp_derivation_subject.subject_no%TYPE INDEX BY BINARY_INTEGER;

  v_tds table_derivation_subject;

  --定义遍历衍生科目信息游标
  CURSOR cursor_derivation_subject
  IS SELECT subject_no FROM RWA_DEV.rwa_tmp_derivation_subject;

  cursor_ds cursor_derivation_subject%ROWTYPE;

  BEGIN
 
 DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --清除目标表中的原有记录
  DELETE FROM RWA_DEV.rwa_articulation_result WHERE datadate = v_datadate;
  DELETE FROM RWA_DEV.rwa_ei_client WHERE datadate = v_datadate AND ssysid = 'GC';
  DELETE FROM RWA_DEV.rwa_ei_contract WHERE datadate = v_datadate AND ssysid = 'GC';
  DELETE FROM RWA_DEV.rwa_ei_exposure WHERE datadate = v_datadate AND ssysid = 'GC';
  COMMIT;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_GLBALANCE';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_GLBALANCE02';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_DERIVATION_SUBJECT';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_DERIVATION_BALANCE01';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_DERIVATION_BALANCE02';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_DERIVATION_BALANCE03';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_EXPOBALANCE';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_ABSBOND';


 --获取总账金额信息进行汇率折算
  INSERT INTO RWA_DEV.RWA_TMP_GLBALANCE(
              SUBJECT_NO
              ,ORGID
              ,CURRENCY
              ,ACCOUNT_BALANCE
  )
  SELECT FGB.SUBJECT_NO,
         FGB.ORG_ID AS ORGID,
         FGB.CURRENCY_CODE AS CURRENCY,
         CASE WHEN CL.ATTRIBUTE8 = 'C-D' 
           /*总账金额是否转换汇率金额*/
           THEN SUM(FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1))
             ELSE SUM(FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1))
           END AS ACCOUNT_BALANCE --科目余额
    FROM RWA_DEV.FNS_GL_BALANCE FGB
    LEFT JOIN RWA_DEV.TMP_CURRENCY_CHANGE NPQ --汇率中间价表
      ON NPQ.DATANO = FGB.DATANO
     AND NPQ.CURRENCYCODE = FGB.CURRENCY_CODE
    LEFT JOIN RWA.CODE_LIBRARY CL
      ON CL.CODENO = 'NewSubject'
     AND CL.ITEMNO = FGB.SUBJECT_NO
   WHERE FGB.DATANO = v_datano
     AND FGB.CURRENCY_CODE <> 'RMB'
   GROUP BY FGB.SUBJECT_NO, FGB.ORG_ID, CURRENCY_CODE,CL.ATTRIBUTE8;

   COMMIT;

  --获取衍生科目信息
  INSERT INTO RWA_DEV.RWA_TMP_DERIVATION_SUBJECT (SUBJECT_NO, ARTICULATERELATION)
    SELECT T.THIRD_SUBJECT_NO AS SUBJECT_NO, T.ARTICULATERELATION
      FROM RWA_DEV.RWA_ARTICULATION_PARAM T
     WHERE ARTICULATERELATION IS NOT NULL
        AND T.ISINUSE = '1'--1:已启用
        AND T.ARTICULATETYPE = '01' --01:勾稽成本
  ;
  COMMIT;

  --提取总账勾稽参数表中ARTICULATERELATION勾稽关系里包含的科目号
  INSERT INTO RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2
  WITH TEMP_SUBJECT1 AS  --提取简单加减的科目
       (
       SELECT DISTINCT DS.ARTICULATERELATION --原始科目号
                       ,REGEXP_SUBSTR(DS.ARTICULATERELATION,'[^-+]+', 1,LEVEL, 'i') AS REL_SUBJECT_NO --衍生科目
          FROM (
              SELECT * FROM RWA_DEV.RWA_ARTICULATION_PARAM  T
              WHERE UPPER(ARTICULATERELATION) NOT LIKE 'MAX%'
               AND ISCALCULATE = '1' --是否RWA计算 0否 1是
               AND ISINUSE = '1' --启用状态 1启用 0停用
               AND ARTICULATETYPE = '01' --勾稽类型 01:勾稽成本
               AND ARTICULATERELATION IS NOT NULL
           ) DS
           CONNECT BY LEVEL <= LENGTH(REGEXP_REPLACE(DS.ARTICULATERELATION, '[^-+]+', '')) + 1)
        ,
        TEMP_SUbJECT2 AS  --提取复杂科目(去掉ARTICULATERELATION字段MAX(,0))
       (SELECT DISTINCT ARTICULATERELATION--原始科目号
                       ,REGEXP_SUBSTR(ARTICULATERELATION2,'[^-+]+', 1,LEVEL, 'i') AS REL_SUBJECT_NO --衍生科目
          FROM(
         SELECT ARTICULATERELATION,
                --去掉ARTICULATERELATION字段MAX(,0)
                CASE WHEN instr(DS.ARTICULATERELATION,',')>6 THEN SUBSTR(DS.ARTICULATERELATION,5,instr(DS.ARTICULATERELATION,',')-5)
                     ELSE SUBSTR(DS.ARTICULATERELATION,7,LENGTH(DS.ARTICULATERELATION)-7) END as ARTICULATERELATION2
          FROM RWA_DEV.RWA_ARTICULATION_PARAM DS
         WHERE UPPER(DS.ARTICULATERELATION) LIKE 'MAX%'
       AND DS.ISCALCULATE = '1' --是否RWA计算 0否 1是
           AND DS.ISINUSE = '1' --启用状态 1启用 0停用
           AND DS.ARTICULATETYPE = '01' --勾稽类型 01:勾稽成本
           AND DS.ARTICULATERELATION IS NOT NULL
           )
           CONNECT BY LEVEL <= LENGTH(REGEXP_REPLACE(ARTICULATERELATION2, '[^-+]+', '')) + 1)
      SELECT DISTINCT RAP.THIRD_SUBJECT_NO AS SUBJECT_NO, TS.REL_SUBJECT_NO
        FROM RWA_DEV.RWA_ARTICULATION_PARAM RAP
       INNER JOIN (SELECT * FROM TEMP_SUBJECT1
                   UNION
                   SELECT * FROM TEMP_SUBJECT2
                  ) TS
          ON TS.ARTICULATERELATION = RAP.ARTICULATERELATION
       ORDER BY RAP.THIRD_SUBJECT_NO, TS.REL_SUBJECT_NO ASC;
    COMMIT;

  --获取衍生科目金额
  INSERT INTO RWA_DEV.RWA_TMP_DERIVATION_BALANCE01
    (SUBJECT_NO, ORGID, CURRENCY, ARTICULATERELATION, ACCOUNT_BALANCE)
    SELECT GLT.SUBJECT_NO,
           GLT.ORGID,
           GLT.CURRENCY,
           DS.ARTICULATERELATION,
           GLT.ACCOUNT_BALANCE
      FROM RWA_DEV.RWA_TMP_GLBALANCE GLT
     INNER JOIN RWA_DEV.RWA_TMP_DERIVATION_SUBJECT DS
        ON GLT.SUBJECT_NO = DS.SUBJECT_NO;
  COMMIT;

  --获取衍生科目关联科目ARTICULATERELATION里包含的科目对应金额
  INSERT INTO RWA_DEV.RWA_TMP_DERIVATION_BALANCE02 (SUBJECT_NO, ORGID, CURRENCY, ACCOUNT_BALANCE)
  SELECT GLT.SUBJECT_NO, GLT.ORGID, GLT.CURRENCY, SUM(GLT.ACCOUNT_BALANCE )
    FROM RWA_DEV.RWA_TMP_GLBALANCE GLT
   WHERE EXISTS (
    SELECT 1 FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2 DS2 WHERE GLT.SUBJECT_NO = DS2.rel_subject_no
   ) 
  /* WHERE GLT.SUBJECT_NO IN (
  SELECT DISTINCT REGEXP_SUBSTR(DS.ARTICULATERELATION,'[^-+]+',1,LEVEL,'i') AS subject_no
                           FROM (SELECT CASE WHEN INSTR(ARTICULATERELATION,',')=0 THEN ARTICULATERELATION
                                             WHEN INSTR(ARTICULATERELATION,',')>6 THEN SUBSTR(ARTICULATERELATION,5,INSTR(ARTICULATERELATION,',')-5)
                                              ELSE SUBSTR(ARTICULATERELATION,7,LENGTH(ARTICULATERELATION)-7) END AS ARTICULATERELATION
                                   FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT
                                ) DS
                           CONNECT BY LEVEL <= LENGTH(REGEXP_REPLACE(DS.ARTICULATERELATION,'[^-+]+',''))+1
                          )
      */        
  GROUP BY GLT.SUBJECT_NO,GLT.ORGID,GLT.CURRENCY
  ;

  COMMIT;

  --向衍生科目信息表插入数据
  --通过游标获取衍生科目信息
  V_COUNT := 1;
  IF CURSOR_DERIVATION_SUBJECT%ISOPEN = FALSE THEN
     OPEN CURSOR_DERIVATION_SUBJECT;
  END IF;

  LOOP
      FETCH CURSOR_DERIVATION_SUBJECT INTO CURSOR_DS;
      EXIT WHEN CURSOR_DERIVATION_SUBJECT%NOTFOUND;
      V_TDS(V_COUNT) := CURSOR_DS.SUBJECT_NO;
    V_COUNT := V_COUNT+1;
  END LOOP;

  IF CURSOR_DERIVATION_SUBJECT%ISOPEN THEN
     CLOSE CURSOR_DERIVATION_SUBJECT;
  END IF;

  v_count := 0;

  --dbms_output.put_line('衍生科目数:' || v_tds.count);
  --按主科目进行分析
  IF V_TDS.COUNT >0 THEN
     FOR I IN 1..V_TDS.COUNT LOOP
       --变量V_SUBJECT_STR下文未使用 故注释
     /*SELECT REGEXP_REPLACE(ARTICULATERELATION, '[^0-9]+', ',') INTO V_SUBJECT_STR
       FROM (SELECT SUBJECT_NO,CASE WHEN INSTR(ARTICULATERELATION,',')=0 then ARTICULATERELATION
                                    WHEN instr(ARTICULATERELATION,',')>6 THEN SUBSTR(ARTICULATERELATION,5,instr(ARTICULATERELATION,',')-5)
                                    ELSE SUBSTR(ARTICULATERELATION,7,LENGTH(ARTICULATERELATION)-7) END as ARTICULATERELATION
               FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT
              WHERE ARTICULATERELATION LIKE 'MAX%'
              UNION
             SELECT SUBJECT_NO,ARTICULATERELATION
               FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT
              WHERE ARTICULATERELATION NOT LIKE 'MAX%')
      WHERE SUBJECT_NO = V_TDS(I); */

     --dbms_output.put_line('当前字符串:' || v_subject_str);
     INSERT INTO RWA_DEV.RWA_TMP_DERIVATION_BALANCE03(
                     SUBJECT_NO
                     ,ORGID
                     ,CURRENCY
                     ,ARTICULATERELATION
                     ,REPLACED_FUNCTION
                     ,LOGIC_VALUE
        )
        SELECT
             SUBJECT_NO
             ,ORGID
             ,CURRENCY
             ,ARTICULATERELATION
             ,SUBSTR(COMPLEX_LOGIC_FUNCTION,1,INSTR(COMPLEX_LOGIC_FUNCTION,'@',1,1)-1) AS REPLACED_FUNCTION
             ,SUBSTR(COMPLEX_LOGIC_FUNCTION,INSTR(COMPLEX_LOGIC_FUNCTION,'@',1,1)+1) AS LOGIC_VALUE
        FROM (
              WITH GL_TEMP AS (
                       SELECT DISTINCT
                              TT.SUBJECT_NO,
                              REPLACE(T1.ARTICULATERELATION, 'MAX', 'GREATEST') AS ARTICULATERELATION,
                              TT.ORGID,
                              TT.CURRENCY,
                              TT.REL_SUBJECT_NO AS SUB_SUBJECT_NO ,
                              NVL(T2.ACCOUNT_BALANCE, 0) AS SUB_ACCOUNT_BALANCE
                         FROM (SELECT DISTINCT B.SUBJECT_NO,B.REL_SUBJECT_NO,A.ORGID,A.CURRENCY
                                 FROM (SELECT SUBJECT_NO, CURRENCY, ORGID
                                         FROM RWA_DEV.RWA_TMP_DERIVATION_BALANCE02 AA
                                        WHERE EXISTS (
                                              SELECT 1 FROM RWA_DEV.RWA_ARTICULATION_PARAM BB
                                               WHERE BB.THIRD_SUBJECT_NO = V_TDS(I)
                                                 AND INSTR(BB.ARTICULATERELATION,AA.SUBJECT_NO)>0
                                                 AND BB.ISGATHER = '0' --是否汇总到总行勾稽 0:否
                                                 ) 
                                       ) A,
                                      (SELECT SUBJECT_NO,REL_SUBJECT_NO
                                         FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2
                                        WHERE SUBJECT_NO = V_TDS(I)
                                      ) B
                                ) TT
                        LEFT JOIN RWA_DEV.RWA_TMP_DERIVATION_BALANCE01 T1
                          ON T1.SUBJECT_NO=TT.SUBJECT_NO
                        LEFT JOIN RWA_DEV.RWA_TMP_DERIVATION_BALANCE02 T2
                          ON TT.CURRENCY = T2.CURRENCY
                         AND TT.ORGID = T2.ORGID
                         AND TT.REL_SUBJECT_NO = T2.SUBJECT_NO
                       UNION
                       SELECT SUBJECT_NO,ARTICULATERELATION,ORGID,CURRENCY,SUB_SUBJECT_NO,SUM(SUB_ACCOUNT_BALANCE) AS SUB_ACCOUNT_BALANCE 
                       FROM (
                       SELECT DISTINCT TT.SUBJECT_NO,
                                       REPLACE(T1.ARTICULATERELATION, 'MAX', 'GREATEST') AS ARTICULATERELATION,
                                       '9998' AS ORGID,
                                       TT.CURRENCY,
                                       TT.REL_SUBJECT_NO AS SUB_SUBJECT_NO,
                                       NVL(T2.ACCOUNT_BALANCE, 0) AS SUB_ACCOUNT_BALANCE
                         FROM (SELECT DISTINCT B.SUBJECT_NO,
                                               B.REL_SUBJECT_NO,
                                               A.CURRENCY
                                 FROM ( SELECT DISTINCT SUBJECT_NO, CURRENCY
                                          FROM RWA_DEV.RWA_TMP_DERIVATION_BALANCE02 AA
                                         WHERE EXISTS (
                                               SELECT 1 FROM RWA_DEV.RWA_ARTICULATION_PARAM BB
                                                WHERE BB.THIRD_SUBJECT_NO = V_TDS(I)
                                                  AND INSTR(BB.ARTICULATERELATION, AA.SUBJECT_NO) > 0
                                                  AND BB.ISGATHER = '1' --是否汇总到总行勾稽 1:是
                                                  )
                                        ) A,
                                       (SELECT SUBJECT_NO, REL_SUBJECT_NO
                                          FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2
                                         WHERE SUBJECT_NO = V_TDS(I)
                                        ) B
                               ) TT
                         LEFT JOIN (SELECT SUBJECT_NO,CURRENCY,ARTICULATERELATION,SUM(ACCOUNT_BALANCE ) AS ACCOUNT_BALANCE
                            FROM RWA_DEV.RWA_TMP_DERIVATION_BALANCE01
                            WHERE SUBJECT_NO = V_TDS(I)
                            GROUP BY SUBJECT_NO,CURRENCY,ARTICULATERELATION) T1
                           ON T1.SUBJECT_NO = TT.SUBJECT_NO
                         LEFT JOIN (SELECT SUBJECT_NO,CURRENCY,SUM(ACCOUNT_BALANCE ) AS ACCOUNT_BALANCE
                            FROM RWA_DEV.RWA_TMP_DERIVATION_BALANCE02
                            GROUP BY SUBJECT_NO,CURRENCY ) T2
                           ON TT.CURRENCY = T2.CURRENCY
                          AND TT.REL_SUBJECT_NO = T2.SUBJECT_NO
                         )
                       GROUP BY SUBJECT_NO,ARTICULATERELATION,ORGID,CURRENCY,SUB_SUBJECT_NO
               )
               SELECT
                     SUBJECT_NO
                     ,ORGID
                     ,CURRENCY
                     ,ARTICULATERELATION
                     ,FUN_DERIVATION_SUBJECT(SUB_SUBJECT_NO||'@'||ARTICULATERELATION||'@'||SUB_ACCOUNT_BALANCE) AS COMPLEX_LOGIC_FUNCTION
               FROM  GL_TEMP
               GROUP BY SUBJECT_NO ,ORGID ,CURRENCY ,ARTICULATERELATION
         );
         COMMIT;

   END LOOP;
  END IF;

  --更新RWA_DEV.RWA_TMP_GLBALANCE金额
   DELETE FROM RWA_DEV.RWA_TMP_GLBALANCE
    WHERE SUBJECT_NO IN (SELECT SUBJECT_NO FROM RWA_DEV.RWA_TMP_DERIVATION_BALANCE03);

   INSERT INTO RWA_DEV.RWA_TMP_GLBALANCE (SUBJECT_NO, ORGID, CURRENCY, ACCOUNT_BALANCE)
     SELECT T.SUBJECT_NO,
            T.ORGID,
            T.CURRENCY,
            T.LOGIC_VALUE AS ACCOUNT_BALANCE
       FROM RWA_DEV.RWA_TMP_DERIVATION_BALANCE03 T;
   COMMIT;

--通过游标获取总账勾稽容忍度配置信息
  IF CURSOR_TYPE_TOLERANCE%ISOPEN = FALSE THEN
     OPEN CURSOR_TYPE_TOLERANCE;
  END IF;

  LOOP
      FETCH CURSOR_TYPE_TOLERANCE INTO CURSOR_TOLERANCE;
      EXIT WHEN CURSOR_TYPE_TOLERANCE%NOTFOUND;
      IF CURSOR_TOLERANCE.TOLERANCE_TYPE = '01' THEN --表内
        IF CURSOR_TOLERANCE.TOLERANCE IS NOT NULL THEN
           V_INTOLERANCE := CURSOR_TOLERANCE.TOLERANCE;
        END IF;
      ELSE
           IF CURSOR_TOLERANCE.TOLERANCE IS NOT NULL THEN
               V_OUTTOLERANCE := CURSOR_TOLERANCE.TOLERANCE;
           END IF;
      END IF;
  END LOOP;

  IF CURSOR_TYPE_TOLERANCE%ISOPEN THEN
     CLOSE CURSOR_TYPE_TOLERANCE;
  END IF;

  --勾稽本金 是/否汇总到总行勾稽
  INSERT INTO RWA_DEV.RWA_TMP_EXPOBALANCE (
        SUBJECT_NO
        ,ORGID
        ,CURRENCY
        ,EXPOSE_BALANCE
  )
  WITH TEMP_RWA_EI_EXPOSURE AS
   (SELECT R.ACCSUBJECT1,
           R.ORGID,
           R.CURRENCY,
           R.NORMALPRINCIPAL
      FROM RWA_DEV.RWA_EI_EXPOSURE R
     WHERE R.DATADATE = V_DATADATE
       AND R.SSYSID <> 'ABS'
       AND R.ACCSUBJECT1  NOT IN (SELECT REL_SUBJECT_NO FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2)
     UNION ALL
    SELECT RTDS.SUBJECT_NO,
           R.ORGID,
           R.CURRENCY,
           SUM(R.NORMALPRINCIPAL) AS  NORMALPRINCIPAL
      FROM RWA_DEV.RWA_EI_EXPOSURE R
     INNER JOIN RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2 RTDS
        ON RTDS.REL_SUBJECT_NO = R.ACCSUBJECT1
     WHERE R.DATADATE = V_DATADATE
       AND R.SSYSID <> 'ABS'
     GROUP BY RTDS.SUBJECT_NO, R.ORGID, R.CURRENCY
   )
  SELECT REE.ACCSUBJECT1,
         '9998' AS ORGID,
         REE.CURRENCY,
         NVL(SUM(REE.NORMALPRINCIPAL), 0) AS EXPOSE_BALANCE --暴露金额
    FROM TEMP_RWA_EI_EXPOSURE REE
   INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
      ON REE.ACCSUBJECT1 = RAP.THIRD_SUBJECT_NO
     AND RAP.ARTICULATETYPE = '01' --勾稽本金
     AND RAP.ISINUSE = '1' --1：已启用
     AND RAP.ISGATHER = '1' --是否汇总到总行勾稽
   GROUP BY REE.ACCSUBJECT1,  REE.CURRENCY
   UNION ALL
  SELECT REE.ACCSUBJECT1,
         REE.ORGID,
         REE.CURRENCY,
         NVL(SUM(REE.NORMALPRINCIPAL), 0) AS EXPOSE_BALANCE --暴露金额
    FROM TEMP_RWA_EI_EXPOSURE REE
   INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
      ON REE.ACCSUBJECT1 = RAP.THIRD_SUBJECT_NO
     AND RAP.ARTICULATETYPE = '01' --勾稽本金
     AND RAP.ISINUSE = '1' --1：已启用
     AND RAP.ISGATHER = '0' --是否汇总到总行勾稽
   GROUP BY REE.ACCSUBJECT1, REE.ORGID, REE.CURRENCY
  ;

  COMMIT;

  --勾稽利息+勾稽费用 是/否汇总到总行勾稽
  INSERT INTO RWA_DEV.RWA_TMP_EXPOBALANCE (
        SUBJECT_NO
        ,ORGID
        ,CURRENCY
        ,EXPOSE_BALANCE
  )
  WITH TEMP_RWA_EI_EXPOSURE AS
   (SELECT R.ACCSUBJECT1,
           R.ORGID,
           R.CURRENCY,
           R.NORMALINTEREST,
           R.ONDEBITINTEREST,
           R.EXPENSERECEIVABLE
      FROM RWA_DEV.RWA_EI_EXPOSURE R
     WHERE R.DATADATE = V_DATADATE
       AND R.SSYSID <> 'ABS'
   )
  SELECT RAP4.INTERESTSUBJECT AS ACCSUBJECT1,
         '9998' AS ORGID,
         REE.CURRENCY,
         CASE WHEN RAP4.INTERESTTYPE = '01' THEN NVL(SUM(REE.NORMALINTEREST), 0)
              WHEN RAP4.INTERESTTYPE = '03' THEN  NVL(SUM(REE.NORMALINTEREST + REE.ONDEBITINTEREST), 0)
              ELSE NVL(SUM(REE.ONDEBITINTEREST), 0) END AS EXPOSE_BALANCE --暴露金额
    FROM TEMP_RWA_EI_EXPOSURE REE
   INNER JOIN (SELECT DISTINCT RAP3.INTERESTSUBJECT,
                               RAP3.THIRD_SUBJECT_NO,
                               RAP3.INTERESTTYPE
                 FROM RWA_DEV.RWA_ARTICULATION_PARAM RAP2
                INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP3
                   ON RAP2.THIRD_SUBJECT_NO = RAP3.INTERESTSUBJECT
                WHERE RAP2.ISGATHER = '1' --1:汇总到总行勾稽
                  AND RAP2.ARTICULATETYPE = '02' --勾稽利息
                  AND RAP2.ISINUSE = '1'
               ) RAP4
      ON REE.ACCSUBJECT1 = RAP4.THIRD_SUBJECT_NO
   GROUP BY RAP4.INTERESTSUBJECT, RAP4.INTERESTTYPE, REE.CURRENCY
   UNION ALL
  SELECT RAP4.INTERESTSUBJECT AS ACCSUBJECT1,
         REE.ORGID,
         REE.CURRENCY,
         CASE WHEN RAP4.INTERESTTYPE = '01' THEN NVL(SUM(REE.NORMALINTEREST), 0)
              WHEN RAP4.INTERESTTYPE = '03' THEN  NVL(SUM(REE.NORMALINTEREST + REE.ONDEBITINTEREST), 0)
              ELSE NVL(SUM(REE.ONDEBITINTEREST), 0) END AS EXPOSE_BALANCE --暴露金额
    FROM TEMP_RWA_EI_EXPOSURE REE
   INNER JOIN (SELECT DISTINCT RAP3.INTERESTSUBJECT,
                               RAP3.THIRD_SUBJECT_NO,
                               RAP3.INTERESTTYPE
                 FROM RWA_DEV.RWA_ARTICULATION_PARAM RAP2
                INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP3
                   ON RAP2.THIRD_SUBJECT_NO = RAP3.INTERESTSUBJECT
                WHERE RAP2.ISGATHER = '0' --0:不汇总到总行勾稽
                  AND RAP2.ARTICULATETYPE = '02' --勾稽利息
                  AND RAP2.ISINUSE = '1'
               ) RAP4
      ON REE.ACCSUBJECT1 = RAP4.THIRD_SUBJECT_NO
   GROUP BY RAP4.INTERESTSUBJECT,REE.ORGID, RAP4.INTERESTTYPE, REE.CURRENCY
   UNION ALL
  SELECT RAP4.EXPENSESUBJECT AS ACCSUBJECT1,
         '9998' AS ORGID,
         REE.CURRENCY,
         CASE WHEN RAP4.EXPENSETYPE = '01' THEN NVL(SUM(REE.EXPENSERECEIVABLE), 0)
              ELSE NVL(SUM(REE.EXPENSERECEIVABLE), 0) END AS EXPOSE_BALANCE --暴露金额
    FROM TEMP_RWA_EI_EXPOSURE REE
   INNER JOIN (SELECT DISTINCT RAP3.EXPENSESUBJECT,
                               RAP3.THIRD_SUBJECT_NO,
                               RAP3.EXPENSETYPE
                 FROM RWA_DEV.RWA_ARTICULATION_PARAM RAP2
                INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP3
                   ON RAP2.THIRD_SUBJECT_NO = RAP3.EXPENSESUBJECT
                WHERE RAP2.ISGATHER = '1' --1:汇总到总行勾稽
                  AND RAP2.ARTICULATETYPE = '03' --勾稽费用
                  AND RAP2.ISINUSE = '1'
               ) RAP4
      ON REE.ACCSUBJECT1 = RAP4.THIRD_SUBJECT_NO
   GROUP BY RAP4.EXPENSESUBJECT, RAP4.EXPENSETYPE, REE.CURRENCY
   UNION ALL
  SELECT RAP4.EXPENSESUBJECT AS ACCSUBJECT1,
         REE.ORGID,
         REE.CURRENCY,
         CASE WHEN RAP4.EXPENSETYPE = '01' THEN NVL(SUM(REE.EXPENSERECEIVABLE), 0)
              ELSE NVL(SUM(REE.EXPENSERECEIVABLE), 0) END AS EXPOSE_BALANCE --暴露金额
    FROM TEMP_RWA_EI_EXPOSURE REE
   INNER JOIN (SELECT DISTINCT RAP3.EXPENSESUBJECT,
                               RAP3.THIRD_SUBJECT_NO,
                               RAP3.EXPENSETYPE
                 FROM RWA_DEV.RWA_ARTICULATION_PARAM RAP2
                INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP3
                   ON RAP2.THIRD_SUBJECT_NO = RAP3.EXPENSESUBJECT
                WHERE RAP2.ISGATHER = '0' --0:不汇总到总行勾稽
                  AND RAP2.ARTICULATETYPE = '03' --勾稽费用
                  AND RAP2.ISINUSE = '1'
               ) RAP4
      ON REE.ACCSUBJECT1 = RAP4.THIRD_SUBJECT_NO
   GROUP BY RAP4.EXPENSESUBJECT,REE.ORGID, RAP4.EXPENSETYPE, REE.CURRENCY
 ;
  COMMIT;

  --获取资产证券化资产
  INSERT INTO RWA_TMP_ABSBOND (SUBJECTNO,BALANCE,INTEREST,CURRENCY,ORGID)
  with tmp_abs_bond as
 (select bond_id, balance, interest
    from (select T1.bond_id as bond_id
                 ,RANK() OVER(PARTITION BY T1.bond_id ORDER BY T1.sort_seq DESC) AS RECORDNO
                 --INITIAL_COST成本 + INT_ADJUST利息调整 + MKT_VALUE_CHANGE公允价值变动/公允价值变动损益 +ACCOUNTABLE_INT应计利息
                 ,NVL(T1.INITIAL_COST, 0) + NVL(T1.INT_ADJUST, 0) + NVL(T1.MKT_VALUE_CHANGE, 0) + NVL(T1.ACCOUNTABLE_INT, 0) as balance
                 ,NVL(T1.RECEIVABLE_INT, 0) as interest --应收利息
            from rwa_dev.fns_bnd_book_b T1
           inner join (select zqnm
                        from rwa.rwa_wsib_abs_issue_exposure
                       where dataDate = V_DATADATE
                      union
                      select zqnm
                        from rwa.rwa_wsib_abs_invest_exposure
                       where dataDate = V_DATADATE) T2
              on T1.bond_id = T2.ZQNM
           where T1.Datano = V_DATANO AND T1.AS_OF_DATE <= V_DATANO)
   where RECORDNO = 1)
select CASE
         WHEN T1.ASSET_CLASS = '10' THEN
          CASE
            WHEN T1.BOND_TYPE2 IN ('30', '50') THEN
             '11012001' --交易性其他投资本金
            ELSE
             '11010101' --交易性债券投资本金
          END
         WHEN T1.ASSET_CLASS = '20' THEN
          CASE
            WHEN T1.BOND_TYPE2 IN ('30', '50') THEN
             '15012001' --持有至到期其他资产本金
            ELSE
             '15010101' --持有至到期债券资产本金
          END
         WHEN T1.ASSET_CLASS = '40' THEN
          CASE
            WHEN T1.BOND_TYPE2 IN ('30', '50') THEN
             '15032001' --可供出售其他资产本金
            ELSE
             '15030101' --可供出售债券资产本金
          END
       END AS subject_no,
       T2.balance as balance,
       T2.interest as interest,
       NVL(T1.CURRENCY_CODE,'CNY') as currency,
       T1.DEPARTMENT as orgid
  from rwa_dev.fns_bnd_info_b T1
 inner join tmp_abs_bond T2
    on T1.bond_id = T2.bond_id
 where (T1.ASSET_CLASS = '20' OR
       (T1.ASSET_CLASS = '40' AND T1.BOND_TYPE2 NOT IN ('30', '50')) OR
       (T1.ASSET_CLASS = '40' AND T1.BOND_TYPE2 IN ('30', '50') AND
       T1.CLOSED = '1'))
   AND T1.DATANO = V_DATANO;

  COMMIT;
  
  --更新资产证券化所属的本金科目金额
  --汇总
  UPDATE RWA_DEV.RWA_TMP_EXPOBALANCE T
     SET T.EXPOSE_BALANCE = T.EXPOSE_BALANCE +
                            NVL((SELECT SUM(T1.BALANCE)
                               FROM RWA_DEV.RWA_TMP_ABSBOND T1
                              WHERE T1.SUBJECTNO = T.SUBJECT_NO
                                AND T1.CURRENCY = T.CURRENCY
                                AND T1.SUBJECTNO IN (
                                SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '1'
                                )),0)
   WHERE EXISTS (SELECT 1
            FROM RWA_DEV.RWA_TMP_ABSBOND T1
           WHERE T1.SUBJECTNO = T.SUBJECT_NO
             AND T1.CURRENCY = T.CURRENCY
             AND T1.SUBJECTNO IN (
                                SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '1'
                                )
             );

    COMMIT;
     --不汇总
    UPDATE RWA_DEV.RWA_TMP_EXPOBALANCE T
     SET T.EXPOSE_BALANCE = T.EXPOSE_BALANCE +
                            NVL((SELECT SUM(T1.BALANCE)
                               FROM RWA_DEV.RWA_TMP_ABSBOND T1
                              WHERE T1.SUBJECTNO = T.SUBJECT_NO
                                AND T1.CURRENCY = T.CURRENCY
                                AND T1.ORGID = T.ORGID   --不汇总时增加了机构号关联条件
                                AND T1.SUBJECTNO IN (
                                SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '0'
                                )),0)
   WHERE EXISTS (SELECT 1
            FROM RWA_DEV.RWA_TMP_ABSBOND T1
           WHERE T1.SUBJECTNO = T.SUBJECT_NO
             AND T1.CURRENCY = T.CURRENCY
             AND T1.ORGID = T.ORGID --不汇总时增加了机构号关联条件
             AND T1.SUBJECTNO IN (
                                SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '0'
                                ));

    COMMIT;
  --更新资产证券化所属的利息科目金额
  --汇总
 /* UPDATE RWA_dEV.Rwa_Tmp_Expobalance T
     SET T.EXPOSE_BALANCE = T.EXPOSE_BALANCE +
                            NVL((SELECT SUM(T1.INTEREST)
                                  FROM RWA_DEV.RWA_TMP_ABSBOND T1
                                 INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM T2
                                    ON T2.THIRD_SUBJECT_NO = T1.SUBJECTNO
                                 WHERE T2.INTERESTSUBJECT = T.SUBJECT_NO
                                   AND T1.CURRENCY = T.CURRENCY
                                   AND T1.ORGID = T.ORGID
                                   AND T2.INTERESTSUBJECT IN (
                                  SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '0'
                                  )),
                                0)
   WHERE EXISTS (SELECT 1
            FROM RWA_DEV.RWA_TMP_ABSBOND T1
           INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM T2
              ON T2.THIRD_SUBJECT_NO = T1.SUBJECTNO
           WHERE T2.INTERESTSUBJECT = T.SUBJECT_NO
             AND T1.CURRENCY = T.CURRENCY
             AND T1.ORGID = T.ORGID
             AND T2.INTERESTSUBJECT IN (
                                  SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '0'
                                  ));
    COMMIT;
    --不汇总
     UPDATE RWA_dEV.Rwa_Tmp_Expobalance T
     SET T.EXPOSE_BALANCE = T.EXPOSE_BALANCE +
                            NVL((SELECT SUM(T1.INTEREST)
                                  FROM RWA_DEV.RWA_TMP_ABSBOND T1
                                 INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM T2
                                    ON T2.THIRD_SUBJECT_NO = T1.SUBJECTNO
                                 WHERE T2.INTERESTSUBJECT = T.SUBJECT_NO
                                   AND T1.CURRENCY = T.CURRENCY
                                   AND T2.INTERESTSUBJECT IN (
                                  SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '1'
                                  )),.
                                0)
   WHERE EXISTS (SELECT 1
            FROM RWA_DEV.RWA_TMP_ABSBOND T1
           INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM T2
              ON T2.THIRD_SUBJECT_NO = T1.SUBJECTNO
           WHERE T2.INTERESTSUBJECT = T.SUBJECT_NO
             AND T1.CURRENCY = T.CURRENCY
             AND T2.INTERESTSUBJECT IN (
                                  SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '1'
                                  ));
  COMMIT;
  */

   --获取无形资产扣减项
  SELECT COUNT(1) INTO V_ILDDEBT FROM RWA_dEV.Rwa_Ei_Profitdist T WHERE T.DATADATE = V_DATADATE;

    IF V_ILDDEBT > 0 THEN
      SELECT T.ILDDEBT INTO V_ILDDEBT FROM RWA_dEV.Rwa_Ei_Profitdist T WHERE T.DATADATE = V_DATADATE;
    END IF;
  
  --更新无形资产金额，补回相关扣减项
  UPDATE RWA_DEV.RWA_TMP_EXPOBALANCE T
     SET T.EXPOSE_BALANCE = T.EXPOSE_BALANCE + V_ILDDEBT
   WHERE T.SUBJECT_NO = '17010000'
     AND T.CURRENCY = 'CNY'
     AND T.ORGID = '9998';
  COMMIT;
  --
  INSERT INTO RWA_DEV.RWA_TMP_GLBALANCE02 (
        SUBJECT_NO
        ,ORGID
        ,CURRENCY
        ,IOFLAG
        ,RETAILFLAG
        ,ACCOUNT_BALANCE
  )
  SELECT T3.SUBJECT_NO,
         '9998' AS ORGID --记账机构
        ,T3.CURRENCY --币种
        ,DECODE(REGEXP_INSTR(T3.SUBJECT_NO, '^[123456]'), 1, '01', '02') AS IOFLAG --表内表外标识
        ,RAP.RETAILFLAG --零售标识
        ,SUM(T3.ACCOUNT_BALANCE) AS ACCOUNT_BALANCE
    FROM (SELECT ORGID, CURRENCY, SUBJECT_NO, ACCOUNT_BALANCE
            FROM RWA_DEV.RWA_TMP_GLBALANCE) T3
   INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
      ON T3.SUBJECT_NO = RAP.THIRD_SUBJECT_NO
     AND RAP.ARTICULATETYPE <> '04' --是否勾稽 04:不勾稽
     AND RAP.ISINUSE = '1' --启用状态 1启用 0停用
     AND RAP.ISGATHER = '1' --是否汇总到总行 1:是
   GROUP BY T3.SUBJECT_NO, T3.CURRENCY, RAP.RETAILFLAG
  UNION ALL
 SELECT T3.SUBJECT_NO,
        T3.ORGID --记账机构
       ,T3.CURRENCY --币种
       ,DECODE(REGEXP_INSTR(T3.SUBJECT_NO, '^[123456]'), 1, '01', '02') AS IOFLAG --表内表外标识
       ,RAP.RETAILFLAG --零售标识
       ,T3.ACCOUNT_BALANCE
   FROM (SELECT T2.ORGID, T2.CURRENCY, T2.SUBJECT_NO, T2.ACCOUNT_BALANCE
           FROM RWA_DEV.RWA_TMP_GLBALANCE T2) T3
  INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
     ON T3.SUBJECT_NO = RAP.THIRD_SUBJECT_NO
    AND RAP.ARTICULATETYPE <> '04' --是否勾稽 04:不勾稽
    AND RAP.ISINUSE = '1' --启用状态 1启用 0停用
    AND RAP.ISGATHER = '0' --是否汇总到总行 0:否
  ;

  COMMIT;

    /*插入目标表RWA_DEV.RWA_ARTICULATION_RESULT*/
  INSERT INTO RWA_DEV.RWA_ARTICULATION_RESULT(
                DATADATE                      --数据日期
                ,SUBJECT_NO                   --科目号
                ,ORGID                        --管理机构号
                ,ORGNAME                      --管理机构名称
                ,CURRENCY                     --币种
                ,EXPOSE_BALANCE               --暴露金额
                ,ACCOUNT_BALANCE              --总账金额
                ,MINUS_BALANCE                --差异金额
                ,MINUS_RATE                   --差异率
                ,TOLERANCE                    --容忍度
                ,ISTOLERATE                   --是否容忍 1可容忍 0不可容忍
                ,IOFLAG                       --表内/表外标识 01表内 02表外
                ,RETAILFLAG                    --对公零售标识 0对公 1零售
                ,ORGSORTNO                    --机构排序号
                ,ISNETTING
    )
   SELECT V_DATADATE AS DATADATE --数据日期
         , SUBJECT_NO AS SUBJECT_NO --科目
         , ORGID AS ORGID
         , ORGNAME AS ORGNAME
         , CURRENCY AS CURRENCY --币种
         , EXPOSE_BALANCE AS EXPOSE_BALANCE --暴露金额
         , ACCOUNT_BALANCE AS ACCOUNT_BALANCE --总账余额
         , ABS(ACCOUNT_BALANCE) - ABS(EXPOSE_BALANCE) AS MINUS_BALANCE --差异金额
         , CASE WHEN ACCOUNT_BALANCE = 0 THEN
             CASE WHEN EXPOSE_BALANCE = 0 THEN 0 ELSE 1 END
            ELSE
             CASE WHEN EXPOSE_BALANCE = 0 THEN 1
               ELSE
                ABS(ABS(ACCOUNT_BALANCE) - ABS(EXPOSE_BALANCE)) /
                ABS(ACCOUNT_BALANCE)
             END
          END AS MINUS_RATE --差异率
         , TOLERANCE AS TOLERANCE --容忍度
         , CASE WHEN ACCOUNT_BALANCE = 0 THEN CASE WHEN EXPOSE_BALANCE = 0 THEN  '1' ELSE '0' END
               ELSE CASE WHEN EXPOSE_BALANCE = 0 THEN '0' ELSE
                DECODE(SIGN(ABS(ABS(ACCOUNT_BALANCE) - ABS(EXPOSE_BALANCE)) - TOLERANCE * ABS(ACCOUNT_BALANCE)), -1, '1', 0, '1', '0')
             END
          END AS ISTOLERATE --是否容忍
         , IOFLAG AS IOFLAG --表内/表外标识
         , RETAILFLAG       --对公零售标识
         , SORTNO
         ,'1' AS ISNETTING --轧差标识
     FROM (
           SELECT DISTINCT
                  TEMP02.SUBJECT_NO
                  ,TEMP02.ORGID
                  ,OI.ORGNAME
                  ,OI.SORTNO
                  ,TEMP02.CURRENCY
                  ,TEMP02.IOFLAG
                  ,TEMP02.RETAILFLAG
                  ,NVL(TEMP01.EXPOSE_BALANCE, 0) AS EXPOSE_BALANCE
                  ,NVL(TEMP02.ACCOUNT_BALANCE, 0) AS ACCOUNT_BALANCE --总账金额
                  ,DECODE(TEMP02.IOFLAG, '01', V_INTOLERANCE, V_OUTTOLERANCE) AS TOLERANCE --容忍度
             FROM RWA_DEV.RWA_TMP_GLBALANCE02 TEMP02
             LEFT JOIN RWA_DEV.RWA_TMP_EXPOBALANCE TEMP01
               ON TEMP02.SUBJECT_NO = TEMP01.SUBJECT_NO
              AND TEMP02.ORGID = TEMP01.ORGID
              AND TEMP02.CURRENCY = TEMP01.CURRENCY
             LEFT JOIN RWA.ORG_INFO OI
               ON OI.ORGID = TEMP02.ORGID
          )
    WHERE  SUBJECT_NO not in ('11012002','11012003','11012004') -------20190826  BY WZB  特殊科目 本金利息都是虚拟的
       AND( 
        EXPOSE_BALANCE <> 0
       OR ACCOUNT_BALANCE <> 0
       );
    COMMIT;

    --检查分币种对账对不上的科目，不分币种对账是否能够对上
    MERGE INTO RWA_DEV.RWA_ARTICULATION_RESULT RAR
    USING (SELECT TEMP02.ORGID,
                  TEMP02.SUBJECT_NO,
                   CASE WHEN SUM(TEMP02.ACCOUNT_BALANCE) - SUM(TEMP01.EXPOSE_BALANCE) <> 0 AND SUM(TEMP01.EXPOSE_BALANCE) <> 0
                          THEN '0'
                        WHEN SUM(TEMP02.ACCOUNT_BALANCE) - SUM(TEMP01.EXPOSE_BALANCE) = 0
                          THEN '0'
                        ELSE '1' END AS ISNETTING
             FROM (select subject_no, orgid,
                          sum(t.account_balance) as ACCOUNT_BALANCE
                     from RWA_DEV.Rwa_Tmp_Glbalance02 t
                    group by subject_no, orgid) TEMP02
             LEFT JOIN (select subject_no, orgid,
                              sum(expose_balance) as expose_balance
                         from RWA_DEV.RWA_TMP_EXPOBALANCE
                        group by subject_no, orgid) TEMP01
               ON TEMP02.SUBJECT_NO = TEMP01.SUBJECT_NO
              AND TEMP02.ORGID = TEMP01.ORGID
            WHERE EXISTS (SELECT 1 FROM (
                        /* SELECT ORGID, SUBJECT_NO
                             FROM (SELECT T.ORGID, T.SUBJECT_NO, T.CURRENCY
                                     FROM RWA_DEV.RWA_ARTICULATION_RESULT T
                                    WHERE T.DATADATE = TO_DATE(P_DATA_DT_STR, 'yyyymmdd')
                                      AND T.MINUS_BALANCE <> 0)
                            GROUP BY ORGID, SUBJECT_NO
                           HAVING COUNT(1) > 1 */
                           SELECT T.ORGID, T.SUBJECT_NO
                               FROM RWA_DEV.RWA_ARTICULATION_RESULT T
                              WHERE T.DATADATE =v_datadate --TO_DATE('20170630', 'yyyymmdd')
                                AND T.MINUS_BALANCE <> 0
                           GROUP BY ORGID, SUBJECT_NO
                             HAVING COUNT(SUBJECT_NO) > 1
                           ) TEMP03
                    WHERE TEMP03.ORGID = TEMP02.ORGID
                      AND TEMP03.SUBJECT_NO = TEMP02.SUBJECT_NO)
            GROUP BY TEMP02.ORGID, TEMP02.SUBJECT_NO) RAR2
    ON (RAR.DATADATE = TO_DATE(P_DATA_DT_STR, 'yyyymmdd') AND RAR.SUBJECT_NO = RAR2.SUBJECT_NO AND RAR.ORGID = RAR2.ORGID)
    WHEN MATCHED THEN
      UPDATE SET RAR.ISNETTING = RAR2.ISNETTING;

    COMMIT;

    /*插入目标表RWA_DEV.RWA_EI_CONTRACT*/
   INSERT INTO RWA_DEV.RWA_EI_CONTRACT(
               DataDate                             --数据日期
              ,DataNo                               --数据流水号
              ,ContractID                           --合同ID
              ,SContractID                          --源合同ID
              ,SSysID                               --源系统ID
              ,ClientID                             --参与主体ID
              ,SOrgID                               --源机构ID
              ,SOrgName                             --源机构名称
              ,OrgSortNo                            --所属机构排序号
              ,OrgID                                --所属机构ID
              ,OrgName                              --所属机构名称
              ,IndustryID                           --所属行业代码
              ,IndustryName                         --所属行业名称
              ,BusinessLine                         --业务条线
              ,AssetType                            --资产大类
              ,AssetSubType                         --资产小类
              ,BusinessTypeID                       --业务品种代码
              ,BusinessTypeName                     --业务品种名称
              ,CreditRiskDataType                   --信用风险数据类型
              ,StartDate                            --起始日期
              ,DueDate                              --到期日期
              ,OriginalMaturity                     --原始期限
              ,ResidualM                            --剩余期限
              ,SettlementCurrency                   --结算币种
              ,ContractAmount                       --合同总金额
              ,NotExtractPart                       --合同未提取部分
              ,UncondCancelFlag                     --是否可随时无条件撤销
              ,ABSUAFlag                            --资产证券化基础资产标识
              ,ABSPoolID                            --证券化资产池ID
              ,GroupID                              --分组编号
              ,GUARANTEETYPE                        --主要担保方式
              ,ABSPROPORTION                        --资产证券化比重
    )
    SELECT
          V_DATADATE                                       AS DATADATE
          ,V_DATANO                                        AS DATANO
          ,'GC-' || RAR.SUBJECT_NO || '-' || RAR.ORGID || '-' || RAR.CURRENCY
                                                           AS CONTRACTID
          ,'GC-' || RAR.SUBJECT_NO || '-' || RAR.ORGID || '-' || RAR.CURRENCY
                                                           AS SCONTRACTID
          ,'GC'                                            AS SSYSID
          ,'GC-' || RAR.SUBJECT_NO || '-' || RAR.ORGID || '-' || RAR.CURRENCY
                                                           AS CLIENTID
          ,RAR.ORGID                                       AS SORGID
          ,RAR.ORGNAME                                     AS SORGNAME
          ,OI.SORTNO                                       AS ORGSORTNO            --机构排序号
          ,RAR.ORGID                                       AS ORGID
          ,RAR.ORGNAME                                     AS ORGNAME
          ,'999999'                                        AS INDUSTRYID
          ,'未知'                                          AS INDUSTRYNAME
          ,'0501'                                          AS BUSINESSLINE --总行 0501:总行
          ,''                                              AS ASSETTYPE
          ,''                                              AS ASSETSUBTYPE
          ,'9010101010'                                    AS BUSINESSTYPEID
          ,'虚拟业务品种'                                  AS BUSINESSTYPENAME
          ,DECODE(RAP.RETAILFLAG,1,'02' ,'01' )            AS CREDITRISKDATATYPE
          ,V_STARTDATE                                     AS STARTDATE
          ,TO_CHAR(ADD_MONTHS(V_DATADATE, 6) ,'yyyy-mm-dd')AS DUEDATE
          ,0.5                                             AS ORIGINALMATURITY
          ,0.5                                             AS RESIDUALM
          ,RAR.CURRENCY                                    AS SETTLEMENTCURRENCY
          ,RAR.MINUS_BALANCE                               AS CONTRACTAMOUNT
          ,0                                               AS NOTEXTRACTPART         --合同未提取部分     (默认为0)
          ,'0'                                             AS UNCONDCANCELFLAG       --是否可随时无条件撤销(默认为否，0是 1否)
          ,'0'                                             AS ABSUAFLAG
          ,''                                              AS ABSPOOLID
          ,''                                              AS GROUPID                --分组编号         (源系统ID)
          ,''                                              AS GUARANTEETYPE
          ,1                                               AS ABSPROPORTION
       FROM RWA_DEV.RWA_ARTICULATION_RESULT RAR
      INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
         ON RAR.SUBJECT_NO = RAP.THIRD_SUBJECT_NO
        AND RAP.ISNETTING = '1' --是否轧差 0否 1是
        AND RAP.ISINUSE = '1' --启用状态 1启用 0停用
       LEFT JOIN RWA.ORG_INFO OI
         ON OI.ORGID = RAR.ORGID
      WHERE RAR.DATADATE = V_DATADATE
           --AND RAR.ISTOLERATE = '0'                     --是否容忍 1可容忍 0不可容忍
        AND RAR.MINUS_BALANCE > 0
        AND RAR.ISNETTING = '1'
     ;

    COMMIT;

     /*插入目标表RWA_DEV.RWA_EI_CONTRACT*/
   INSERT INTO RWA_DEV.RWA_EI_CONTRACT(
               DataDate                             --数据日期
              ,DataNo                               --数据流水号
              ,ContractID                           --合同ID
              ,SContractID                          --源合同ID
              ,SSysID                               --源系统ID
              ,ClientID                             --参与主体ID
              ,SOrgID                               --源机构ID
              ,SOrgName                             --源机构名称
              ,OrgSortNo                            --所属机构排序号
              ,OrgID                                --所属机构ID
              ,OrgName                              --所属机构名称
              ,IndustryID                           --所属行业代码
              ,IndustryName                         --所属行业名称
              ,BusinessLine                         --业务条线
              ,AssetType                            --资产大类
              ,AssetSubType                         --资产小类
              ,BusinessTypeID                       --业务品种代码
              ,BusinessTypeName                     --业务品种名称
              ,CreditRiskDataType                   --信用风险数据类型
              ,StartDate                            --起始日期
              ,DueDate                              --到期日期
              ,OriginalMaturity                     --原始期限
              ,ResidualM                            --剩余期限
              ,SettlementCurrency                   --结算币种
              ,ContractAmount                       --合同总金额
              ,NotExtractPart                       --合同未提取部分
              ,UncondCancelFlag                     --是否可随时无条件撤销
              ,ABSUAFlag                            --资产证券化基础资产标识
              ,ABSPoolID                            --证券化资产池ID
              ,GroupID                              --分组编号
              ,GUARANTEETYPE                        --主要担保方式
              ,ABSPROPORTION                        --资产证券化比重
    )
    SELECT
          V_DATADATE                                       AS DATADATE
          ,V_DATANO                                        AS DATANO
          ,'GC-' || B1.SUBJECT_NO || '-' || B1.ORGID || '-' || B1.CURRENCY
                                                           AS CONTRACTID
          ,'GC-' || B1.SUBJECT_NO || '-' || B1.ORGID || '-' || B1.CURRENCY
                                                           AS SCONTRACTID
          ,'GC'                                            AS SSYSID
          ,'GC-'||B1.SUBJECT_NO || '-' || B1.ORGID || '-' || B1.CURRENCY
                                                           AS CLIENTID
          ,OI.ORGID                                        AS SORGID
          ,OI.ORGNAME                                      AS SORGNAME
          ,OI.SORTNO                                       AS ORGSORTNO            --机构排序号
          ,OI.ORGID                                        AS ORGID
          ,OI.ORGNAME                                      AS ORGNAME
          ,'999999'                                        AS INDUSTRYID
          ,'未知'                                           AS INDUSTRYNAME
          ,'0501'                                          AS BUSINESSLINE
          ,''                                              AS ASSETTYPE
          ,''                                              AS ASSETSUBTYPE
          ,'9010101010'                                    AS BUSINESSTYPEID
          ,'虚拟业务品种'                                  AS BUSINESSTYPENAME
          ,DECODE(RAP.RETAILFLAG,1,'02' ,'01' )            AS CREDITRISKDATATYPE
          ,V_STARTDATE                                     AS STARTDATE
          ,TO_CHAR(ADD_MONTHS(V_DATADATE, 6) ,'yyyy-mm-dd')AS DUEDATE
          ,0.5                                             AS ORIGINALMATURITY
          ,0.5                                             AS RESIDUALM
          ,b1.CURRENCY                                     AS SETTLEMENTCURRENCY
          ,b1.MINUS_BALANCE                                AS CONTRACTAMOUNT
          ,0                                               AS NOTEXTRACTPART         --合同未提取部分     (默认为0)
          ,'0'                                             AS UNCONDCANCELFLAG       --是否可随时无条件撤销(默认为否，0是 1否)
          ,'0'                                             AS ABSUAFLAG
          ,''                                              AS ABSPOOLID
          ,''                                              AS GROUPID                --分组编号         (源系统ID)
          ,''                                              AS GUARANTEETYPE
          ,1                                               AS ABSPROPORTION
       FROM (SELECT
               TEMP02.SUBJECT_NO,
               TEMP02.ORGID,
               'CNY' as CURRENCY,
               SUM(TEMP02.ACCOUNT_BALANCE) - SUM(TEMP01.EXPOSE_BALANCE) as MINUS_BALANCE,
               CASE
                 WHEN SUM(TEMP02.ACCOUNT_BALANCE) - SUM(TEMP01.EXPOSE_BALANCE) <> 0 AND SUM(TEMP01.EXPOSE_BALANCE) <> 0
                   THEN '0'
                 WHEN SUM(TEMP02.ACCOUNT_BALANCE) - SUM(TEMP01.EXPOSE_BALANCE) = 0
                   THEN '0'
                 ELSE '1'
               END AS ISNETTING
          FROM (select subject_no,
                       orgid,
                       sum(t.account_balance) as ACCOUNT_BALANCE
                  from RWA_DEV.Rwa_Tmp_Glbalance02 t
                 group by subject_no, orgid) TEMP02
          LEFT JOIN (select subject_no,
                           orgid,
                           sum(expose_balance) as expose_balance
                      from RWA_DEV.RWA_TMP_EXPOBALANCE
                     group by subject_no, orgid) TEMP01
            ON TEMP02.SUBJECT_NO = TEMP01.SUBJECT_NO
           AND TEMP02.ORGID = TEMP01.ORGID
          LEFT JOIN RWA_ARTICULATION_PARAM RAP
            ON RAP.THIRD_SUBJECT_NO = TEMP02.SUBJECT_NO
         WHERE EXISTS (SELECT 1
                  FROM (
                  /*SELECT ORGID, SUBJECT_NO
                          FROM (SELECT T.ORGID, T.SUBJECT_NO, T.CURRENCY
                                  FROM RWA_DEV.RWA_ARTICULATION_RESULT T
                                 WHERE T.DATADATE =
                                       TO_DATE(P_DATA_DT_STR, 'yyyymmdd')
                                   AND T.MINUS_BALANCE <> 0)
                         GROUP BY ORGID, SUBJECT_NO
                        HAVING COUNT(1) > 1 */
                        SELECT T.ORGID, T.SUBJECT_NO
                               FROM RWA_DEV.RWA_ARTICULATION_RESULT T
                              WHERE T.DATADATE = TO_DATE('20170630', 'yyyymmdd')
                                AND T.MINUS_BALANCE <> 0
                           GROUP BY ORGID, SUBJECT_NO
                             HAVING COUNT(SUBJECT_NO) > 1 ) TEMP03
                 WHERE TEMP03.ORGID = TEMP02.ORGID
                   AND TEMP03.SUBJECT_NO = TEMP02.SUBJECT_NO)
         GROUP BY TEMP02.ORGID, TEMP02.SUBJECT_NO) B1
  LEFT JOIN RWA_DEV.Rwa_Articulation_Param RAP
    ON RAP.THIRD_SUBJECT_NO = B1.SUBJECT_NO
  left join rwa.org_info oi
   on oi.orgid = b1.orgid
 where B1.ISNETTING = 0
   and B1.minus_balance > 0
 ;
    COMMIT;

  /*插入目标表RWA_DEV.RWA_EI_EXPOSURE*/
    INSERT INTO RWA_DEV.RWA_EI_EXPOSURE(
                 DataDate                                                     --数据日期
                ,DataNo                                                       --数据流水号
                ,ExposureID                                                   --风险暴露ID
                ,DueID                                                        --债项ID
                ,SSysID                                                       --源系统ID
                ,ContractID                                                   --合同ID
                ,ClientID                                                     --参与主体ID
                ,SOrgID                                                       --源机构ID
                ,SOrgName                                                     --源机构名称
                ,OrgSortNo                                                    --所属机构排序号
                ,OrgID                                                        --所属机构ID
                ,OrgName                                                      --所属机构名称
                ,AccOrgID                                                     --账务机构ID
                ,AccOrgName                                                   --账务机构名称
                ,IndustryID                                                   --所属行业代码
                ,IndustryName                                                 --所属行业名称
                ,BusinessLine                                                 --业务条线
                ,AssetType                                                    --资产大类
                ,AssetSubType                                                 --资产小类
                ,BusinessTypeID                                               --业务品种代码
                ,BusinessTypeName                                             --业务品种名称
                ,CreditRiskDataType                                           --信用风险数据类型
                ,AssetTypeOfHaircuts                                          --折扣系数对应资产类别
                ,BusinessTypeSTD                                              --权重法业务类型
                ,ExpoClassSTD                                                 --权重法暴露大类
                ,ExpoSubClassSTD                                              --权重法暴露小类
                ,ExpoClassIRB                                                 --内评法暴露大类
                ,ExpoSubClassIRB                                              --内评法暴露小类
                ,ExpoBelong                                                   --暴露所属标识
                ,BookType                                                     --账户类别
                ,ReguTranType                                                 --监管交易类型
                ,RepoTranFlag                                                 --回购交易标识
                ,RevaFrequency                                                --重估频率
                ,Currency                                                     --币种
                ,NormalPrincipal                                              --正常本金余额
                ,OverdueBalance                                               --逾期余额
                ,NonAccrualBalance                                            --非应计余额
                ,OnSheetBalance                                               --表内余额
                ,NormalInterest                                               --正常利息
                ,OnDebitInterest                                              --表内欠息
                ,OffDebitInterest                                             --表外欠息
                ,ExpenseReceivable                                            --应收费用
                ,AssetBalance                                                 --资产余额
                ,AccSubject1                                                  --科目一
                ,AccSubject2                                                  --科目二
                ,AccSubject3                                                  --科目三
                ,StartDate                                                    --起始日期
                ,DueDate                                                      --到期日期
                ,OriginalMaturity                                             --原始期限
                ,ResidualM                                                    --剩余期限
                ,RiskClassify                                                 --风险分类
                ,ExposureStatus                                               --风险暴露状态
                ,OverdueDays                                                  --逾期天数
                ,SpecialProvision                                             --专项准备金
                ,GeneralProvision                                             --一般准备金
                ,EspecialProvision                                            --特别准备金
                ,WrittenOffAmount                                             --已核销金额
                ,OffExpoSource                                                --表外暴露来源
                ,OffBusinessType                                              --表外业务类型
                ,OffBusinessSdvsSTD                                           --权重法表外业务类型细分
                ,UncondCancelFlag                                             --是否可随时无条件撤销
                ,CCFLevel                                                     --信用转换系数级别
                ,CCFAIRB                                                      --高级法信用转换系数
                ,ClaimsLevel                                                  --债权级别
                ,BondFlag                                                     --是否为债券
                ,BondIssueIntent                                              --债券发行目的
                ,NSURealPropertyFlag                                          --是否非自用不动产
                ,RepAssetTermType                                             --抵债资产期限类型
                ,DependOnFPOBFlag                                             --是否依赖于银行未来盈利
                ,IRating                                                      --内部评级
                ,PD                                                           --违约概率
                ,LGDLevel                                                     --违约损失率级别
                ,LGDAIRB                                                      --高级法违约损失率
                ,MAIRB                                                        --高级法有效期限
                ,EADAIRB                                                      --高级法违约风险暴露
                ,DefaultFlag                                                  --违约标识
                ,BEEL                                                         --已违约暴露预期损失比率
                ,DefaultLGD                                                   --已违约暴露违约损失率
                ,EquityExpoFlag                                               --股权暴露标识
                ,EquityInvestType                                             --股权投资对象类型
                ,EquityInvestCause                                            --股权投资形成原因
                ,SLFlag                                                       --专业贷款标识
                ,SLType                                                       --专业贷款类型
                ,PFPhase                                                      --项目融资阶段
                ,ReguRating                                                   --监管评级
                ,CBRCMPRatingFlag                                             --银监会认定评级是否更为审慎
                ,LargeFlucFlag                                                --是否波动性较大
                ,LiquExpoFlag                                                 --是否清算过程中风险暴露
                ,PaymentDealFlag                                              --是否货款对付模式
                ,DelayTradingDays                                             --延迟交易天数
                ,SecuritiesFlag                                               --有价证券标识
                ,SecuIssuerID                                                 --证券发行人ID
                ,RatingDurationType                                           --评级期限类型
                ,SecuIssueRating                                              --证券发行等级
                ,SecuResidualM                                                --证券剩余期限
                ,SecuRevaFrequency                                            --证券重估频率
                ,CCPTranFlag                                                  --是否中央交易对手相关交易
                ,CCPID                                                        --中央交易对手ID
                ,QualCCPFlag                                                  --是否合格中央交易对手
                ,BankRole                                                     --银行角色
                ,ClearingMethod                                               --清算方式
                ,BankAssetFlag                                                --是否银行提交资产
                ,MatchConditions                                              --符合条件情况
                ,SFTFlag                                                      --证券融资交易标识
                ,MasterNetAgreeFlag                                           --净额结算主协议标识
                ,MasterNetAgreeID                                             --净额结算主协议ID
                ,SFTType                                                      --证券融资交易类型
                ,SecuOwnerTransFlag                                           --证券所有权是否转移
                ,OTCFlag                                                      --场外衍生工具标识
                ,ValidNettingFlag                                             --有效净额结算协议标识
                ,ValidNetAgreementID                                          --有效净额结算协议ID
                ,OTCType                                                      --场外衍生工具类型
                ,DepositRiskPeriod                                            --保证金风险期间
                ,MTM                                                          --重置成本
                ,MTMCurrency                                                  --重置成本币种
                ,BuyerOrSeller                                                --买方卖方
                ,QualROFlag                                                   --合格参照资产标识
                ,ROIssuerPerformFlag                                          --参照资产发行人是否能履约
                ,BuyerInsolvencyFlag                                          --信用保护买方是否破产
                ,NonpaymentFees                                               --尚未支付费用
                ,RetailExpoFlag                                               --零售暴露标识
                ,RetailClaimType                                              --零售债权类型
                ,MortgageType                                                 --住房抵押贷款类型
                ,ExpoNumber                                                   --风险暴露个数
                ,LTV                                                          --贷款价值比
                ,Aging                                                        --账龄
                ,NewDefaultDebtFlag                                           --新增违约债项标识
                ,PDPoolModelID                                                --PD分池模型ID
                ,LGDPoolModelID                                               --LGD分池模型ID
                ,CCFPoolModelID                                               --CCF分池模型ID
                ,PDPoolID                                                     --所属PD池ID
                ,LGDPoolID                                                    --所属LGD池ID
                ,CCFPoolID                                                    --所属CCF池ID
                ,ABSUAFlag                                                    --资产证券化基础资产标识
                ,ABSPoolID                                                    --证券化资产池ID
                ,GroupID                                                      --分组编号
                ,DefaultDate                                                  --违约时点
                ,ABSPROPORTION                                                --资产证券化比重
                ,DEBTORNUMBER                                                 --借款人个数
    )
    SELECT
                V_DATADATE                                      AS DATADATE                     --数据日期
                ,DATANO                                         AS DATANO                       --数据流水号
                ,REC.CONTRACTID                                 AS EXPOSUREID                   --风险暴露ID
                ,REC.CONTRACTID                                 AS DUEID                        --债项ID
                ,REC.SSYSID                                     AS SSYSID                       --源系统ID
                ,REC.CONTRACTID                                 AS CONTRACTID                   --合同ID
                ,REC.CLIENTID                                   AS CLIENTID                     --参与主体ID
                ,REC.ORGID                                    AS SORGID                       --源机构ID
                ,REC.ORGNAME                                    AS SORGNAME                     --源机构名称
                ,REC.ORGSORTNO                                 AS ORGSORTNO                    --机构排序号
                ,REC.ORGID                                  AS ORGID                        --所属机构ID
                ,REC.ORGNAME                                    AS ORGNAME                      --所属机构名称
                ,REC.ORGID                                     AS ACCORGID                     --账务机构ID
                ,REC.ORGNAME                                AS ACCORGNAME                   --账务机构名称
                ,REC.INDUSTRYID                                 AS INDUSTRYID                   --所属行业代码
                ,REC.INDUSTRYNAME                               AS INDUSTRYNAME                 --所属行业名称
                ,REC.BUSINESSLINE                               AS BUSSINESSLINE                --条线
                ,REC.ASSETTYPE                                  AS ASSETTYPE                    --资产大类
                ,REC.ASSETSUBTYPE                               AS ASSETSUBTYPE                 --资产小类
                ,'9010101010'                                   AS BUSINESSTYPEID               --业务品种代码
                ,'虚拟业务品种'                                 AS BUSINESSTYPENAME             --业务品种名称
                ,DECODE(RAP.RETAILFLAG,1,'02','01')             AS CREDITRISKDATATYPE           --信用风险数据类型(01:一般非零售,02:一般零售)
                ,'01'                                           AS ASSETTYPEOFHAIRCUTS          --折扣系数对应资产类别
                ,DECODE(RAP.RETAILFLAG,1 ,'06' ,'07')           AS BUSINESSTYPESTD              --权重法业务类型(对公 一般资产07 零售 个人06)
                ,''                                             AS EXPOCLASSSTD                 --权重法暴露大类(对公 0106 零售 0108)
                ,''                                             AS EXPOSUBCLASSSTD              --权重法暴露小类(对公 010601 零售 010803)
                ,''                                             AS EXPOCLASSIRB                 --内评法暴露大类(对公 0203 零售 0204)
                ,''                                             AS EXPOSUBCLASSIRB              --内评法暴露小类(对公 020301 零售 020403)
                ,DECODE(
                      REGEXP_INSTR(RAP.THIRD_SUBJECT_NO ,'^[123456]')
                      ,1
                      ,'01'
                      ,'02'
                  )                                             AS EXPOBELONG                   --暴露所属标识((01:表内;02:一般表外;03:交易对手;))
                ,'01'                                           AS BOOKTYPE                     --账户类别(固定值"银行账户",01:银行账户,02:交易账户)
                ,'03'                                           AS REGUTRANTYPE                 --监管交易类型(固定值"抵押贷款",01:回购交易;02:其他资本市场交易;03:抵押贷款;)
                ,'0'                                            AS REPOTRANFLAG                 --回购交易标识(固定值为"否" 0)
                ,1                                              AS REVAFREQUENCY                --重估频率
                ,REC.SETTLEMENTCURRENCY                         AS CURRENCY                     --币种
                ,REC.CONTRACTAMOUNT                             AS NORMALPRINCIPAL              --正常本金余额
                ,0                                              AS OVERDUEBALANCE               --逾期余额
                ,0                                              AS NONACCRUALBALANCE            --非应计余额
                ,REC.CONTRACTAMOUNT                             AS ONSHEETBALANCE               --表内余额(正常本金余额+逾期余额+非应计余额)
                ,0                                              AS NORMALINTEREST               --正常利息
                ,0                                              AS ONDEBITINTEREST              --表内欠息
                ,0                                              AS OFFDEBITINTEREST             --表外欠息
                ,0                                              AS EXPENSERECEIVABLE            --应收费用
                ,REC.CONTRACTAMOUNT                             AS ASSETBALANCE                 --资产余额
                ,RAP.THIRD_SUBJECT_NO                           AS ACCSUBJECT1                  --科目一
                ,NULL                                           AS ACCSUBJECT2                  --科目二
                ,NULL                                           AS ACCSUBJECT3                  --科目三
                ,REC.STARTDATE                                  AS STARTDATE                    --起始日期
                ,REC.DUEDATE                                    AS DUEDATE                      --到期日期
                ,REC.ORIGINALMATURITY                           AS ORIGINALMATURITY             --原始期限
                ,REC.RESIDUALM                                  AS RESIDUALM                    --剩余期限
                ,'01'                                           AS RISKCLASSIFY                 --风险分类(01正常,02关注,03次级,04可疑,05损失)
                ,'01'                                           AS EXPOSURESTATUS               --风险暴露状态(01代表正常，02代表逾期)
                ,0                                              AS OVERDUEDAYS                  --逾期天数
                ,0                                              AS SPECIALPROVISION             --专项准备金
                ,0                                              AS GENERALPROVISION             --一般准备金
                ,0                                              AS ESPECIALPROVISION            --特别准备金
                ,0                                              AS WRITTENOFFAMOUNT             --已核销金额
                ,DECODE(
                        REGEXP_INSTR(RAP.THIRD_SUBJECT_NO ,'^[123456]')
                        ,1
                        ,''
                        ,'03'
                       )                                        AS OFFEXPOSOURCE                --表外暴露来源
                ,''                                             AS OFFBUSINESSTYPE              --表外业务类型
                ,''                                             AS OFFBUSINESSSDVSSTD           --权重法表外业务类型细分
                ,'0'                                            AS UNCONDCANCELFLAG             --是否可随时无条件撤销
                ,''                                             AS CCFLEVEL                     --信用转换系数级别
                ,''                                             AS CCFAIRB                      --高级法信用转换系数
                ,'01'                                           AS CLAIMSLEVEL                  --债权级别(01:高级债权,02:次级债权)
                ,'0'                                            AS BONDFLAG                     --是否为债券
                ,''                                             AS BONDISSUEINTENT              --债券发行目的
                ,'0'                                            AS NSUREALPROPERTYFLAG          --是否非自用不动产
                ,''                                             AS REPASSETTERMTYPE             --抵债资产期限类型
                ,'0'                                            AS DEPENDONFPOBFLAG             --是否依赖于银行未来盈利
                ,NULL                                           AS IRATING                      --内部评级
                ,NULL                                           AS PD                           --违约概率
                ,''                                             AS LGDLEVEL                     --违约损失率级别
                ,0                                              AS LGDAIRB                      --高级法违约损失率
                ,NULL                                           AS MAIRB                        --高级法有效期限
                ,0                                              AS EADAIRB                      --高级法违约风险暴露
                ,'0'                                            AS DEFAULTFLAG                  --违约标识
                ,0.45                                           AS BEEL                         --已违约暴露预期损失比率
                ,0.45                                           AS DEFAULTLGD                   --已违约暴露违约损失率
                ,'0'                                            AS EQUITYEXPOFLAG               --股权暴露标识
                ,''                                             AS EQUITYINVESTTYPE             --股权投资对象类型
                ,''                                             AS EQUITYINVESTCAUSE            --股权投资形成原因
                ,'0'                                            AS SLFLAG                       --专业贷款标识
                ,''                                             AS SLTYPE                       --专业贷款类型
                ,''                                             AS PFPHASE                      --项目融资阶段
                ,''                                             AS REGURATING                   --监管评级
                ,''                                             AS CBRCMPRATINGFLAG             --银监会认定评级是否更为审慎
                ,'0'                                            AS LARGEFLUCFLAG                --是否波动性较大
                ,'0'                                            AS LIQUEXPOFLAG                 --是否清算过程中风险暴露
                ,'0'                                            AS PAYMENTDEALFLAG              --是否货款对付模式
                ,0                                              AS DELAYTRADINGDAYS             --延迟交易天数
                ,'0'                                            AS SECURITIESFLAG               --有价证券标识
                ,''                                             AS SECUISSUERID                 --证券发行人ID
                ,''                                             AS RATINGDURATIONTYPE           --评级期限类型
                ,''                                             AS SECUISSUERATING              --证券发行等级
                ,0                                              AS SECURESIDUALM                --证券剩余期限
                ,1                                              AS SECUREVAFREQUENCY            --证券重估频率
                ,'0'                                            AS CCPTRANFLAG                  --是否中央交易对手相关交易
                ,''                                             AS CCPID                        --中央交易对手ID
                ,'0'                                            AS QUALCCPFLAG                  --是否合格中央交易对手
                ,''                                             AS BANKROLE                     --银行角色
                ,''                                             AS CLEARINGMETHOD               --清算方式
                ,''                                             AS BANKASSETFLAG                --是否银行提交资产
                ,''                                             AS MATCHCONDITIONS              --符合条件情况
                ,'0'                                            AS SFTFLAG                      --证券融资交易标识
                ,'0'                                            AS MASTERNETAGREEFLAG           --净额结算主协议标识
                ,''                                             AS MASTERNETAGREEID             --净额结算主协议ID
                ,''                                             AS SFTTYPE                      --证券融资交易类型
                ,'0'                                            AS SECUOWNERTRANSFLAG           --证券所有权是否转移
                ,'0'                                            AS OTCFLAG                      --场外衍生工具标识
                ,'0'                                            AS VALIDNETTINGFLAG             --有效净额结算协议标识
                ,''                                             AS VALIDNETAGREEMENTID          --有效净额结算协议ID
                ,''                                             AS OTCTYPE                      --场外衍生工具类型
                ,0                                              AS DEPOSITRISKPERIOD            --保证金风险期间
                ,0                                              AS MTM                          --重置成本
                ,''                                             AS MTMCURRENCY                  --重置成本币种
                ,''                                             AS BUYERORSELLER                --买方卖方
                ,'0'                                            AS QUALROFLAG                   --合格参照资产标识
                ,'0'                                            AS ROISSUERPERFORMFLAG          --参照资产发行人是否能履约
                ,''                                             AS BUYERINSOLVENCYFLAG          --信用保护买方是否破产
                ,0                                              AS NONPAYMENTFEES               --尚未支付费用
                ,DECODE(RAP.RETAILFLAG,1,'1','0')               AS RETAILEXPOFLAG               --零售暴露标识
                ,DECODE(RAP.RETAILFLAG,1,'020403','')           AS RETAILCLAIMTYPE              --零售债权类型
                ,''                                             AS MORTGAGETYPE                 --住房抵押贷款类型
                ,1                                              AS ExpoNumber                   --风险暴露个数                默认 1
                ,0.8                                            AS LTV                          --贷款价值比                 默认 0.8
                ,NULL                                           AS Aging                        --账龄                        默认 NULL
                ,''                                             AS NewDefaultDebtFlag           --新增违约债项标识            默认 NULL
                ,''                                             AS PDPoolModelID                --PD分池模型ID                默认 NULL
                ,''                                             AS LGDPoolModelID               --LGD分池模型ID               默认 NULL
                ,''                                             AS CCFPoolModelID               --CCF分池模型ID               默认 NULL
                ,''                                             AS PDPoolID                     --所属PD池ID                 默认 NULL
                ,''                                             AS LGDPoolID                    --所属LGD池ID                默认 NULL
                ,''                                             AS CCFPoolID                    --所属CCF池ID                默认 NULL
                ,'0'                                            AS ABSUAFlag                    --资产证券化基础资产标识     默认 否(0)
                ,''                                             AS ABSPoolID                    --证券化资产池ID              默认 NULL
                ,''                                             AS GroupID                      --分组编号                    默认 NULL
                ,NULL                                           AS DefaultDate                  --违约时点
                ,NULL                                           AS ABSPROPORTION                --资产证券化比重
                ,NULL                                           AS DEBTORNUMBER                 --借款人个数

    FROM         RWA_DEV.RWA_EI_CONTRACT REC
    INNER JOIN   RWA_DEV.RWA_ARTICULATION_PARAM RAP
    ON           SUBSTR(REC.CONTRACTID,4,8) = RAP.THIRD_SUBJECT_NO
    AND          RAP.ISNETTING = '1'                      --是否轧差 0否 1是
    AND          RAP.ISINUSE = '1'                        --启用状态 1启用 0停用
    WHERE        REC.DATADATE = V_DATADATE
    AND          REC.SSYSID = 'GC'
    ;

    COMMIT;

    /*插入目标表RWA_DEV.RWA_EI_CLIENT*/
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DataDate                   --数据日期
                ,DataNo                     --数据流水号
                ,ClientID                   --参与主体ID
                ,SourceClientID             --源参与主体ID
                ,SSysID                     --源系统ID
                ,ClientName                 --参与主体名称
                ,SOrgID                     --源机构ID
                ,SOrgName                   --源机构名称
                ,OrgSortNo                  --所属机构排序号
                ,OrgID                      --所属机构ID
                ,OrgName                    --所属机构名称
                ,IndustryID                 --所属行业代码
                ,IndustryName               --所属行业名称
                ,ClientType                 --参与主体大类
                ,ClientSubType              --参与主体小类
                ,RegistState                --注册国家或地区
                ,RCERating                  --境外注册地外部评级
                ,RCERAgency                 --境外注册地外部评级机构
                ,OrganizationCode           --组织机构代码
                ,ConsolidatedSCFlag         --是否并表子公司
                ,SLClientFlag               --专业贷款客户标识
                ,SLClientType               --专业贷款客户类型
                ,ExpoCategoryIRB            --内评法暴露类别
                ,ModelID                    --模型ID
                ,ModelIRating               --模型内部评级
                ,ModelPD                    --模型违约概率
                ,IRating                    --内部评级
                ,PD                         --违约概率
                ,DefaultFlag                --违约标识
                ,NewDefaultFlag             --新增违约标识
                ,DefaultDate                --违约时点
                ,ClientERating              --参与主体外部评级
                ,CCPFlag                    --中央交易对手标识
                ,QualCCPFlag                --是否合格中央交易对手
                ,ClearMemberFlag            --清算会员标识
                ,CompanySize                --企业规模
                ,SSMBFlag                   --标准小微企业标识
                ,AnnualSale                 --公司客户年销售额
                ,CountryCode                --注册国家代码
                ,MSMBFlag                   --工信部微小企业标识
    )
    SELECT
                V_DATADATE                AS DATADATE             --数据日期
                ,V_DATANO                 AS DATANO               --数据流水号
                ,REE.CLIENTID             AS CLIENTID             --参与主体代号
                ,REE.CLIENTID             AS SOURCECLIENTID       --源参与主体代号
                ,REE.SSYSID               AS SSYSID               --源系统代号
                ,'总账虚拟客户'           AS CLIENTNAME           --参与主体名称
                ,REE.ORGID                AS SORGID               --源机构代码
                ,REE.ORGNAME              AS SORGNAME             --源机构名称
                ,REE.ORGSORTNO            AS ORGSORTNO            --机构排序号
                ,REE.ORGID                AS ORGID                --所属机构代码
                ,REE.ORGNAME              AS ORGNAME              --所属机构名称
                ,REE.INDUSTRYID           AS INDUSTRYID           --所属行业代码
                ,REE.INDUSTRYNAME         AS INDUSTRYNAME         --所属行业名称
                ,DECODE(RAP.RETAILFLAG,1,'04','03')
                                          AS CLIENTTYPE           --参与主体大类
                ,DECODE(RAP.RETAILFLAG,1,'0401','0301')
                                          AS CLIENTSUBTYPE        --参与主体小类
                ,'01'                     AS REGISTSTATE          --注册国家或地区
                ,NULL                     AS RCERATING            --境外注册地外部评级
                ,NULL                     AS RCERAGENCY           --境外注册地外部评级机构
                ,NULL                     AS ORGANIZATIONCODE     --组织机构代码
                ,'0'                      AS CONSOLIDATEDSCFLAG   --是否并表子公司
                ,'0'                      AS SLCLIENTFLAG         --专业贷款客户标识
                ,NULL                     AS SLCLIENTTYPE         --专业贷款客户类型
                ,'020701'                 AS EXPOCATEGORYIRB      --内评法暴露类别
                ,NULL                     AS ModelID              --模型ID
                ,NULL                     AS ModelIRating         --模型内部评级
                ,NULL                     AS ModelPD              --模型违约概率
                ,NULL                     AS IRating              --内部评级
                ,NULL                     AS PD                   --违约概率
                ,'0'                      AS DefaultFlag          --违约标识
                ,'0'                      AS NewDefaultFlag       --新增违约标识
                ,NULL                     AS DefaultDate          --违约时点
                ,''                       AS CLIENTERATING        --参与主体外部评级
                ,'0'                      AS CCPFLAG              --中央交易对手标识
                ,'0'                      AS QUALCCPFLAG          --是否合格中央交易对手
                ,'0'                      AS CLEARMEMBERFLAG      --清算会员标识
                ,'01'                     AS CompanySize          --企业规模
                ,'0'                      AS SSMBFLAG             --标准小微企业标识
                ,null                     AS ANNUALSALE           --公司客户年销售额
                ,'CHN'                    AS COUNTRYCODE          --注册国家代码
                ,'0'                      AS MSMBFLAG             --工信部微小企业标识
    FROM  RWA_DEV.RWA_EI_EXPOSURE REE
    LEFT JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
    ON    RAP.THIRD_SUBJECT_NO = REE.ACCSUBJECT1
    WHERE DATADATE = V_DATADATE
    AND   SSYSID = 'GC'
    ;
    COMMIT;




--1  应收利息减值准备收工更新到一个固定的科目上，科目只要是1132开头的科目, 且日常科目余额要大于减值金额
  UPDATE RWA_DEV.RWA_EI_EXPOSURE T1 
         SET T1.GENERALPROVISION=(SELECT SUM(T.BALANCE_C-T.BALANCE_D) AS PROVISION FROM RWA_DEV.FNS_GL_BALANCE T
                                  WHERE T.DATANO=p_data_dt_str
                                  AND T.SUBJECT_NO='12310100'
                                  AND T.CURRENCY_CODE<>'RMB' )
  WHERE T1.DATANO=p_data_dt_str AND T1.ACCSUBJECT1='11320901';
  COMMIT;
  
  --1  其他坏账减值准备收工更新到一个固定的科目上，科目只要是1132开头的科目都可，, 且日常科目余额要大于减值金额
  UPDATE RWA_DEV.RWA_EI_EXPOSURE T1 
         SET T1.GENERALPROVISION=(SELECT SUM(T.BALANCE_C-T.BALANCE_D) AS PROVISION FROM RWA_DEV.FNS_GL_BALANCE T
                                  WHERE T.DATANO=p_data_dt_str
                                  AND T.SUBJECT_NO='12312000'
                                  AND T.CURRENCY_CODE<>'RMB' )
  WHERE T1.DATANO=p_data_dt_str AND T1.ACCSUBJECT1='11320601' AND T1.CURRENCY='CNY';
  COMMIT;

--2  勾稽减值科目,并插入到勾稽结果表
--插入结果到临时表，插入之前先清空表
EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.TEMP_GENERALPROVISION';
COMMIT;

--2.1插入减值准备明细到临时表
INSERT INTO RWA_DEV.TEMP_GENERALPROVISION(
               EXPOSUREID  
              ,ACCSUBJECTNO
              ,NORMALBAL
              ,PROVISIONSUBJECTNO
              ,PROVISIONSUBJECTNAME
              ,GENERALPROVISIONBAL
  )
  SELECT
               T1.EXPOSUREID
               ,T1.ACCSUBJECT1
               ,T1.NORMALPRINCIPAL
               ,CASE WHEN T1.ACCSUBJECT1='11320901' THEN '12310100'      --应收利息减值准备
                     WHEN T1.ACCSUBJECT1='11320601' THEN '12312000'      --其他坏账减值准备
                    WHEN T1.ACCSUBJECT1 LIKE '1011%' THEN '12310300'       --存放同业减值准备
                    WHEN T1.ACCSUBJECT1 LIKE '130201%' THEN '12310501'     --拆放同业减值准备
                    WHEN T1.ACCSUBJECT1 LIKE '1301%' THEN '40030102'       --贴现资产减值准备
                    WHEN T1.ACCSUBJECT1 LIKE '1111%' THEN '12310200'       --买入贩售减值准备
                    WHEN T1.ACCSUBJECT1 LIKE '150101%' THEN '15020100'     --债券投资减值准备
                    --WHEN T1.ACCSUBJECT1 LIKE '150301%' THEN '40030101'     --其他综合收益减值准备   1503科目不计量减值
                    WHEN T1.ACCSUBJECT1 LIKE '122201%' THEN '12310400'     --应收款减值准备
                    WHEN T1.ACCSUBJECT1 LIKE '14410100%' THEN '14420000'   --抵债资产减值准备
                    WHEN (T1.ACCSUBJECT1 LIKE '7001%' OR T1.ACCSUBJECT1 LIKE '7002%' OR T1.ACCSUBJECT1 LIKE '7018%' OR T1.ACCSUBJECT1 LIKE '7119%')
                      THEN '28010101'       --表外业务减值
                    WHEN (T1.ACCSUBJECT1 LIKE '1303%' OR T1.ACCSUBJECT1 LIKE '1305%' OR T1.ACCSUBJECT1 LIKE '1307%' OR T1.ACCSUBJECT1 LIKE '1310%')
                      THEN '13040100'       --贷款类科目减值准备
                    ELSE ''
                END AS PROVISIONSUBJECTNO
                ,CASE WHEN T1.ACCSUBJECT1='11320901' THEN '应收利息减值准备'     
                    WHEN T1.ACCSUBJECT1='11320601' THEN '其他坏账减值准备'      
                    WHEN T1.ACCSUBJECT1 LIKE '1011%' THEN '存放同业减值准备'       
                    WHEN T1.ACCSUBJECT1 LIKE '130201%' THEN '拆放同业减值准备'    
                    WHEN T1.ACCSUBJECT1 LIKE '1301%' THEN '贴现资产减值准备'       
                    WHEN T1.ACCSUBJECT1 LIKE '1111%' THEN '买入贩售减值准备'       
                    WHEN T1.ACCSUBJECT1 LIKE '150101%' THEN '债券投资减值准备'     
                    --WHEN T1.ACCSUBJECT1 LIKE '150301%' THEN '其他综合收益减值准备'     --   1503科目不计量减值
                    WHEN T1.ACCSUBJECT1 LIKE '122201%' THEN '应收款投资减值准备'     --
                    WHEN T1.ACCSUBJECT1 LIKE '14410100%' THEN '抵债资产减值准备'   --
                    WHEN (T1.ACCSUBJECT1 LIKE '7001%' OR T1.ACCSUBJECT1 LIKE '7002%' OR T1.ACCSUBJECT1 LIKE '7018%' OR T1.ACCSUBJECT1 LIKE '7119%')
                      THEN '表外业务减值'       --
                    WHEN (T1.ACCSUBJECT1 LIKE '1303%' OR T1.ACCSUBJECT1 LIKE '1305%' OR T1.ACCSUBJECT1 LIKE '1307%' OR T1.ACCSUBJECT1 LIKE '1310%')
                      THEN '贷款类科目减值准备'       --
                    ELSE ''
                END AS PROVISIONSUBJECTNAME
                ,NVL(T1.GENERALPROVISION,0)
  FROM RWA_DEV.RWA_EI_EXPOSURE T1
  WHERE T1.DATANO=p_data_dt_str 
  AND T1.GENERALPROVISION>0
  UNION      --资产证券化明细也有减值，需要汇总
  SELECT 
        T2.ABSEXPOSUREID
        ,'12220101'
        ,T2.ASSETBALANCE
        ,'12310400'
        ,'应收款投资减值准备'
        ,T2.PROVISIONS
  FROM RWA_DEV.RWA_EI_ABSEXPOSURE T2
  WHERE T2.DATANO=p_data_dt_str
  AND T2.PROVISIONS>0;
  COMMIT;
  
  --2.2插入汇总后的结果到明细表,插入之前先删除
  DELETE FROM RWA_DEV.RWA_GENERALPROVISION_RESULT WHERE DATANO = p_data_dt_str;
  COMMIT;
  
  --插入汇总后的结果到明细表
  INSERT INTO RWA_DEV.RWA_GENERALPROVISION_RESULT
  (
     datano       
     ,subject_no  
     ,subject_name 
     ,currency    
     ,total_bal   
     ,detail_bal   
      ,diff_bal     
     ,percent_bal  
  )
  SELECT p_data_dt_str
         ,T1.PROVISIONSUBJECTNO
         ,T1.PROVISIONSUBJECTNAME
         ,'CNY'
         ,T2.BALANCE
         ,T1.GENERALPROVISIONBAL
         ,T2.BALANCE-T1.GENERALPROVISIONBAL
         ,(T2.BALANCE-T1.GENERALPROVISIONBAL)/T2.BALANCE*100
  FROM (SELECT PROVISIONSUBJECTNO
              ,PROVISIONSUBJECTNAME
              ,SUM(GENERALPROVISIONBAL)  AS GENERALPROVISIONBAL
       FROM RWA_DEV.TEMP_GENERALPROVISION 
       WHERE PROVISIONSUBJECTNO IS NOT NULL
       GROUP BY PROVISIONSUBJECTNO
               ,PROVISIONSUBJECTNAME
       ) T1
  INNER JOIN (SELECT 
               CASE WHEN T.SUBJECT_NO IN ('13040101','13040102','13040103') THEN '13040100' ELSE T.SUBJECT_NO END AS SUBJECTNO
              ,SUM(T.BALANCE_C-T.BALANCE_D) AS BALANCE
              FROM RWA_DEV.FNS_GL_BALANCE T
              WHERE T.DATANO=p_data_dt_str
              AND T.CURRENCY_CODE<>'RMB'
              GROUP BY CASE WHEN T.SUBJECT_NO IN ('13040101','13040102','13040103') THEN '13040100' ELSE T.SUBJECT_NO END
  ) T2
  ON T1.PROVISIONSUBJECTNO=T2.SUBJECTNO;
  COMMIT;

--3  减值差异分摊   by wzb 20191127 

------3.1  表外信用卡减值差异分摊
select SUM(A.GENERALPROVISION)into V_BWXTZBJ from rwa_ei_exposure a   ----表外从I9获取的所有减值
where accsubject1 in('70010100','70020000','70180001','70180002') 
and datano=p_data_dt_str;

 select SUM(BALANCE_C*NVL(B.MIDDLEPRICE/100, 1)-BALANCE_D*NVL(B.MIDDLEPRICE/100, 1)) INTO V_BWZZZBJ  ---表外总账减值
 from fns_gl_balance a
 LEFT JOIN TMP_CURRENCY_CHANGE B
 ON A.currency_code=B.CURRENCYCODE
 AND B.DATANO=p_data_dt_str
 where subject_no ='28010101'
        AND A.CURRENCY_CODE<>'RMB'
        AND A.DATANO=p_data_dt_str;
        
--获取表外循环贷款未使用额度        
SELECT SUM(FINAL_ECL) INTO V_BWXHZBJ FROM SYS_IFRS9_RESULT WHERE DATANO=p_data_dt_str AND ITEM_CODE LIKE '7120%';

------表外差异即信用卡表外差异 ，总账-系统承兑汇票，保函，信用证，以及循环贷款未使用额度明细汇总，得到信用卡表外的总值
V_BWCHA := V_BWZZZBJ-V_BWXTZBJ-V_BWXHZBJ;

/*------分摊表外的减值差异到信用卡表外暴露每一项上 
MERGE INTO RWA_EI_EXPOSURE T1    
USING (
SELECT T2.EXPOSUREID AS EXPOSUREID,
       T2.ASSETBALANCE AS ASSETBALANCE,
       SUM(T2.ASSETBALANCE) over(partition by T2.SSYSID) AS SUMBALANCE,
       (T2.ASSETBALANCE/(SUM(T2.ASSETBALANCE) over(partition by T2.SSYSID)))*V_BWCHA  AS YBZBJ
       FROM RWA_EI_EXPOSURE T2
       WHERE T2.DATANO=p_data_dt_str AND T2.EXPOSUREID LIKE'BW%'
)T3
ON (T1.EXPOSUREID=T3.EXPOSUREID AND T1.DATANO=p_data_dt_str AND T1.EXPOSUREID LIKE 'BW%')
WHEN MATCHED THEN
  UPDATE SET T1.GENERALPROVISION = T1.GENERALPROVISION+NVL(T3.YBZBJ,0)
  ;

COMMIT;*/

------分摊表外的减值差异到信用卡表外暴露每一项上 
MERGE INTO RWA_EI_EXPOSURE T1    
USING (
SELECT T2.EXPOSUREID AS EXPOSUREID,
       T2.ASSETBALANCE AS ASSETBALANCE,
       SUM(T2.ASSETBALANCE) over(partition by T2.SSYSID) AS SUMBALANCE,
       (T2.ASSETBALANCE/(SUM(T2.ASSETBALANCE) over(partition by T2.SSYSID)))*V_BWCHA  AS YBZBJ
       FROM RWA_EI_EXPOSURE T2
       WHERE T2.DATANO=p_data_dt_str AND T2.EXPOSUREID LIKE'BW%'
)T3
ON (T1.EXPOSUREID=T3.EXPOSUREID AND T1.DATANO=p_data_dt_str AND T1.EXPOSUREID LIKE 'BW%')
WHEN MATCHED THEN
  UPDATE SET T1.GENERALPROVISION =NVL(T3.YBZBJ,0)
  ;

COMMIT;

------3.2 表内贷款信用卡差异分摊  BY  WZB  20191127
    select SUM(A.GENERALPROVISION) INTO V_BNXTZBJ from rwa_ei_exposure a   ----表内从I9获取的所有贷款和信用卡减值
where (accsubject1 LIKE'1303%' OR accsubject1 LIKE'1305%'  OR accsubject1 LIKE'1307%' OR accsubject1 LIKE'1310%')
and datano=p_data_dt_str;

 select SUM(BALANCE_C*NVL(B.MIDDLEPRICE/100, 1)-BALANCE_D*NVL(B.MIDDLEPRICE/100, 1)) into V_BNZZZBJ  ---表内总账减值
 from fns_gl_balance a
 LEFT JOIN TMP_CURRENCY_CHANGE B
 ON A.currency_code=B.CURRENCYCODE
 AND B.DATANO=p_data_dt_str
 where subject_no in('13040101','13040102','13040103')
        AND A.CURRENCY_CODE<>'RMB'
        AND A.DATANO=p_data_dt_str;

V_BNCHA :=V_BNZZZBJ-V_BNXTZBJ;------表内差异即信用卡表外差异  总账-系统


MERGE INTO RWA_EI_EXPOSURE T1     ------分摊表内的减值差异到8.3的暴露每一项上 
USING (
SELECT T2.EXPOSUREID AS EXPOSUREID,
       T2.ASSETBALANCE AS ASSETBALANCE,
       SUM(T2.ASSETBALANCE) over(partition by T2.EXPOSUBCLASSSTD) AS SUMBALANCE,
       (T2.ASSETBALANCE/(SUM(T2.ASSETBALANCE) over(partition by T2.EXPOSUBCLASSSTD)))*V_BNCHA  AS YBZBJ
       FROM RWA_EI_EXPOSURE T2
       WHERE T2.DATANO=p_data_dt_str AND T2.EXPOSUBCLASSSTD='010803' AND T2.EXPOBELONG='01'
)T3
ON (T1.EXPOSUREID=T3.EXPOSUREID AND T1.DATANO=p_data_dt_str AND T1.EXPOBELONG='01' AND T1.EXPOSUBCLASSSTD='010803')

WHEN MATCHED THEN
  UPDATE SET T1.GENERALPROVISION = T1.GENERALPROVISION+NVL(T3.YBZBJ,0)
  ;

COMMIT;


   /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_ARTICULATION_RESULT WHERE DATADATE = V_DATADATE;
    DBMS_OUTPUT.PUT_LINE('RWA_DEV.rwa_articulation_result表当前插入的数据记录为:' || V_COUNT || '条');

    SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_EI_CONTRACT WHERE DATADATE = V_DATADATE AND SSYSID = 'GC';
    DBMS_OUTPUT.PUT_LINE('RWA_DEV.rwa_ei_contract表当前插入的数据记录为:' || V_COUNT || '条');

    SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_EI_EXPOSURE WHERE DATADATE = V_DATADATE AND SSYSID = 'GC';
    DBMS_OUTPUT.PUT_LINE('RWA_DEV.rwa_ei_exposure表当前插入的数据记录为:' || V_COUNT || '条');

    DBMS_OUTPUT.PUT_LINE('【执行 ' || V_PRO_NAME || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg := '成功';

    --定义异常
   EXCEPTION
       WHEN OTHERS THEN
       ROLLBACK;
       p_po_rtncode := SQLCODE;
       p_po_rtnmsg := '总账勾稽出错：' || SQLERRM||';错误行数为:'|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
   RETURN;
END PRO_RWA_ACCOUNT_ARTICULATION;
/

