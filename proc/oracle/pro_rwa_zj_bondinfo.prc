CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZJ_BONDINFO(p_data_dt_str IN VARCHAR2, --数据日期 yyyyMMdd
                                                p_po_rtncode  OUT VARCHAR2, --返回编号 1 成功,0 失败
                                                p_po_rtnmsg   OUT VARCHAR2 --返回描述
                                                )
/*
  存储过程名称:RWA_DEV.PRO_RWA_ZJ_BONDINFO
  实现功能:市场风险-资金系统-债券信息表  
  数据口径:全量
  跑批频率:月初运行
  版  本  :V1.0.0
  编写人  :CHENGANG
  编写时间:2019-04-18
  单  位  :上海安硕信息技术股份有限公司
  源  表1 :RWA_DEV.BRD_BOND|债劵信息表
  源  表2 :RWA_DEV.NCM_BOND_INFO|债劵信息
  源  表3 :RWA_DEV.NCM_CUSTOMER_INFO|客户信息表
  源  表4 :RWA_DEV.RWA_CD_CODE_MAPPING\代码映射转换表
  源  表5 :RWA_DEV.BRD_CREDIT_RATING\债劵评级
  变更记录(修改人|修改时间|修改内容):
  pxl 2019/09/05 债券信息逻辑调整
  */
 AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZJ_BONDINFO';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  --v_count1 INTEGER;

BEGIN
  --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  /*如果是全量数据加载需清空目标表*/
  --1.清除目标表中的原有记录
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZJ_BONDINFO';
  
  --1.1 债券发行主体外部评级临时表
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.CREDIT_RATING_TMEP';
  
  --1.2 债券评级临时表
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.CREDIT_RATING_ZQ_TMEP';
  
  --1.3 加工债券发行主体外部评级信息
  INSERT INTO CREDIT_RATING_TMEP
      (BOND_ID, ORG_CD, RATING_DATE, CREDIT_RATING)
  SELECT R.BOND_ID, 
             CASE 
              WHEN ORG_NAME = '中诚信国际信用评级有限责任公司' THEN '001'
              WHEN ORG_NAME = '中诚信证券评估有限公司' THEN '001'
              WHEN ORG_NAME = '中证鹏元资信评估股份有限公司' THEN '002'  
              WHEN ORG_NAME = '鹏元资信评估有限公司' THEN '002' 
              WHEN ORG_NAME = '联合信用评级有限公司' THEN '003'
              WHEN ORG_NAME = '联合资信评估有限公司' THEN '003'
              WHEN ORG_NAME = '大公国际资信评估有限公司' THEN '004'
              WHEN ORG_NAME = '东方金诚国际信用评估有限公司' THEN '005'
              WHEN ORG_NAME = '中债资信评估有限责任公司' THEN '006'
              WHEN ORG_NAME = '上海新世纪资信评估投资服务有限公司' THEN '007'
              WHEN ORG_NAME = '远东资信评估有限公司' THEN '008'                                                                          
              ELSE '099'
            END AS ORG_CD, 
            MAX(R.RATING_DATE) RATING_DATE, 
            CASE 
              WHEN R.CREDIT_RATING = 'AAA+' THEN '0101'
              WHEN R.CREDIT_RATING = 'AAA' THEN '0101'
              WHEN R.CREDIT_RATING = 'AAA-' THEN '0101'
              WHEN R.CREDIT_RATING = 'AA+' THEN '0102'
              WHEN R.CREDIT_RATING = 'AA' THEN '0102'
              WHEN R.CREDIT_RATING = 'AA-' THEN '0102'
              WHEN R.CREDIT_RATING = 'A+' THEN '0105'
              WHEN R.CREDIT_RATING = 'A' THEN '0106'
              WHEN R.CREDIT_RATING = 'A-' THEN '0107'
              WHEN R.CREDIT_RATING = 'BBB+' THEN '0108'
              WHEN R.CREDIT_RATING = 'BBB' THEN '0109'
              WHEN R.CREDIT_RATING = 'BBB-' THEN '0110'
              WHEN R.CREDIT_RATING = 'BB+' THEN '0111'
              WHEN R.CREDIT_RATING = 'BB' THEN '0112'
              WHEN R.CREDIT_RATING = 'BB-' THEN '0113'
              WHEN R.CREDIT_RATING = 'B+' THEN '0114'
              WHEN R.CREDIT_RATING = 'B' THEN '0115'
              WHEN R.CREDIT_RATING = 'B-' THEN '0116'
              WHEN R.CREDIT_RATING = 'CCC+' THEN '0117'
              WHEN R.CREDIT_RATING = 'CCC' THEN '0118'
              WHEN R.CREDIT_RATING = 'CCC-' THEN '0119'
              WHEN R.CREDIT_RATING = 'CC+' THEN '0120'
              WHEN R.CREDIT_RATING = 'CC' THEN '0120'
              WHEN R.CREDIT_RATING = 'CC-' THEN '0120'
              WHEN R.CREDIT_RATING = 'C+' THEN '0121'
              WHEN R.CREDIT_RATING = 'C' THEN '0121'
              WHEN R.CREDIT_RATING = 'C-' THEN '0121'
              WHEN R.CREDIT_RATING = 'A-1' THEN '0201'
              WHEN R.CREDIT_RATING = 'A-2' THEN '0202'
              WHEN R.CREDIT_RATING = 'A-3' THEN '0203'
              WHEN R.CREDIT_RATING = 'D' THEN '0207'
              ELSE '0207'
            END AS CREDIT_RATING
      FROM BRD_CREDIT_RATING R
       WHERE R.DATANO = p_data_dt_str
         AND R.BELONG_GROUP = '2'
         AND R.RATING_TYPE = 'S' --  S  1  主体评级  C  2  信用评级 I  3  内部评级 Z  4  债项评级         
      GROUP BY R.BOND_ID, 
             CASE 
              WHEN ORG_NAME = '中诚信国际信用评级有限责任公司' THEN '001'
              WHEN ORG_NAME = '中诚信证券评估有限公司' THEN '001'
              WHEN ORG_NAME = '中证鹏元资信评估股份有限公司' THEN '002'  
              WHEN ORG_NAME = '鹏元资信评估有限公司' THEN '002' 
              WHEN ORG_NAME = '联合信用评级有限公司' THEN '003'
              WHEN ORG_NAME = '联合资信评估有限公司' THEN '003'
              WHEN ORG_NAME = '大公国际资信评估有限公司' THEN '004'
              WHEN ORG_NAME = '东方金诚国际信用评估有限公司' THEN '005'
              WHEN ORG_NAME = '中债资信评估有限责任公司' THEN '006'
              WHEN ORG_NAME = '上海新世纪资信评估投资服务有限公司' THEN '007'
              WHEN ORG_NAME = '远东资信评估有限公司' THEN '008'                                                                          
              ELSE '099'
            END, 
            CASE 
              WHEN R.CREDIT_RATING = 'AAA+' THEN '0101'
              WHEN R.CREDIT_RATING = 'AAA' THEN '0101'
              WHEN R.CREDIT_RATING = 'AAA-' THEN '0101'
              WHEN R.CREDIT_RATING = 'AA+' THEN '0102'
              WHEN R.CREDIT_RATING = 'AA' THEN '0102'
              WHEN R.CREDIT_RATING = 'AA-' THEN '0102'
              WHEN R.CREDIT_RATING = 'A+' THEN '0105'
              WHEN R.CREDIT_RATING = 'A' THEN '0106'
              WHEN R.CREDIT_RATING = 'A-' THEN '0107'
              WHEN R.CREDIT_RATING = 'BBB+' THEN '0108'
              WHEN R.CREDIT_RATING = 'BBB' THEN '0109'
              WHEN R.CREDIT_RATING = 'BBB-' THEN '0110'
              WHEN R.CREDIT_RATING = 'BB+' THEN '0111'
              WHEN R.CREDIT_RATING = 'BB' THEN '0112'
              WHEN R.CREDIT_RATING = 'BB-' THEN '0113'
              WHEN R.CREDIT_RATING = 'B+' THEN '0114'
              WHEN R.CREDIT_RATING = 'B' THEN '0115'
              WHEN R.CREDIT_RATING = 'B-' THEN '0116'
              WHEN R.CREDIT_RATING = 'CCC+' THEN '0117'
              WHEN R.CREDIT_RATING = 'CCC' THEN '0118'
              WHEN R.CREDIT_RATING = 'CCC-' THEN '0119'
              WHEN R.CREDIT_RATING = 'CC+' THEN '0120'
              WHEN R.CREDIT_RATING = 'CC' THEN '0120'
              WHEN R.CREDIT_RATING = 'CC-' THEN '0120'
              WHEN R.CREDIT_RATING = 'C+' THEN '0121'
              WHEN R.CREDIT_RATING = 'C' THEN '0121'
              WHEN R.CREDIT_RATING = 'C-' THEN '0121'
              WHEN R.CREDIT_RATING = 'A-1' THEN '0201'
              WHEN R.CREDIT_RATING = 'A-2' THEN '0202'
              WHEN R.CREDIT_RATING = 'A-3' THEN '0203'
              WHEN R.CREDIT_RATING = 'D' THEN '0207'
              ELSE '0207'
            END
    ;
         
   COMMIT;

  --1.4 加工债券评级信息
  INSERT INTO CREDIT_RATING_ZQ_TMEP
      (BOND_ID, ORG_CD, RATING_DATE, CREDIT_RATING)
    SELECT R.BOND_ID, 
             CASE 
              WHEN ORG_NAME = '中诚信国际信用评级有限责任公司' THEN '001'
              WHEN ORG_NAME = '中诚信证券评估有限公司' THEN '001'
              WHEN ORG_NAME = '中证鹏元资信评估股份有限公司' THEN '002'  
              WHEN ORG_NAME = '鹏元资信评估有限公司' THEN '002' 
              WHEN ORG_NAME = '联合信用评级有限公司' THEN '003'
              WHEN ORG_NAME = '联合资信评估有限公司' THEN '003'
              WHEN ORG_NAME = '大公国际资信评估有限公司' THEN '004'
              WHEN ORG_NAME = '东方金诚国际信用评估有限公司' THEN '005'
              WHEN ORG_NAME = '中债资信评估有限责任公司' THEN '006'
              WHEN ORG_NAME = '上海新世纪资信评估投资服务有限公司' THEN '007'
              WHEN ORG_NAME = '远东资信评估有限公司' THEN '008'                                                                          
              ELSE '099'
            END AS ORG_CD, 
            R.RATING_DATE, 
            CASE 
              WHEN R.CREDIT_RATING = 'AAA+' THEN '0101'
              WHEN R.CREDIT_RATING = 'AAA' THEN '0101'
              WHEN R.CREDIT_RATING = 'AAA-' THEN '0101'
              WHEN R.CREDIT_RATING = 'AA+' THEN '0102'
              WHEN R.CREDIT_RATING = 'AA' THEN '0102'
              WHEN R.CREDIT_RATING = 'AA-' THEN '0102'
              WHEN R.CREDIT_RATING = 'A+' THEN '0105'
              WHEN R.CREDIT_RATING = 'A' THEN '0106'
              WHEN R.CREDIT_RATING = 'A-' THEN '0107'
              WHEN R.CREDIT_RATING = 'BBB+' THEN '0108'
              WHEN R.CREDIT_RATING = 'BBB' THEN '0109'
              WHEN R.CREDIT_RATING = 'BBB-' THEN '0110'
              WHEN R.CREDIT_RATING = 'BB+' THEN '0111'
              WHEN R.CREDIT_RATING = 'BB' THEN '0112'
              WHEN R.CREDIT_RATING = 'BB-' THEN '0113'
              WHEN R.CREDIT_RATING = 'B+' THEN '0114'
              WHEN R.CREDIT_RATING = 'B' THEN '0115'
              WHEN R.CREDIT_RATING = 'B-' THEN '0116'
              WHEN R.CREDIT_RATING = 'CCC+' THEN '0117'
              WHEN R.CREDIT_RATING = 'CCC' THEN '0118'
              WHEN R.CREDIT_RATING = 'CCC-' THEN '0119'
              WHEN R.CREDIT_RATING = 'CC+' THEN '0120'
              WHEN R.CREDIT_RATING = 'CC' THEN '0120'
              WHEN R.CREDIT_RATING = 'CC-' THEN '0120'
              WHEN R.CREDIT_RATING = 'C+' THEN '0121'
              WHEN R.CREDIT_RATING = 'C' THEN '0121'
              WHEN R.CREDIT_RATING = 'C-' THEN '0121'
              WHEN R.CREDIT_RATING = 'A-1' THEN '0201'
              WHEN R.CREDIT_RATING = 'A-2' THEN '0202'
              WHEN R.CREDIT_RATING = 'A-3' THEN '0203'
              WHEN R.CREDIT_RATING = 'D' THEN '0207'
              ELSE '0207'
            END AS CREDIT_RATING
      FROM BRD_CREDIT_RATING R
       WHERE R.DATANO = p_data_dt_str
         AND R.BELONG_GROUP = '2'
         AND R.RATING_TYPE = 'Z' --  S  1  主体评级  C  2  信用评级 I  3  内部评级 Z  4  债项评级         
   ;
   
   COMMIT; 

  --2.1将满足条件的数据从源表插入到目标表中  
   INSERT INTO RWA_DEV.RWA_ZJ_BONDINFO
    (
     DATADATE, --数据日期
     BONDID, --债券ID
     BONDNAME, --债券名称
     BONDTYPE, --债券类型
     ERATING, --外部评级
     ISSUERID, --发行人ID
     ISSUERNAME, --发行人名称
     ISSUERTYPE, --发行人大类
     ISSUERSUBTYPE, --发行人小类
     ISSUERREGISTSTATE, --发行人注册国家
     ISSUERSMBFLAG, --发行人小微企业标识
     BONDISSUEINTENT, --债券发行目的
     REABSFLAG, --再资产证券化标识
     ORIGINATORFLAG, --是否发起机构
     STARTDATE, --起始日期
     DUEDATE, --到期日期
     ORIGINALMATURITY, --原始期限
     RESIDUALM, --剩余期限
     RATETYPE, --利率类型
     EXECUTIONRATE, --执行利率
     NEXTREPRICEDATE, --下次重定价日
     NEXTREPRICEM, --下次重定价期限
     MODIFIEDDURATION, --修正久期
     DENOMINATION, --面额
     CURRENCY, --币种
     SECURITY_REFERENCE  --证券唯一标示
    ) 
     SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --数据日期         
           T1.ACCT_NO,  --债券ID
           T2.BOND_FULL_NAME, --债券名称
           CASE
            WHEN T2.BOND_TYPE = 'TB' THEN '01'  --TB	国债             
            WHEN T2.BOND_TYPE = 'TBB' THEN '01'  --TBB	央行票据
            WHEN T2.BOND_TYPE = 'PBB' THEN '01'  --PBB	政策性银行债券
            WHEN T2.BOND_TYPE = 'ABS' THEN '03'  --ABS	资产支持证券            
            ELSE CASE 
                    WHEN T6.BOND_ID IS NOT NULL OR T6.BOND_ID <> '' THEN '02' -- 发行主体存在俩个BB+的外部评级信息是 合格证券
                    ELSE '09'		--09	其他证券
                 END         
           END, --债券类型
           T5.CREDIT_RATING, --外部评级
           NVL(T2.ISSUER_CODE, T7.CUSTOMERID),    --发行人ID
           T2.ISSUER_NAME,   --发行人名称
           '',       --发行人大类 发行人类型通过客户类型规则加工
           '',       --发行人小类 发行人类型通过客户类型规则加工
           '01' ,   --发行人注册国家
           NVL(T7.ISSUPERVISESTANDARSMENT, '0'),  --发行人小微企业标识  默认 否
           '02', --债券发行目的  默认 02 其它
           '0',   --再资产证券化标识
           '0',   --是否发起机构
           T2.ISSUE_DATE,  --起始日期
           T2.MATU_DT,     --到期日期
           CASE
             WHEN (TO_DATE(T2.MATU_DT, 'YYYY-MM-DD') -
                  TO_DATE(T2.ISSUE_DATE, 'YYYY-MM-DD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T2.MATU_DT, 'YYYY-MM-DD') -
              TO_DATE(T2.ISSUE_DATE, 'YYYY-MM-DD')) / 365
           END,  --原始期限
           CASE
             WHEN (TO_DATE(T2.MATU_DT, 'YYYY-MM-DD') -
                  TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T2.MATU_DT, 'YYYY-MM-DD') -
              TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
           END,  --剩余期限
           CASE 
             WHEN T2.RATE_TYPE = 'FI' THEN '01'
             WHEN T2.RATE_TYPE = 'FL' THEN '02'
             ELSE '@'
           END ,  --利率类型  --FIXED 01  固定利率 FLOATING  02 浮动利率
           T2.EXEC_INTST_RATE,  --执行利率
           CASE
             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'Y' THEN
              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                         'YYYYMMDD'),
                                 12),
                      'YYYYMMDD') --增加一年
             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'S' THEN
              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                         'YYYYMMDD'),
                                 6),
                      'YYYYMMDD') --增加半年
             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'S' THEN
              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                         'YYYYMMDD'),
                                 3),
                      'YYYYMMDD') --增加一个季度
             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'M' THEN
              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                         'YYYYMMDD'),
                                 1),
                      'YYYYMMDD') --增加一个月
             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'DSD' THEN
              T2.MATU_DT --增加一个月
             ELSE
              NULL
           END ,  --下次重定价日
           CASE
             WHEN (TO_DATE(CASE
                             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'Y' THEN
                              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                         'YYYYMMDD'),
                                                 12),
                                      'YYYYMMDD') --增加一年
                             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'S' THEN
                              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                         'YYYYMMDD'),
                                                 6),
                                      'YYYYMMDD') --增加半年
                             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'S' THEN
                              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                         'YYYYMMDD'),
                                                 3),
                                      'YYYYMMDD') --增加一个季度
                             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'M' THEN
                              TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                         SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                         'YYYYMMDD'),
                                                 1),
                                      'YYYYMMDD') --增加一个月
                             WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'DSD' THEN
                              T2.MATU_DT --增加一个月
                             ELSE
                              NULL
                           END
             , 'YYYY-MM-DD') -
                  TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN 0
         ELSE
              (TO_DATE(CASE
                           WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'Y' THEN
                            TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                       SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                       'YYYYMMDD'),
                                               12),
                                    'YYYYMMDD') --增加一年
                           WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'S' THEN
                            TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                       SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                       'YYYYMMDD'),
                                               6),
                                    'YYYYMMDD') --增加半年
                           WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'S' THEN
                            TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                       SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                       'YYYYMMDD'),
                                               3),
                                    'YYYYMMDD') --增加一个季度
                           WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'M' THEN
                            TO_CHAR(ADD_MONTHS(TO_DATE((SUBSTR(p_data_dt_str, 1, 4)) ||
                                                       SUBSTR(T2.ISSUE_DATE, 5, 4),
                                                       'YYYYMMDD'),
                                               1),
                                    'YYYYMMDD') --增加一个月
                           WHEN T2.RATE_TYPE = 'FI' AND T2.RPRIC_FREQ = 'DSD' THEN
                            T2.MATU_DT --增加一个月
                           ELSE
                            NULL
                         END, 'YYYY-MM-DD') -
              TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
           END,               --下次重定价期限
           NULL,              --修正久期
           T1.PAR_VALUE,  --面额
           DECODE(T2.CCY_CD, '156', 'CNY', T2.CCY_CD), ---币种统一
           T1.SECURITY_REFERENCE 
       FROM BRD_SECURITY_POSI T1 --债券头寸信息
       INNER JOIN BRD_BOND T2 --债券
         ON T1.SECURITY_REFERENCE = T2.BOND_ID
        AND T2.DATANO = p_data_dt_str
        AND T2.BELONG_GROUP = '4' --资金系统
       LEFT JOIN (
            SELECT T.BOND_ID,
              MAX(T.RATING_DATE) RATING_DATE,
              MIN(T.CREDIT_RATING) CREDIT_RATING
           FROM CREDIT_RATING_ZQ_TMEP T  --债券外部评级信息临时表
          GROUP BY T.BOND_ID
       ) T5  --评级结果1
         ON T5.BOND_ID = T1.SECURITY_REFERENCE
       LEFT JOIN (
             SELECT BOND_ID, COUNT(*) AS CREDIT_RATING_NUM
               FROM CREDIT_RATING_TMEP T1   --债券发行主体评级信息临时表
              WHERE CREDIT_RATING < '0111' --BB+ 以上的
              GROUP BY BOND_ID
             HAVING COUNT(*) >= 2 --倆个评级
       ) T6
         ON T6.BOND_ID = T1.SECURITY_REFERENCE 
       LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO T7 
              ON DECODE(T2.ISSUER_NAME, '仙桃市城市建设投资开发有限公司', '仙桃市城市建设投资开发公司', T2.ISSUER_NAME) = T7.CUSTOMERNAME --发行人特殊处理
              AND T7.DATANO = p_data_dt_str
              AND T7.CUSTOMERID <> 'ty2018120600000001' --中华人民共和国财政部 特殊处理                       
      WHERE T1.DATANO = p_data_dt_str
        AND T1.SBJT_CD = '11010101'  --以公允价值计量且其变动计入当期损益的金融资产         
        AND T2.BOND_TYPE NOT IN ('TTC')   --排除非国债  TTC 二级资本工具      
    ;
commit;
----衍生品债券信息
 INSERT INTO RWA_DEV.RWA_ZJ_BONDINFO
    (
     DATADATE, --数据日期
     BONDID, --债券ID
     BONDNAME, --债券名称
     BONDTYPE, --债券类型
     ERATING, --外部评级
     ISSUERID, --发行人ID
     ISSUERNAME, --发行人名称
     ISSUERTYPE, --发行人大类
     ISSUERSUBTYPE, --发行人小类
     ISSUERREGISTSTATE, --发行人注册国家
     ISSUERSMBFLAG, --发行人小微企业标识
     BONDISSUEINTENT, --债券发行目的
     REABSFLAG, --再资产证券化标识
     ORIGINATORFLAG, --是否发起机构
     STARTDATE, --起始日期
     DUEDATE, --到期日期
     ORIGINALMATURITY, --原始期限
     RESIDUALM, --剩余期限
     RATETYPE, --利率类型
     EXECUTIONRATE, --执行利率
     NEXTREPRICEDATE, --下次重定价日
     NEXTREPRICEM, --下次重定价期限
     MODIFIEDDURATION, --修正久期
     DENOMINATION, --面额
     CURRENCY, --币种
     SECURITY_REFERENCE  --证券唯一标示
    ) 
     SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --数据日期      
       T.DEALNO || T.SEQ, --债券ID
     '利率互换', --债券名称
     '02', --债券类型
     0207, --外部评级
      nvl(C.TAXID,'OPI'||H.CNO), --发行人ID
     C.CFN1, --发行人名称
     '', --发行人大类
     '', --发行人小类
     CASE WHEN C.CCODE = 'CN' THEN '01' ELSE '02' END, --发行人注册国家
     '0', --发行人小微企业标识
     '02', --债券发行目的
     '0', --再资产证券化标识
     '0', --是否发起机构
      T.STARTDATE, --起始日期
     CASE WHEN T.FIXFLOATIND = 'X' THEN T.Matdate
     WHEN T.FIXFLOATIND = 'L' AND( SUBSTR(T.RATECODE, 1, 2) )= '6M'
     THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD') 
     END,  --到期日期
      CASE
             WHEN (TO_DATE( (CASE WHEN T.FIXFLOATIND = 'X' THEN T.Matdate
     WHEN T.FIXFLOATIND = 'L' AND( SUBSTR(T.RATECODE, 1, 2) )= '6M'
     THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD') 
     END), 'YYYY-MM-DD') -
                  TO_DATE( T.STARTDATE, 'YYYY-MM-DD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE( (CASE WHEN T.FIXFLOATIND = 'X' THEN T.Matdate
     WHEN T.FIXFLOATIND = 'L' AND( SUBSTR(T.RATECODE, 1, 2) )= '6M'
     THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD') 
     END), 'YYYY-MM-DD') -
              TO_DATE( T.STARTDATE, 'YYYY-MM-DD')) / 365
           END, --原始期限
          CASE
             WHEN (TO_DATE( (CASE WHEN T.FIXFLOATIND = 'X' THEN T.Matdate
     WHEN T.FIXFLOATIND = 'L' AND( SUBSTR(T.RATECODE, 1, 2) )= '6M'
     THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD') 
     END), 'YYYY-MM-DD') -
                  TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE( (CASE WHEN T.FIXFLOATIND = 'X' THEN T.Matdate
     WHEN T.FIXFLOATIND = 'L' AND( SUBSTR(T.RATECODE, 1, 2) )= '6M'
     THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD') 
     END), 'YYYY-MM-DD') -
              TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365
           END, --剩余期限
     CASE WHEN T.FIXFLOATIND = 'X' THEN '01' --固定利率
     WHEN T.FIXFLOATIND = 'L' THEN '02' 
      END,--浮动利率 END, --利率类型
      T.INTRATE_8, --执行利率
      CASE 
      WHEN T.FIXFLOATIND = 'L' AND SUBSTR(T.RATECODE, 1, 2) = '6M' THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD')  --L 浮动利率重定价日期 = 起息日期 + 重估频率  后续根据数据情况增加重估频率算法
      ELSE NULL
      END, --下次重定价日
             CASE
             WHEN (TO_DATE( (CASE 
     WHEN T.FIXFLOATIND = 'L' AND( SUBSTR(T.RATECODE, 1, 2) )= '6M'
     THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD') 
     END), 'YYYY-MM-DD') -
                  TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE( (CASE 
     WHEN T.FIXFLOATIND = 'L' AND( SUBSTR(T.RATECODE, 1, 2) )= '6M'
     THEN TO_CHAR(ADD_MONTHS(TO_DATE(T.STARTDATE, 'YYYYMMDD'), 6), 'YYYYMMDD') 
     END), 'YYYY-MM-DD') -
              TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365
           END, --下次重定价期限
      '', --修正久期
      T.NOTCCYAMT, --面额
      T.INTCCY, --币种
      ''  --证券唯一标示
      FROM  OPI_SWDT T --互换交易
         LEFT JOIN OPI_SWDH H --互换报头 
                ON T.DEALNO = H.DEALNO
               AND H.DATANO = p_data_dt_str
         LEFT JOIN  OPI_CUST C --客户信息
                ON H.CNO = C.CNO 
               AND C.DATANO = p_data_dt_str
        WHERE T.DATANO = p_data_dt_str;
    
    commit;

  dbms_stats.gather_table_stats(ownname => 'RWA_DEV',
                                tabname => 'RWA_ZJ_BONDINFO',
                                cascade => true);

  /*目标表数据统计*/
  --统计插入的记录数
  SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZJ_BONDINFO;
  --Dbms_output.Put_line('RWA_DEV.RWA_TZ_CONTRACT表当前插入的财务系统-应收款投资数据记录为: ' || (v_count1 - v_count) || ' 条');

  --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  p_po_rtncode := '1';
  p_po_rtnmsg  := '成功' || '-' || v_count;
  --定义异常
EXCEPTION
  WHEN OTHERS THEN
    --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
    ROLLBACK;
    p_po_rtncode := sqlcode;
    p_po_rtnmsg  := '债券信息表-资金系统(' || v_pro_name || ')ETL转换失败！' || sqlerrm ||
                    ';错误行数为:' || dbms_utility.format_error_backtrace;
    RETURN;
END PRO_RWA_ZJ_BONDINFO;
/

