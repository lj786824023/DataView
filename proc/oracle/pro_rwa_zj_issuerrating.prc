CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZJ_ISSUERRATING(
                            p_data_dt_str IN  VARCHAR2,   --数据日期 yyyyMMdd
                            p_po_rtncode  OUT VARCHAR2,   --返回编号 1 成功,0 失败
                            p_po_rtnmsg   OUT VARCHAR2    --返回描述
        )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ZJ_ISSUERRATING
    实现功能:市场风险-资金系统-发行人评级信息表
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :CHENGANG
    编写时间:2019-04-18
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.BRD_BOND|债劵信息表
    源  表2 :RWA_DEV.NCM_CUSTOMER_RATING|客户外部评级表
    源  表3 :RWA_DEV.NCM_CUSTOMER_INFO|客户信息表
    源  表4 :RWA_DEV.RWA_CD_CODE_MAPPING\代码映射转换表
    源  表4 :RWA_DEV.RWA_CD_RATING_MAPPING\外部评级向标普转换表
    变更记录(修改人|修改时间|修改内容):
    pxl 20190905 调整发行人评级信息表  一个发行人有多家机构 多个评级结果
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZJ_ISSUERRATING';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  --v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZJ_ISSUERRATING';
    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.CREDIT_RATING_TMEP';

    --2.将满足条件的数据从源表插入到目标表中
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
    
    
    
    
    --插入评级结果表
    INSERT INTO RWA_DEV.RWA_ZJ_ISSUERRATING
      (DATADATE, --数据日期
       ISSUERID, --发行人ID
       ISSUERNAME, --发行人名称
       RATINGORG, --评级机构
       RATINGRESULT, --评级结果
       RATINGDATE, --评级日期
       FETCHFLAG --取数标识
      )
    SELECT DISTINCT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --数据日期                    
           NVL(T2.ISSUER_CODE, T7.CUSTOMERID),    --发行人ID
           T2.ISSUER_NAME,   --发行人名称
           NVL(T5.ORG_CD, 'WPJ'), --评级机构
           T5.CREDIT_RATING, --评级结果
           T5.RATING_DATE, --评级日期
           '1'    --取数标识          
       FROM CREDIT_RATING_TMEP T5
   INNER JOIN BRD_SECURITY_POSI T1 --债券头寸信息
           ON T5.BOND_ID = T1.SECURITY_REFERENCE
          AND T1.DATANO = p_data_dt_str
   INNER JOIN BRD_BOND T2 --债券
           ON T1.SECURITY_REFERENCE = T2.BOND_ID
          AND T2.DATANO = p_data_dt_str
          AND T2.BELONG_GROUP = '4' --资金系统       
   LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO T7 
          ON DECODE(T2.ISSUER_NAME, '仙桃市城市建设投资开发有限公司', '仙桃市城市建设投资开发公司', T2.ISSUER_NAME) = T7.CUSTOMERNAME --发行人特殊处理
          AND T7.DATANO = p_data_dt_str
          AND T7.CUSTOMERID <> 'ty2018120600000001' --中华人民共和国财政部 特殊处理                       
    WHERE T1.SBJT_CD = '11010101'  --以公允价值计量且其变动计入当期损益的金融资产         
      AND T2.BOND_TYPE NOT IN ('TTC')   --排除非国债  TTC 二级资本工具
      ;
        
    COMMIT;
       
    

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZJ_ISSUERRATING',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZJ_ISSUERRATING;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
         ROLLBACK;
         p_po_rtncode := sqlcode;
         p_po_rtnmsg  := '发行人评级信息表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZJ_ISSUERRATING;
/

