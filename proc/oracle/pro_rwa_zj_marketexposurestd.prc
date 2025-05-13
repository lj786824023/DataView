CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZJ_MARKETEXPOSURESTD(
                            p_data_dt_str IN  VARCHAR2,   --数据日期 yyyyMMdd
                            p_po_rtncode  OUT VARCHAR2,   --返回编号 1 成功,0 失败
                            p_po_rtnmsg   OUT VARCHAR2    --返回描述
        )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ZJ_MARKETEXPOSURESTD
    实现功能:市场风险-资金系统-标准法风险暴露表
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :CHENGANG
    编写时间:2019-04-18
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_ZJ_BONDINFO|债劵信息表
    源  表2 :RWA_DEV.RWA_ZJ_TRADBONDPOSITION|债券头寸信息表
    源  表3 :RWA.RWA_WP_COUNTRYRATING |国际评级信息表
    源  表4 :RWA.ORG_INFO|机构信息表
    源  表5 :RWA_DEV.BRD_BOND|债劵表
    源  表6 :RWA_DEV.BRD_SECURITY_POSI|债券头寸信息表
     变更记录(修改人|修改时间|修改内容):
     pxl 2019/09/05  标准法风险暴露表逻辑调整
     
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZJ_MARKETEXPOSURESTD';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  --v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZJ_MARKETEXPOSURESTD';

    --2.将满足条件的数据从源表插入到目标表中
  INSERT INTO RWA_DEV.RWA_ZJ_MARKETEXPOSURESTD
      ( DATADATE,          --数据日期
        DATANO,          --数据流水号
        EXPOSUREID,          --风险暴露ID
        BOOKTYPE,          --账户类别
        INSTRUMENTSID,          --金融工具ID
        INSTRUMENTSTYPE,          --金融工具类型
        ORGSORTNO,          --所属机构排序号
        ORGID,          --所属机构ID
        ORGNAME,          --所属机构名称
        ORGTYPE,          --所属机构类型
        MARKETRISKTYPE,          --市场风险类型
        INTERATERISKTYPE,          --利率风险类型
        EQUITYRISKTYPE,          --股票风险类型
        EXCHANGERISKTYPE,          --外汇风险类型
        COMMODITYNAME,          --商品种类名称
        OPTIONRISKTYPE,          --期权风险类型
        ISSUERID,          --发行人ID
        ISSUERNAME,          --发行人名称
        ISSUERTYPE,          --发行人大类
        ISSUERSUBTYPE,          --发行人小类
        ISSUERREGISTSTATE,          --发行人注册国家
        ISSUERRCERATING,          --发行人境外注册地外部评级
        SMBFLAG,          --小微企业标识
        UNDERBONDFLAG,          --是否承销债券
        PAYMENTDATE,          --缴款日
        SECURITIESTYPE,          --证券类别
        BONDISSUEINTENT,          --债券发行目的
        CLAIMSLEVEL,          --债权级别
        REABSFLAG,          --再资产证券化标识
        ORIGINATORFLAG,          --是否发起机构
        SECURITIESERATING,          --证券外部评级
        STOCKCODE,          --股票/股指代码
        STOCKMARKET,          --交易市场
        EXCHANGEAREA,          --交易地区
        STRUCTURALEXPOFLAG,          --是否结构性敞口
        OPTIONUNDERLYINGFLAG,          --是否期权基础工具
        OPTIONUNDERLYINGNAME,          --期权基础工具名称
        OPTIONID,          --期权工具ID
        VOLATILITY,          --波动率
        STARTDATE,          --起始日期
        DUEDATE,          --到期日期
        ORIGINALMATURITY,          --原始期限
        RESIDUALM,          --剩余期限
        NEXTREPRICEDATE,          --下次重定价日
        NEXTREPRICEM,          --下次重定价期限
        RATETYPE,          --利率类型
        COUPONRATE,          --票面利率
        MODIFIEDDURATION,          --修正久期
        POSITIONTYPE,          --头寸属性
        POSITION,          --头寸
        CURRENCY,          --币种
        OPTIONUNDERLYINGTYPE          --期权基础工具类型
       )
     SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'),    --数据日期
            p_data_dt_str,   --数据流水号
            T1.POSITIONID,    --风险暴露ID
            '02',   --账户类别
            T1.BONDID,    --金融工具ID
            T1.INSTRUMENTSTYPE,   --金融工具类型
            T2.SORTNO,    --所属机构排序号
            T1.ACCORGID,    --所属机构ID
            T2.ORGNAME,   --所属机构名称
            '01',   --所属机构类型
            '01',   --市场风险类型
            '01',   --利率风险类型
            '',   --股票风险类型
            '',   --外汇风险类型
            '',   --商品种类名称
            '',   --期权风险类型
            T3.ISSUERID,    --发行人ID
            T3.ISSUERNAME,    --发行人名称
            T3.ISSUERTYPE,    --发行人大类
            T3.ISSUERSUBTYPE,   --发行人小类
            T3.ISSUERREGISTSTATE,   --发行人注册国家
            T4.RATINGRESULT,    --发行人境外注册地外部评级
            T3.ISSUERSMBFLAG,   --小微企业标识
            '0',    --是否承销债券
            '',   --缴款日
            T3.BONDTYPE,    --证券类别
            T3.BONDISSUEINTENT,   --债券发行目的
            CASE
              WHEN T5.BOND_TYPE IN ('OBB',
                                 'XYBS' --政策性银行次级债、商业银行次级债
                                 ) THEN
               '01'
              ELSE
               '02'
            END CLAIMSLEVEL,   --债权级别
            T3.REABSFLAG,   --再资产证券化标识
            T3.ORIGINATORFLAG,    --是否发起机构
            T3.ERATING,   --证券外部评级
            ''  ,   --股票/股指代码
            ''  ,   --交易市场
            ''  ,   --交易地区
            ''  ,   --是否结构性敞口
            '0' ,   --是否期权基础工具
            ''  ,   --期权基础工具名称
            ''  ,   --期权工具ID
            ''  ,   --波动率
            T3.STARTDATE  ,   --起始日期
            T3.DUEDATE  ,   --到期日期
            T3.ORIGINALMATURITY ,   --原始期限
            T3.RESIDUALM  ,   --剩余期限
            T3.NEXTREPRICEDATE  ,   --下次重定价日
            T3.NEXTREPRICEM ,   --下次重定价期限
            T3.RATETYPE ,   --利率类型
            T3.EXECUTIONRATE  ,   --票面利率
            T3.MODIFIEDDURATION ,   --修正久期
            '01'  ,   --头寸属性 默认 01  多头
            T1.BOOKBALANCE  ,   --头寸
            T1.CURRENCY ,   --币种
            ''      --期权基础工具类型
   FROM RWA_ZJ_TRADBONDPOSITION T1
      INNER JOIN RWA_ZJ_BONDINFO T3
         ON T1.BONDID = T3.BONDID
        AND T3.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
       LEFT JOIN RWA.ORG_INFO T2
         ON T1.ACCORGID = T2.ORGID
        AND T2.STATUS = '1'
       LEFT JOIN RWA.RWA_WP_COUNTRYRATING T4
         ON T3.ISSUERREGISTSTATE = T4.COUNTRYCODE
        AND T4.ISINUSE = '1'
       INNER JOIN BRD_BOND T5
         ON T3.SECURITY_REFERENCE = T5.BOND_ID  --证券唯一标示
        AND T5.BELONG_GROUP = '4' --资金系统
        AND T5.DATANO = p_data_dt_str
   WHERE T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') 
    ;
    
    COMMIT;

 --2.衍生品暴露表
 /*
    INSERT INTO RWA_DEV.RWA_ZJ_MARKETEXPOSURESTD
      ( DATADATE,          --数据日期
        DATANO,          --数据流水号
        EXPOSUREID,          --风险暴露ID
        BOOKTYPE,          --账户类别
        INSTRUMENTSID,          --金融工具ID
        INSTRUMENTSTYPE,          --金融工具类型
        ORGSORTNO,          --所属机构排序号
        ORGID,          --所属机构ID
        ORGNAME,          --所属机构名称
        ORGTYPE,          --所属机构类型
        MARKETRISKTYPE,          --市场风险类型
        INTERATERISKTYPE,          --利率风险类型
        EQUITYRISKTYPE,          --股票风险类型
        EXCHANGERISKTYPE,          --外汇风险类型
        COMMODITYNAME,          --商品种类名称
        OPTIONRISKTYPE,          --期权风险类型
        ISSUERID,          --发行人ID
        ISSUERNAME,          --发行人名称
        ISSUERTYPE,          --发行人大类
        ISSUERSUBTYPE,          --发行人小类
        ISSUERREGISTSTATE,          --发行人注册国家
        ISSUERRCERATING,          --发行人境外注册地外部评级
        SMBFLAG,          --小微企业标识
        UNDERBONDFLAG,          --是否承销债券
        PAYMENTDATE,          --缴款日
        SECURITIESTYPE,          --证券类别
        BONDISSUEINTENT,          --债券发行目的
        CLAIMSLEVEL,          --债权级别
        REABSFLAG,          --再资产证券化标识
        ORIGINATORFLAG,          --是否发起机构
        SECURITIESERATING,          --证券外部评级
        STOCKCODE,          --股票/股指代码
        STOCKMARKET,          --交易市场
        EXCHANGEAREA,          --交易地区
        STRUCTURALEXPOFLAG,          --是否结构性敞口
        OPTIONUNDERLYINGFLAG,          --是否期权基础工具
        OPTIONUNDERLYINGNAME,          --期权基础工具名称
        OPTIONID,          --期权工具ID
        VOLATILITY,          --波动率
        STARTDATE,          --起始日期
        DUEDATE,          --到期日期
        ORIGINALMATURITY,          --原始期限
        RESIDUALM,          --剩余期限
        NEXTREPRICEDATE,          --下次重定价日
        NEXTREPRICEM,          --下次重定价期限
        RATETYPE,          --利率类型
        COUPONRATE,          --票面利率
        MODIFIEDDURATION,          --修正久期
        POSITIONTYPE,          --头寸属性
        POSITION,          --头寸
        CURRENCY,          --币种
        OPTIONUNDERLYINGTYPE          --期权基础工具类型
       )
     SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --数据日期
            p_data_dt_str, --数据流水号
            T1.POSITIONID, --风险暴露ID
            CASE
              WHEN SUBSTR(H.COST, 1， 4) = 'E' THEN
               '01'
              ELSE
               '02'
            END, --账户类别
            T1.BONDID, --金融工具ID
            T1.INSTRUMENTSTYPE, --金融工具类型
            T2.SORTNO, --所属机构排序号
            T1.TRANORGID, --所属机构ID
            T2.ORGNAME, --所属机构名称
            CASE
              WHEN C.Ccode = 'CN' THEN
               '01'
              ELSE
               '02'
            END, --所属机构类型
            '01', --市场风险类型
            '02', --利率风险类型
            '', --股票风险类型
            '', --外汇风险类型
            '', --商品种类名称
            '', --期权风险类型
            T3.ISSUERID, --发行人ID
            T3.ISSUERNAME, --发行人名称
            T3.ISSUERTYPE, --发行人大类
            T3.ISSUERSUBTYPE, --发行人小类
            T3.ISSUERREGISTSTATE, --发行人注册国家
            '', --发行人境外注册地外部评级
            T3.ISSUERSMBFLAG, --小微企业标识
            '0', --是否承销债券
            '', --缴款日
            '', --证券类别
            T3.BONDISSUEINTENT, --债券发行目的
            '01', --债权级别
            T3.REABSFLAG, --再资产证券化标识
            T3.ORIGINATORFLAG, --是否发起机构
            T3.ERATING, --证券外部评级
            '', --股票/股指代码
            '', --交易市场
            '', --交易地区
            '0', --是否结构性敞口
            '0', --是否期权基础工具
            '', --期权基础工具名称
            '', --期权工具ID
            '', --波动率
            T3.STARTDATE, --起始日期
            T3.DUEDATE, --到期日期
            T3.ORIGINALMATURITY, --原始期限
            T3.RESIDUALM, --剩余期限
            T3.NEXTREPRICEDATE, --下次重定价日
            T3.NEXTREPRICEM, --下次重定价期限
            T3.RATETYPE, --利率类型
            T3.EXECUTIONRATE, --票面利率
            T3.MODIFIEDDURATION, --修正久期
            CASE
              WHEN T.PAYRECIND = 'P' THEN
               '01' --多头  
              WHEN T.PAYRECIND = 'R' THEN
               '02' --空头
              ELSE
               '01' --多头
            END, --头寸属性
            T1.Bookbalance, --头寸
            T1.CURRENCY, --币种
            '' --期权基础工具类型
       FROM  RWA_ZJ_TRADBONDPOSITION T1 
       INNER JOIN OPI_SWDT T 
         ON T1.POSITIONID = T.DEALNO || T.SEQ
        AND T1.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')
       LEFT JOIN OPI_SWDH H 
         ON T.DEALNO = H.DEALNO
        AND H.DATANO = p_data_dt_str
       LEFT JOIN OPI_CUST C 
         ON H.CNO = C.CNO
        AND C.DATANO = p_data_dt_str
       INNER JOIN OPI_SWDT T 
         ON T1.POSITIONID = T.DEALNO || T.SEQ
        AND T1.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')
       LEFT JOIN RWA.ORG_INFO T2
         ON T1.TRANORGID = T2.ORGID
       LEFT JOIN RWA_ZJ_BONDINFO T3
         ON T1.BONDID = T3.BONDID
        AND T1.DATADATE = T3.DATADATE
      WHERE T.DATANO = p_data_dt_str;
            
            commit;
        */    
            
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZJ_MARKETEXPOSURESTD',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZJ_MARKETEXPOSURESTD;
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
         p_po_rtnmsg  := '标准法风险暴露表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZJ_MARKETEXPOSURESTD;
/

