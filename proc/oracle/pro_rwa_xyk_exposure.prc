CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XYK_EXPOSURE(
                             p_data_dt_str  IN  VARCHAR2,    --数据日期 YYYYMMDD
                             P_PO_RTNCODE  OUT  VARCHAR2,    --返回编号 1 成功,0 失败
                            P_PO_RTNMSG    OUT  VARCHAR2    --返回描述
        )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_XYK_EXPOSURE
    实现功能:信用卡系统-信用风险暴露(从数据源财务系统将业务相关信息全量导入RWA信用卡接口表风险暴露表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2016-04-22
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.CSS_CUSTR|源系统客户基本资料表
    源  表2 :RWA_DEV.CCS_ACCT|源系统人民币贷记帐户表
    源  表3 :RWA_DEV.CCS_ACCA|源系统账户附加记录表
    源  表4 :RWA_DEV.NCM_CUSTOMER_INFO|客户信息表
    源  表5 :RWA_DEV.CCS_CARD|卡片资料表
    目标表  :RWA_DEV.RWA_XYK_EXPOSURE|信用卡系统信用风险暴露表
    变更记录(修改人|修改时间|修改内容):
    XLP 20191016  调整信用加工逻辑
    XLP 20191026  新增信用卡 安居分产品  暴露分类规则  默认 150% 个人住房抵押追加贷款  
    XLP 20191109  调整信用卡客户信息加工方式，数据集市已增加客户号字段  RWA系统无需在通过证件号关联客户号
    XLP 20191120  增加合格表外未使用额度20%逻辑处理
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  V_PRO_NAME VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XYK_EXPOSURE';
  --定义异常变量
  V_RAISE EXCEPTION;
  --定义当前插入的记录数
  V_COUNT INTEGER;

  BEGIN
    --DBMS_OUTPUT.PUT_LINE('【执行 ' || V_PRO_NAME || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));


     /*如果是全量数据加载需清空临时表-账户临时表*/
     EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TEMP_XYK_CARD';

     --插入所有账户下的卡片信息
     INSERT INTO RWA_DEV.RWA_TEMP_XYK_CARD(
                XACCOUNT         --01账号
                ,CLOSE_CODE      --02帐户状态（关帐代码）
                ,CANCL_CODE      --03卡片注销代码
                ,MTHS_ODUE       --04当前逾期期数
               )
     SELECT A.XACCOUNT          --01账号
            ,A.CLOSE_CODE       --02帐户状态（关帐代码）
            ,B.CANCL_CODE       --03卡片注销代码
            ,A.MTHS_ODUE        --04当前逾期期数
     FROM CCS_ACCT A         --人民币贷记帐户
     INNER JOIN CCS_CARD B   --卡片资料表
     ON A.XACCOUNT = B.XACCOUNT
     AND B.DATANO=p_data_dt_str
     WHERE A.DATANO=p_data_dt_str
     ;
     COMMIT;

     DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'RWA_DEV',TABNAME => 'RWA_TEMP_XYK_CARD',CASCADE => TRUE);

     --更新卡片状态
     --逾期                                                                                          
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='202'                                                      
      where mths_odue>0;    
                                                                    
                                                                                                                                                                                                                                                                       
      --问题                                                                                        
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='204'                                                      
      where trim(cancl_code) in('D' ,'F', 'X' ,'BA','B1','V', 'Y','M', 'N','P', 'S', 'K', 'I','H','B','NX','B2','1H','2H','3H','3B','L','O', 'Z','3X','8H','B4', '4H');                                                                       
                                                                                             
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='204'                                                      
      where trim(close_code) in('D' ,'F', 'X' ,'BA', 'C2','1H','2H','3H','H', 'Z','3X', '1R', '4H');   
        
      --正常     
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='203'                                                      
      where trim(cancl_code) is null;                                                                             
                                                                                                        
      --关闭                                                                                            
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='207'                                                      
      where trim(cancl_code)  in ('T', 'C', 'E','Q', 'PQ');  

      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='207'                                                      
      where trim(close_code)  in ('C','Q','PQ');                                                            
                                                                                                        
      --未激活                                                                                           
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='205'                                                      
      where trim(cancl_code)  ='A';                                           
      commit;  
       
        --睡眠                                                                                           
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='206'                                                      
      where trim(cancl_code)  ='U';                                           
      commit; 

       
      --诉讼停计息费
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='208'                                                      
      where trim(close_code) in ('1S');  
      commit;  

      --委外催收
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='209'                                                      
      where trim(close_code) in ('7H','8H');  
      commit; 

       --核销
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='201'                                                      
      where trim(close_code) in ('W1','W2','W','WQ');  
      commit; 


     /*如果是全量数据加载需清空临时表-账户临时表*/
     EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TEMP_XYK_ACCT';

     --插入账户临时表数据 每个账号的最小代码
     INSERT INTO RWA_DEV.RWA_TEMP_XYK_ACCT(
            XACCOUNT,
            CANCL_CODE
            )
      SELECT XACCOUNT, MIN(CANCL_CODE) AS CANCL_CODE
        FROM RWA_TEMP_XYK_CARD
       GROUP BY XACCOUNT
      ;
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='核销'
      WHERE CANCL_CODE='201';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='诉讼停计息费'
      WHERE CANCL_CODE='208';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='逾期'
      WHERE CANCL_CODE='202';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='正常'
      WHERE CANCL_CODE='203';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='问题'
      WHERE CANCL_CODE='204';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE = '未激活' WHERE CANCL_CODE = '205';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='睡眠' WHERE CANCL_CODE='206';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='关闭' WHERE CANCL_CODE='207';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='委外催收' WHERE CANCL_CODE='209';
      COMMIT;

    DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'RWA_DEV',TABNAME => 'RWA_TEMP_XYK_ACCT',CASCADE => TRUE);


    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.TMP_XYK_YE';
    --插入每个账户对应的余额和利息 ADD20190827
    INSERT INTO TMP_XYK_YE(
         XACCOUNT, --账号
         CLOSE_CODE,--帐户状态（关帐代码）
         CRED_LIMIT, --帐户信用额度
         TEMP_LIMIT, --临时额度
         MP_L_LMT,   --大额分期付款额度
         TLMT_BEG,   --临时额度生效日期
         TLMT_END,   --临时额度失效日期
         DK,         --余额
         DKLX,       --利息
         FY          --费用
         )
         SELECT A.XACCOUNT, --账号
                 A.CLOSE_CODE,--帐户状态（关帐代码）
                 nvl(A.CRED_LIMIT, 0) CRED_LIMIT, --帐户信用额度
                 nvl(A.TEMP_LIMIT, 0) TEMP_LIMIT, --临时额度
                 nvl(B.MP_L_LMT, 0) MP_L_LMT,--大额分期付款额度
                 A.TLMT_BEG,--临时额度生效日期
                 A.TLMT_END,--临时额度失效日期
               (NVL(A.BAL_FREE,0)+NVL(A.STM_BALFRE,0)+NVL(A.BAL_INT,0)+NVL(A.STM_BALINT,0) --消费余额（未出账单组成） + 帐单消费余额 + 日记息余额（未出账单组成） + 帐单日记息余额
                + NVL(A.BAL_MP,0)+NVL(A.STM_BALMP,0) -- 分期付款未出帐单余额+分期付款已出帐单余额
                + NVL(A.MP_REM_PPL,0)+ NVL(B.MP_REM_PPL,0)) AS DK, --分期付款目前剩余本金+大额分期剩余本金
               (NVL(BAL_ORINT, 0)+NVL(BAL_CMPINT, 0)+NVL(STM_BALORI, 0)) AS DKLX, --利息 = 利息余额（未出账单组成） + 复利余额 + 帐单利息余额
               (NVL(A.BAL_NOINT, 0)+NVL(A.STM_NOINT,0)) AS FY --费用 = 不记息余额（未出账单组成） + 帐单免息余额 
             FROM CCS_ACCT A
        LEFT JOIN CCS_ACCA B
             ON A.XACCOUNT=B.XACCOUNT
             AND A.DATANO=B.DATANO
           WHERE A.DATANO=p_data_dt_str
            --有溢缴款
           AND  TO_NUMBER(BAL_INTFLAG || TO_CHAR(BAL_INT)) +TO_NUMBER(STMBALINTFLAG || TO_CHAR(STM_BALINT)) >= 0
      UNION ALL
        SELECT A.XACCOUNT,--账号
               A.CLOSE_CODE,--帐户状态（关帐代码）
               nvl(A.CRED_LIMIT, 0) CRED_LIMIT,--帐户信用额度
               nvl(A.TEMP_LIMIT, 0) TEMP_LIMIT,--临时额度
               nvl(B.MP_L_LMT, 0) MP_L_LMT,--大额分期付款额度
               A.TLMT_BEG,--临时额度生效日期
               A.TLMT_END,--临时额度失效日期
               (NVL(A.MP_REM_PPL,0)+NVL(B.MP_REM_PPL,0)) DK,  --贷款     无需再除以100 分期付款目前剩余本金 + 大额分期剩余本金
               (NVL(BAL_ORINT,0)+NVL(BAL_CMPINT,0)+NVL(STM_BALORI,0))  DKLX, --贷款利息  利息余额（未出账单组成） + 复利余额 + 帐单利息余额
               (NVL(A.BAL_NOINT, 0)+NVL(A.STM_NOINT,0)) AS FY --费用 = 不记息余额（未出账单组成） + 帐单免息余额 
          FROM CCS_ACCT A
        LEFT JOIN CCS_ACCA B
        ON A.XACCOUNT=B.XACCOUNT
       AND A.DATANO=B.DATANO
      WHERE A.DATANO=p_data_dt_str
           --无溢缴款
        AND TO_NUMBER(BAL_INTFLAG || TO_CHAR(BAL_INT)) +TO_NUMBER(STMBALINTFLAG || TO_CHAR(STM_BALINT))<0
      ;
      COMMIT;

      --核销账户垫款、利息置0  20191026 调整
      UPDATE TMP_XYK_YE SET DK=0, DKLX = 0, FY = 0 WHERE NVL(TRIM(CLOSE_CODE), 'XXX') IN ('W');
      COMMIT;

     EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.tmp_xyk_ysy';

     --插入已使用额度临时表
     INSERT INTO TMP_XYK_YSY(
              XACCOUNT,--账号
              CANCL_CODE,--卡片注销代码
              ZED,     --总额度
              DK,      --余额（已使用额度）
              DKLX,    --利息
              FY       --费用
     )
     SELECT A.XACCOUNT,
              A.CANCL_CODE,
              B.ZED,
              B.DK,
              B.DKLX,
              B.FY           
         FROM RWA_TEMP_XYK_ACCT A
         LEFT JOIN (
              SELECT A.XACCOUNT,
                     NVL(A.CRED_LIMIT,0)+NVL(A.MP_L_LMT,0) AS ZED, --帐户信用额度 + 大额分期付款额度 = 总额度
                     A.DK,
                     A.DKLX,
                     A.FY
                     FROM TMP_XYK_YE A
              WHERE (A.TLMT_BEG>TO_CHAR(TO_DATE(p_data_dt_str, 'yyyymmdd')-1,'yyyymmdd') OR A.TLMT_END<TO_CHAR(TO_DATE(p_data_dt_str, 'yyyymmdd')-1,'yyyymmdd'))  --临时额度生效日期， 临时额度到期日期
              UNION ALL
              SELECT A.XACCOUNT,
                     NVL(A.TEMP_LIMIT,0)+NVL(A.MP_L_LMT,0) AS ZED, --临时额度 + 大额分期付款额度 = 总额度
                     A.DK,
                     A.DKLX,
                     A.FY
                FROM TMP_XYK_YE A
              WHERE (A.TLMT_BEG<=TO_CHAR(TO_DATE(p_data_dt_str, 'yyyymmdd')-1,'yyyymmdd') AND A.TLMT_END>=TO_CHAR(TO_DATE(p_data_dt_str, 'yyyymmdd')-1,'yyyymmdd')) --临时额度生效日期， 临时额度到期日期
         )B
        ON A.XACCOUNT=B.XACCOUNT ;

        COMMIT;


    
     --清楚信用卡表内五级分类期数
     EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.TMP_XYK_WJFL_VIEW';

     --清楚信用卡表内五级分类期数 
     INSERT INTO RWA_DEV.TMP_XYK_WJFL_VIEW
        (MTHS_ODUE, CUSTR_NBR, DK, DKLX, FY)
      WITH WJFL_VIEW AS (
        SELECT CASE
                 WHEN A.CUSTR_NBR IS NOT NULL OR A.CUSTR_NBR <> '' THEN 0
                 WHEN B.CUSTR_NBR IS NOT NULL OR B.CUSTR_NBR <> '' THEN B.MTHS_ODUE
                 WHEN C.CUSTR_NBR IS NOT NULL OR C.CUSTR_NBR <> '' THEN C.MTHS_ODUE
                 ELSE T3.MTHS_ODUE
               END  MTHS_ODUE,
               T3.CUSTR_NBR ,
               CASE
                 WHEN A.CUSTR_NBR IS NOT NULL OR A.CUSTR_NBR <> '' THEN 0
                 ELSE T1.ZED
               END ZED,
               CASE
                 WHEN A.CUSTR_NBR IS NOT NULL OR A.CUSTR_NBR <> '' THEN 0
                 ELSE T1.DK
               END DK,	
               CASE
                 WHEN A.CUSTR_NBR IS NOT NULL OR A.CUSTR_NBR <> '' THEN 0
                 ELSE T1.DKLX
               END DKLX,
               CASE
                 WHEN A.CUSTR_NBR IS NOT NULL OR A.CUSTR_NBR <> '' THEN 0
                 ELSE T1.FY
               END FY 
        FROM TMP_XYK_YSY T1 
        LEFT JOIN CCS_ACCT T3 
                ON T1.XACCOUNT = T3.XACCOUNT 
               AND T3.DATANO = p_data_dt_str
        LEFT JOIN (
             SELECT DISTINCT A.CUSTR_NBR
              FROM CCS_ACCT A
             WHERE A.DATANO = p_data_dt_str
               AND TRIM(A.CLOSE_CODE) = 'W'  --特殊处理1
        ) A ON T3.CUSTR_NBR = A.CUSTR_NBR 
        LEFT JOIN (
             SELECT 
            CASE
               WHEN MAX(A.MTHS_ODUE) <= 4 THEN 5
               ELSE  MAX(A.MTHS_ODUE) 
             END MTHS_ODUE,
             A.CUSTR_NBR
          FROM CCS_ACCT A
         WHERE A.DATANO = p_data_dt_str
           AND TRIM(A.CLOSE_CODE) = 'F'  --特殊处理2
         GROUP BY  A.CUSTR_NBR   
        ) B ON T3.CUSTR_NBR = B.CUSTR_NBR 
        LEFT JOIN (
             SELECT CASE
               WHEN MAX(A.MTHS_ODUE) <= 3 THEN 4
               ELSE  MAX(A.MTHS_ODUE) 
             END MTHS_ODUE,
             A.CUSTR_NBR
            FROM CCS_ACCT A
           WHERE A.DATANO = p_data_dt_str
             AND TRIM(A.CLOSE_CODE) = 'C2' --特殊处理3
           GROUP BY  A.CUSTR_NBR
        ) C ON T3.CUSTR_NBR = C.CUSTR_NBR 

      )
             --汇总客户项下的期数及金额
             SELECT MAX(MTHS_ODUE) MTHS_ODUE,
                    CUSTR_NBR,
                    SUM(DK) DK,
                    SUM(DKLX) DKLX,
                    SUM(FY) FY
               FROM WJFL_VIEW
              GROUP BY CUSTR_NBR

      ;



    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_XYK_EXPOSURE';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 信用卡系统-信用风险暴露-已使用额度
    INSERT INTO RWA_DEV.RWA_XYK_EXPOSURE(
                DATADATE                        --数据日期
               ,DATANO                          --数据流水号
               ,EXPOSUREID                      --风险暴露ID
               ,DUEID                           --债项ID
               ,SSYSID                          --源系统ID
               ,CONTRACTID                      --合同ID
               ,CLIENTID                        --参与主体ID
               ,SORGID                          --源机构ID
               ,SORGNAME                        --源机构名称
               ,ORGSORTNO                       --所属机构排序号
               ,ORGID                           --所属机构ID
               ,ORGNAME                         --所属机构名称
               ,ACCORGID                        --账务机构ID
               ,ACCORGNAME                      --账务机构名称
               ,INDUSTRYID                      --所属行业代码
               ,INDUSTRYNAME                    --所属行业名称
               ,BUSINESSLINE                    --业务条线
               ,ASSETTYPE                       --资产大类
               ,ASSETSUBTYPE                    --资产小类
               ,BUSINESSTYPEID                  --业务品种代码
               ,BUSINESSTYPENAME                --业务品种名称
               ,CREDITRISKDATATYPE              --信用风险数据类型
               ,ASSETTYPEOFHAIRCUTS             --折扣系数对应资产类别
               ,BUSINESSTYPESTD                 --权重法业务类型
               ,EXPOCLASSSTD                    --权重法暴露大类
               ,EXPOSUBCLASSSTD                 --权重法暴露小类
               ,EXPOCLASSIRB                    --内评法暴露大类
               ,EXPOSUBCLASSIRB                 --内评法暴露小类
               ,EXPOBELONG                      --暴露所属标识
               ,BOOKTYPE                        --账户类别
               ,REGUTRANTYPE                    --监管交易类型
               ,REPOTRANFLAG                    --回购交易标识
               ,REVAFREQUENCY                   --重估频率
               ,CURRENCY                        --币种
               ,NORMALPRINCIPAL                 --正常本金余额
               ,OVERDUEBALANCE                  --逾期余额
               ,NONACCRUALBALANCE               --非应计余额
               ,ONSHEETBALANCE                  --表内余额
               ,NORMALINTEREST                  --正常利息
               ,ONDEBITINTEREST                 --表内欠息
               ,OFFDEBITINTEREST                --表外欠息
               ,EXPENSERECEIVABLE               --应收费用
               ,ASSETBALANCE                    --资产余额
               ,ACCSUBJECT1                     --科目一
               ,ACCSUBJECT2                     --科目二
               ,ACCSUBJECT3                     --科目三
               ,STARTDATE                       --起始日期
               ,DUEDATE                         --到期日期
               ,ORIGINALMATURITY                --原始期限
               ,RESIDUALM                       --剩余期限
               ,RISKCLASSIFY                    --风险分类
               ,EXPOSURESTATUS                  --风险暴露状态
               ,OVERDUEDAYS                     --逾期天数
               ,SPECIALPROVISION                --专项准备金
               ,GENERALPROVISION                --一般准备金
               ,ESPECIALPROVISION               --特别准备金
               ,WRITTENOFFAMOUNT                --已核销金额
               ,OFFEXPOSOURCE                   --表外暴露来源
               ,OFFBUSINESSTYPE                 --表外业务类型
               ,OFFBUSINESSSDVSSTD              --权重法表外业务类型细分
               ,UNCONDCANCELFLAG                --是否可随时无条件撤销
               ,CCFLEVEL                        --信用转换系数级别
               ,CCFAIRB                         --高级法信用转换系数
               ,CLAIMSLEVEL                     --债权级别
               ,BONDFLAG                        --是否为债券
               ,BONDISSUEINTENT                 --债券发行目的
               ,NSUREALPROPERTYFLAG             --是否非自用不动产
               ,REPASSETTERMTYPE                --抵债资产期限类型
               ,DEPENDONFPOBFLAG                --是否依赖于银行未来盈利
               ,IRATING                         --内部评级
               ,PD                              --违约概率
               ,LGDLEVEL                        --违约损失率级别
               ,LGDAIRB                         --高级法违约损失率
               ,MAIRB                           --高级法有效期限
               ,EADAIRB                         --高级法违约风险暴露
               ,DEFAULTFLAG                     --违约标识
               ,BEEL                            --已违约暴露预期损失比率
               ,DEFAULTLGD                      --已违约暴露违约损失率
               ,EQUITYEXPOFLAG                  --股权暴露标识
               ,EQUITYINVESTTYPE                --股权投资对象类型
               ,EQUITYINVESTCAUSE               --股权投资形成原因
               ,SLFLAG                          --专业贷款标识
               ,SLTYPE                          --专业贷款类型
               ,PFPHASE                         --项目融资阶段
               ,REGURATING                      --监管评级
               ,CBRCMPRATINGFLAG                --银监会认定评级是否更为审慎
               ,LARGEFLUCFLAG                   --是否波动性较大
               ,LIQUEXPOFLAG                    --是否清算过程中风险暴露
               ,PAYMENTDEALFLAG                 --是否货款对付模式
               ,DELAYTRADINGDAYS                --延迟交易天数
               ,SECURITIESFLAG                  --有价证券标识
               ,SECUISSUERID                    --证券发行人ID
               ,RATINGDURATIONTYPE              --评级期限类型
               ,SECUISSUERATING                 --证券发行等级
               ,SECURESIDUALM                   --证券剩余期限
               ,SECUREVAFREQUENCY               --证券重估频率
               ,CCPTRANFLAG                     --是否中央交易对手相关交易
               ,CCPID                           --中央交易对手ID
               ,QUALCCPFLAG                     --是否合格中央交易对手
               ,BANKROLE                        --银行角色
               ,CLEARINGMETHOD                  --清算方式
               ,BANKASSETFLAG                   --是否银行提交资产
               ,MATCHCONDITIONS                 --符合条件情况
               ,SFTFLAG                         --证券融资交易标识
               ,MASTERNETAGREEFLAG              --净额结算主协议标识
               ,MASTERNETAGREEID                --净额结算主协议ID
               ,SFTTYPE                         --证券融资交易类型
               ,SECUOWNERTRANSFLAG              --证券所有权是否转移
               ,OTCFLAG                         --场外衍生工具标识
               ,VALIDNETTINGFLAG                --有效净额结算协议标识
               ,VALIDNETAGREEMENTID             --有效净额结算协议ID
               ,OTCTYPE                         --场外衍生工具类型
               ,DEPOSITRISKPERIOD               --保证金风险期间
               ,MTM                             --重置成本
               ,MTMCURRENCY                     --重置成本币种
               ,BUYERORSELLER                   --买方卖方
               ,QUALROFLAG                      --合格参照资产标识
               ,ROISSUERPERFORMFLAG             --参照资产发行人是否能履约
               ,BUYERINSOLVENCYFLAG             --信用保护买方是否破产
               ,NONPAYMENTFEES                  --尚未支付费用
               ,RETAILEXPOFLAG                  --零售暴露标识
               ,RETAILCLAIMTYPE                 --零售债权类型
               ,MORTGAGETYPE                    --住房抵押贷款类型
               ,EXPONUMBER                      --风险暴露个数
               ,LTV                             --贷款价值比
               ,AGING                           --账龄
               ,NEWDEFAULTDEBTFLAG              --新增违约债项标识
               ,PDPOOLMODELID                   --PD分池模型ID
               ,LGDPOOLMODELID                  --LGD分池模型ID
               ,CCFPOOLMODELID                  --CCF分池模型ID
               ,PDPOOLID                        --所属PD池ID
               ,LGDPOOLID                       --所属LGD池ID
               ,CCFPOOLID                       --所属CCF池ID
               ,ABSUAFLAG                       --资产证券化基础资产标识
               ,ABSPOOLID                       --证券化资产池ID
               ,GROUPID                         --分组编号
               ,DEFAULTDATE                     --违约时点
               ,ABSPROPORTION                   --资产证券化比重
               ,DEBTORNUMBER                    --借款人个数
               ,CLIENTNAME                      --客户名称
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,T1.DATANO                                   AS DATANO                   --数据流水号
                ,T1.XACCOUNT                                 AS EXPOSUREID               --风险暴露ID
                ,T1.XACCOUNT                                 AS DUEID                    --债项ID
                ,'XYK'                                       AS SSYSID                   --源系统代号                默认：信用卡(XYK)
                ,T1.XACCOUNT                                 AS CONTRACTID               --合同ID
                ,NVL(T1.CUST_NO, T1.CUSTR_NBR)             AS CLIENTID                 --参与主体ID                关联不上用账号做客户号
                ,'9997'                                  AS SORGID                   --源机构ID
                ,'重庆银行股份有限公司信用卡中心'                               AS SORGNAME                  --源机构名称
                ,'1600'                                   AS ORGSORTNO                --所属机构排序号
                ,'9997'                                  AS ORGID                    --所属机构代码
                ,'重庆银行股份有限公司信用卡中心'                               AS ORGNAME                   --所属机构名称
                ,'9997'                                  AS ACCORGID                 --账务机构ID
                ,'重庆银行股份有限公司信用卡中心'                               AS ACCORGNAME               --账务机构名称
                ,'999999'                                    AS INDUSTRYID               --所属行业代码             默认：空
                ,'未知'                                      AS INDUSTRYNAME             --所属行业名称             默认：空
                ,'0301'                                      AS BUSINESSLINE             --条线                     默认：零售条线(2)
                ,'115'                                       AS ASSETTYPE                --资产大类                  默认
                ,'11501'                                     AS ASSETSUBTYPE             --资产小类                  默认
                ,'11106010'                                  AS BUSINESSTYPEID           --业务品种代码              信用卡业务
                ,'信用卡垫款'                                 AS BUSINESSTYPENAME         --业务品种名称              固定值：信用卡
                ,'02'                                        AS CREDITRISKDATATYPE       --信用风险数据类型          默认：一般零售(02)
                ,'01'                                        AS ASSETTYPEOFHAIRCUTS      --折扣系数对应资产类别     默认: 现金及现金等价物(01)
                ,'04'                                        AS BUSINESSTYPESTD          --权重法业务类型           固定值：个人
                ,'0108'                                      AS EXPOCLASSSTD              --权重法暴露大类           固定值：对个人债权
                ,CASE 
                   WHEN T1.CATEGORY = '40' THEN  '010802' --信用卡账户类别 40 为 安居分产品  直接按 010802	个人住房抵押追加贷款  150%风险权重计量
                   ELSE '010803'  --010803	对个人其他债权 75%计量
                 END                                         AS EXPOSUBCLASSSTD          --权重法暴露小类          
                ,''                                          AS EXPOCLASSIRB              --内评法暴露大类           固定值：零售风险暴露
                ,''                                          AS EXPOSUBCLASSIRB          --内评法暴露小类           固定值：合格循环风险暴露
                ,'01'                                        AS EXPOBELONG                --暴露所属标识              固定值：表内(01)
                ,'01'                                        AS BOOKTYPE                  --账户类别                  固定值：01-银行账户
                ,'03'                                        AS REGUTRANTYPE             --监管交易类型              固定值：抵押贷款(03)
                ,'0'                                         AS REPOTRANFLAG             --回购交易标识               固定值：否(0)
                ,1                                           AS REVAFREQUENCY            --重估频率                  默认： 1
                ,'CNY'                                       AS CURRENCY                 --币种                         固定值：人民币
                , YSY.DK                  AS NORMALPRINCIPAL          --正常本金余额             当前余额
                ,0                                           AS OVERDUEBALANCE            --逾期余额                  默认：0
                ,0                                           AS NONACCRUALBALANCE        --非应计余额                默认：0
                ,YSY.DK                   AS ONSHEETBALANCE           --表内余额                   表内余额=正常本金余额
                ,0                                            AS NORMALINTEREST           --正常利息                  默认：0
                ,0                                            AS ONDEBITINTEREST          --表内欠息                  帐单利息余额+利息昨日余额  NVL(T1.STM_BALORI,0)+NVL(T1.BAL_ORINT,0)
                ,0                                            AS OFFDEBITINTEREST         --表外欠息 NVL(T1.BAL_CMPINT,0)
                ,0                                            AS EXPENSERECEIVABLE        --应收费用                  不记息余额+账单免息余额 NVL(T1.BAL_NOINT,0)+NVL(T1.STM_NOINT,0)
                ,YSY.DK                   AS ASSETBALANCE             --资产余额                 表内资产总余额=表内余额+应收费用+正常利息+表内欠息
                ,CASE WHEN T2.MTHS_ODUE=0
                           THEN '130704'	--垫款-信用卡垫款

                      WHEN T2.MTHS_ODUE>0 AND T2.MTHS_ODUE<=3
                           THEN '131001'	--逾期贷款-信用卡垫款逾期                       
                      WHEN T2.MTHS_ODUE>3
                           THEN '131001'	--逾期贷款-信用卡垫款逾期                       
                    ELSE '130704'	--垫款-信用卡垫款
                END                                          AS ACCSUBJECT1               --科目一
                ,''                                           AS ACCSUBJECT2               --科目二
                ,''                                            AS ACCSUBJECT3               --科目三
                ,T1.DAY_OPENED                                AS STARTDATE                 --起始日期
                ,p_data_dt_str                                AS DUEDATE                    --到期日期
                ,0                                            AS ORIGINALMATURITY         --原始期限
                ,0                                           AS RESIDUALM                 --剩余期限
                ,CASE WHEN T2.MTHS_ODUE=0 THEN '01'
                      WHEN T2.MTHS_ODUE>=1 AND T2.MTHS_ODUE<=3 THEN '02'
                      WHEN T2.MTHS_ODUE=4 THEN '03'
                      WHEN T2.MTHS_ODUE>=5 AND T2.MTHS_ODUE<=6 THEN '04'
                      ELSE '05'
                      END                                     AS RISKCLASSIFY             --风险分类                  根据逾期期数判断，具体规则如下：0、正常；1-3、关注；4、次级；5-6、可疑；>=7、损失
                ,CASE WHEN T2.MTHS_ODUE=0 THEN '01'
                      WHEN T2.MTHS_ODUE>=1 AND T2.MTHS_ODUE<=3 THEN '02'
                      ELSE '03'
                END                                          AS EXPOSURESTATUS           --风险暴露状态             根据逾期期数判断，具体规则如下：0、正常；1-3、逾期；>=4、其他
                ,T2.MTHS_ODUE                                AS OVERDUEDAYS              --逾期天数
                ,0                                           AS SPECIALPROVISION         --专项准备金               RWA计算
                ,0                                           AS GENERALPROVISION         --一般准备金               RWA计算
                ,0                                           AS ESPECIALPROVISION        --特别准备金               RWA计算
                ,0                                           AS WRITTENOFFAMOUNT         --已核销金额               默认：0
                ,''                                          AS OFFEXPOSOURCE            --表外暴露来源             默认：空
                ,''                                          AS OFFBUSINESSTYPE          --表外业务类型             默认：空
                ,''                                          AS OFFBUSINESSSDVSSTD       --权重法表外业务类型细分   默认：空
                ,'0'                                         AS UNCONDCANCELFLAG         --是否可随时无条件撤销     默认：'否'
                ,''                                          AS CCFLEVEL                 --信用转换系数级别         默认：空
                ,T4.CCFVALUE                                 AS CCFAIRB                  --高级法信用转换系数       默认：空
                ,'01'                                        AS CLAIMSLEVEL              --债权级别                 默认：高级债权(01)
                ,'0'                                         AS BONDFLAG                 --是否为债券               默认：否(0)
                ,'02'                                        AS BONDISSUEINTENT          --债券发行目的             默认：否(0)
                ,'0'                                         AS NSUREALPROPERTYFLAG      --是否非自用不动产         默认：否(0)
                ,''                                          AS REPASSETTERMTYPE         --抵债资产期限类型         默认：空
                ,'0'                                         AS DEPENDONFPOBFLAG         --是否依赖于银行未来盈利   默认：否(0)
                ,''                                          AS IRATING                  --内部评级                 待定
                ,T4.PDVALUE                                  AS PD                       --违约概率                 待定
                ,''                                          AS LGDLEVEL                 --违约损失率级别           待定
                ,T4.LGDVALUE                                 AS LGDAIRB                  --高级法违约损失率         待定
                ,NULL                                        AS MAIRB                    --高级法有效期限           待定
                ,YSY.DK                                        AS EADAIRB                  --高级法违约风险暴露
                ,CASE WHEN T4.DEFAULTFLAG='1' THEN '1'
                      ELSE '0'
                 END                                         AS DEFAULTFLAG              --违约标识
                ,NVL(T4.BEELVALUE,0)                                AS BEEL                     --已违约暴露预期损失比率
                ,NVL(T4.LGDVALUE,0)                          AS DEFAULTLGD               --已违约暴露违约损失率
                ,'0'                                         AS EQUITYEXPOFLAG           --股权暴露标识
                ,''                                          AS EQUITYINVESTTYPE         --股权投资对象类型
                ,''                                          AS EQUITYINVESTCAUSE        --股权投资形成原因
                ,'1'                                         AS SLFLAG                   --专业贷款标识
                ,'02030302'                                  AS SLTYPE                   --专业贷款类型
                ,''                                          AS PFPHASE                   --项目融资阶段
                ,'01'                                        AS REGURATING               --监管评级
                ,'0'                                         AS CBRCMPRATINGFLAG         --银监会认定评级是否更为审慎
                ,'0'                                         AS LARGEFLUCFLAG            --是否波动性较大
                ,'0'                                         AS LIQUEXPOFLAG             --是否清算过程中风险暴露
                ,'0'                                         AS PAYMENTDEALFLAG          --是否货款对付模式
                ,0                                           AS DELAYTRADINGDAYS         --延迟交易天数
                ,'0'                                         AS SECURITIESFLAG           --有价证券标识
                ,''                                          AS SECUISSUERID             --证券发行人ID
                ,''                                          AS RATINGDURATIONTYPE       --评级期限类型
                ,''                                          AS SECUISSUERATING          --证券发行等级
                ,0                                           AS SECURESIDUALM            --证券剩余期限
                ,1                                           AS SECUREVAFREQUENCY        --证券重估频率
                ,'0'                                         AS CCPTRANFLAG              --是否中央交易对手相关交易
                ,''                                          AS CCPID                    --中央交易对手ID
                ,'0'                                         AS QUALCCPFLAG              --是否合格中央交易对手
                ,'02'                                        AS BANKROLE                 --银行角色
                ,'02'                                        AS CLEARINGMETHOD           --清算方式
                ,'0'                                         AS BANKASSETFLAG            --是否银行提交资产
                ,'01'                                        AS MATCHCONDITIONS          --符合条件情况
                ,'0'                                         AS SFTFLAG                  --证券融资交易标识
                ,'0'                                         AS MASTERNETAGREEFLAG       --净额结算主协议标识
                ,''                                          AS MASTERNETAGREEID         --净额结算主协议ID
                ,'01'                                        AS SFTTYPE                  --证券融资交易类型
                ,'0'                                         AS SECUOWNERTRANSFLAG       --证券所有权是否转移
                ,'0'                                         AS OTCFLAG                  --场外衍生工具标识
                ,'0'                                         AS VALIDNETTINGFLAG         --有效净额结算协议标识
                ,''                                          AS VALIDNETAGREEMENTID      --有效净额结算协议ID
                ,'01'                                        AS OTCTYPE                  --场外衍生工具类型
                ,0                                           AS DEPOSITRISKPERIOD        --保证金风险期间
                ,0                                           AS MTM                      --重置成本
                ,'01'                                        AS MTMCURRENCY              --重置成本币种
                ,'01'                                        AS BUYERORSELLER            --买方卖方
                ,'0'                                         AS QUALROFLAG               --合格参照资产标识
                ,'0'                                         AS ROISSUERPERFORMFLAG      --参照资产发行人是否能履约
                ,'0'                                         AS BUYERINSOLVENCYFLAG      --信用保护买方是否破产
                ,0                                           AS NONPAYMENTFEES           --尚未支付费用
                ,'1'                                         AS RETAILEXPOFLAG           --零售暴露标识             默认：是(1)
                ,'020403'                                    AS RETAILCLAIMTYPE          --零售债权类型             默认：空
                ,'0'                                         AS MORTGAGETYPE             --住房抵押贷款类型         默认：否(0)
                ,1                                           AS EXPONUMBER               --风险暴露个数             默认：空
                ,0.8                                         AS LTV                     --贷款价值比  统一更新
                ,T4.MOB                                      AS AGING                   --账龄
                ,T1.NEWDEFAULTFLAG                           AS NEWDEFAULTDEBTFLAG      --新增违约债项标识
                ,T4.PDMODELCODE                              AS PDPOOLMODELID           -- PD分池模型ID
                ,T4.LGDMODELCODE                             AS LGDPOOLMODELID          -- LGD分池模型ID
                ,T4.CCFMODELCODE                             AS CCFPOOLMODELID          -- CCF分池模型ID
                ,T4.PDCODE                                   AS PDPOOLID                -- 所属PD池ID
                ,T4.LGDCODE                                  AS LGDPOOLID               -- 所属LGD池ID
                ,T4.CCFCODE                                  AS CCFPOOLID               -- 所属CCF池ID
                ,'0'                                         AS ABSUAFLAG                --资产证券化基础资产标识   默认：否(0)
                ,''                                          AS ABSPOOLID                --证券化资产池ID           默认：空
                ,''                                          AS GROUPID                  --分组编号                 RWA系统赋值
                ,CASE WHEN T4.DEFAULTFLAG='1' THEN TO_DATE(T4.UPDATETIME,'YYYYMMDD')
                      ELSE NULL
                 END                                          AS DEFAULTDATE              --违约时点
                ,NULL                                        AS ABSPROPORTION            --资产证券化比重
                ,NULL                                        AS DEBTORNUMBER             --借款人个数
                ,''                             AS CLIENTNAME               --客户名称
            FROM        RWA_DEV.TMP_XYK_YSY YSY --已使用临时表
            LEFT JOIN   RWA_DEV.CCS_ACCT T1  --人民币贷记帐户
                 ON     YSY.XACCOUNT = T1.XACCOUNT
                 AND    T1.DATANO= p_data_dt_str
            LEFT JOIN   RWA_DEV.RWA_TEMP_LGDLEVEL T4
                 ON     T1.XACCOUNT=T4.BUSINESSID
            AND         T4.BUSINESSTYPE='CREDITCARD'
            LEFT JOIN   RWA_DEV.TMP_XYK_WJFL_VIEW T2
                   ON   T1.CUSTR_NBR = T2.CUSTR_NBR
            WHERE YSY.DK > 0 --只计量余额大于零
            ;
            
     COMMIT;




    --2.1 信用卡系统-信用风险暴露-未使用额度
    INSERT INTO RWA_DEV.RWA_XYK_EXPOSURE(
                DATADATE                        --数据日期
               ,DATANO                          --数据流水号
               ,EXPOSUREID                      --风险暴露ID
               ,DUEID                           --债项ID
               ,SSYSID                          --源系统ID
               ,CONTRACTID                      --合同ID
               ,CLIENTID                        --参与主体ID
               ,SORGID                          --源机构ID
               ,SORGNAME                        --源机构名称
               ,ORGSORTNO                       --所属机构排序号
               ,ORGID                           --所属机构ID
               ,ORGNAME                         --所属机构名称
               ,ACCORGID                        --账务机构ID
               ,ACCORGNAME                      --账务机构名称
               ,INDUSTRYID                      --所属行业代码
               ,INDUSTRYNAME                    --所属行业名称
               ,BUSINESSLINE                    --业务条线
               ,ASSETTYPE                       --资产大类
               ,ASSETSUBTYPE                    --资产小类
               ,BUSINESSTYPEID                  --业务品种代码
               ,BUSINESSTYPENAME                --业务品种名称
               ,CREDITRISKDATATYPE              --信用风险数据类型
               ,ASSETTYPEOFHAIRCUTS             --折扣系数对应资产类别
               ,BUSINESSTYPESTD                 --权重法业务类型
               ,EXPOCLASSSTD                    --权重法暴露大类
               ,EXPOSUBCLASSSTD                 --权重法暴露小类
               ,EXPOCLASSIRB                    --内评法暴露大类
               ,EXPOSUBCLASSIRB                 --内评法暴露小类
               ,EXPOBELONG                      --暴露所属标识
               ,BOOKTYPE                        --账户类别
               ,REGUTRANTYPE                    --监管交易类型
               ,REPOTRANFLAG                    --回购交易标识
               ,REVAFREQUENCY                   --重估频率
               ,CURRENCY                        --币种
               ,NORMALPRINCIPAL                 --正常本金余额
               ,OVERDUEBALANCE                  --逾期余额
               ,NONACCRUALBALANCE               --非应计余额
               ,ONSHEETBALANCE                  --表内余额
               ,NORMALINTEREST                  --正常利息
               ,ONDEBITINTEREST                 --表内欠息
               ,OFFDEBITINTEREST                --表外欠息
               ,EXPENSERECEIVABLE               --应收费用
               ,ASSETBALANCE                    --资产余额
               ,ACCSUBJECT1                     --科目一
               ,ACCSUBJECT2                     --科目二
               ,ACCSUBJECT3                     --科目三
               ,STARTDATE                       --起始日期
               ,DUEDATE                         --到期日期
               ,ORIGINALMATURITY                --原始期限
               ,RESIDUALM                       --剩余期限
               ,RISKCLASSIFY                    --风险分类
               ,EXPOSURESTATUS                  --风险暴露状态
               ,OVERDUEDAYS                     --逾期天数
               ,SPECIALPROVISION                --专项准备金
               ,GENERALPROVISION                --一般准备金
               ,ESPECIALPROVISION               --特别准备金
               ,WRITTENOFFAMOUNT                --已核销金额
               ,OFFEXPOSOURCE                   --表外暴露来源
               ,OFFBUSINESSTYPE                 --表外业务类型
               ,OFFBUSINESSSDVSSTD              --权重法表外业务类型细分
               ,UNCONDCANCELFLAG                --是否可随时无条件撤销
               ,CCFLEVEL                        --信用转换系数级别
               ,CCFAIRB                         --高级法信用转换系数
               ,CLAIMSLEVEL                     --债权级别
               ,BONDFLAG                        --是否为债券
               ,BONDISSUEINTENT                 --债券发行目的
               ,NSUREALPROPERTYFLAG             --是否非自用不动产
               ,REPASSETTERMTYPE                --抵债资产期限类型
               ,DEPENDONFPOBFLAG                --是否依赖于银行未来盈利
               ,IRATING                         --内部评级
               ,PD                              --违约概率
               ,LGDLEVEL                        --违约损失率级别
               ,LGDAIRB                         --高级法违约损失率
               ,MAIRB                           --高级法有效期限
               ,EADAIRB                         --高级法违约风险暴露
               ,DEFAULTFLAG                     --违约标识
               ,BEEL                            --已违约暴露预期损失比率
               ,DEFAULTLGD                      --已违约暴露违约损失率
               ,EQUITYEXPOFLAG                  --股权暴露标识
               ,EQUITYINVESTTYPE                --股权投资对象类型
               ,EQUITYINVESTCAUSE               --股权投资形成原因
               ,SLFLAG                          --专业贷款标识
               ,SLTYPE                          --专业贷款类型
               ,PFPHASE                         --项目融资阶段
               ,REGURATING                      --监管评级
               ,CBRCMPRATINGFLAG                --银监会认定评级是否更为审慎
               ,LARGEFLUCFLAG                   --是否波动性较大
               ,LIQUEXPOFLAG                    --是否清算过程中风险暴露
               ,PAYMENTDEALFLAG                 --是否货款对付模式
               ,DELAYTRADINGDAYS                --延迟交易天数
               ,SECURITIESFLAG                  --有价证券标识
               ,SECUISSUERID                    --证券发行人ID
               ,RATINGDURATIONTYPE              --评级期限类型
               ,SECUISSUERATING                 --证券发行等级
               ,SECURESIDUALM                   --证券剩余期限
               ,SECUREVAFREQUENCY               --证券重估频率
               ,CCPTRANFLAG                     --是否中央交易对手相关交易
               ,CCPID                           --中央交易对手ID
               ,QUALCCPFLAG                     --是否合格中央交易对手
               ,BANKROLE                        --银行角色
               ,CLEARINGMETHOD                  --清算方式
               ,BANKASSETFLAG                   --是否银行提交资产
               ,MATCHCONDITIONS                 --符合条件情况
               ,SFTFLAG                         --证券融资交易标识
               ,MASTERNETAGREEFLAG              --净额结算主协议标识
               ,MASTERNETAGREEID                --净额结算主协议ID
               ,SFTTYPE                         --证券融资交易类型
               ,SECUOWNERTRANSFLAG              --证券所有权是否转移
               ,OTCFLAG                         --场外衍生工具标识
               ,VALIDNETTINGFLAG                --有效净额结算协议标识
               ,VALIDNETAGREEMENTID             --有效净额结算协议ID
               ,OTCTYPE                         --场外衍生工具类型
               ,DEPOSITRISKPERIOD               --保证金风险期间
               ,MTM                             --重置成本
               ,MTMCURRENCY                     --重置成本币种
               ,BUYERORSELLER                   --买方卖方
               ,QUALROFLAG                      --合格参照资产标识
               ,ROISSUERPERFORMFLAG             --参照资产发行人是否能履约
               ,BUYERINSOLVENCYFLAG             --信用保护买方是否破产
               ,NONPAYMENTFEES                  --尚未支付费用
               ,RETAILEXPOFLAG                  --零售暴露标识
               ,RETAILCLAIMTYPE                 --零售债权类型
               ,MORTGAGETYPE                    --住房抵押贷款类型
               ,EXPONUMBER                      --风险暴露个数
               ,LTV                             --贷款价值比
               ,AGING                           --账龄
               ,NEWDEFAULTDEBTFLAG              --新增违约债项标识
               ,PDPOOLMODELID                   --PD分池模型ID
               ,LGDPOOLMODELID                  --LGD分池模型ID
               ,CCFPOOLMODELID                  --CCF分池模型ID
               ,PDPOOLID                        --所属PD池ID
               ,LGDPOOLID                       --所属LGD池ID
               ,CCFPOOLID                       --所属CCF池ID
               ,ABSUAFLAG                       --资产证券化基础资产标识
               ,ABSPOOLID                       --证券化资产池ID
               ,GROUPID                         --分组编号
               ,DEFAULTDATE                     --违约时点
               ,ABSPROPORTION                   --资产证券化比重
               ,DEBTORNUMBER                    --借款人个数
               ,CLIENTNAME                      --客户名称
    )
   WITH TMPE_HGFLAG AS (
          --4.3.1符合标准的未使用额度
          --1.  账户类别=“个人卡”
          --2.  担保标识=“无担保”
          --注：监管要求满足以下条件可列入此列：（1）授信对象为自然人，授信方式为无担保循环授信
          --                                    （2）对同一持卡人的授信额度不超过100万人民币
          SELECT T1.XACCOUNT, --账号
                 T5.FLAG1,    --是否担保
                 T5.FLAG2,    --是否循环
                 T4.ZED       --客户总授信额度  以客户号为中心汇总
          FROM TMP_XYK_YSY T1
          LEFT JOIN CCS_ACCT T3
            ON T1.XACCOUNT = T3.XACCOUNT
           AND T3.DATANO = p_data_dt_str
          LEFT JOIN (               
                     SELECT SUM(T1.ZED) ZED, T2.DOCUMENT_ID
                       FROM RWA_DEV.TMP_XYK_YSY T1
                       LEFT JOIN RWA_DEV.CCS_GUAR T2
                         ON T1.XACCOUNT = T2.ACCT_NO
                      WHERE T2.DATANO = p_data_dt_str
                      GROUP BY T2.DOCUMENT_ID
                     
                     ) T4 --额度使用情况
            ON T3.CUSTR_NBR = T4.DOCUMENT_ID
          LEFT JOIN RWA_DEV.CCS_GUAR T5
            ON T1.XACCOUNT = T5.ACCT_NO
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,T1.DATANO                                   AS DATANO                   --数据流水号
                ,'BW_'||T1.XACCOUNT                          AS EXPOSUREID               --风险暴露ID
                ,T1.XACCOUNT                              AS DUEID                    --债项ID
                ,'XYK'                                       AS SSYSID                   --源系统代号                默认：信用卡(XYK)
                ,'BW_'||T1.XACCOUNT                          AS CONTRACTID               --合同ID
                ,NVL(T1.CUST_NO, T1.CUSTR_NBR)             AS CLIENTID                 --参与主体ID              关联不上的用账号做客户号
                ,'9997'                                  AS SORGID                    --源机构ID
                ,'重庆银行股份有限公司信用卡中心'        AS SORGNAME                 --源机构名称
                ,'1600'                                   AS ORGSORTNO                --所属机构排序号
                ,'9997'                                  AS ORGID                    --所属机构代码
                ,'重庆银行股份有限公司信用卡中心'        AS ORGNAME                  --所属机构名称
                ,'9997'                                  AS ACCORGID                 --账务机构ID
                ,'重庆银行股份有限公司信用卡中心'        AS ACCORGNAME               --账务机构名称
                ,'999999'                                          AS INDUSTRYID               --所属行业代码             默认：空
                ,'未知'                                          AS INDUSTRYNAME             --所属行业名称             默认：空
                ,'0301'                                      AS BUSINESSLINE             --条线                     默认：零售条线(2)
                ,'215'                                       AS ASSETTYPE                --资产大类
                ,'21503'                                     AS ASSETSUBTYPE             --资产小类
                ,'11106020'                                  AS BUSINESSTYPEID           --业务品种代码              信用卡业务
                ,'信用卡垫款_未使用额度'                      AS BUSINESSTYPENAME         --业务品种名称              固定值：信用卡
                ,'02'                                        AS CREDITRISKDATATYPE       --信用风险数据类型          默认：一般零售(02)
                ,'01'                                        AS ASSETTYPEOFHAIRCUTS      --折扣系数对应资产类别     默认: 现金及现金等价物(01)
                ,'04'                                        AS BUSINESSTYPESTD          --权重法业务类型           固定值：个人
                ,'0108'                                      AS EXPOCLASSSTD              --权重法暴露大类           固定值：对个人债权
                ,'010803'                                    AS EXPOSUBCLASSSTD          --权重法暴露小类           固定值：对个人其他债权
                ,''                                          AS EXPOCLASSIRB              --内评法暴露大类           固定值：零售风险暴露
                ,''                                          AS EXPOSUBCLASSIRB          --内评法暴露小类           固定值：合格循环风险暴露
                ,'02'                                        AS EXPOBELONG                --暴露所属标识              固定值：表外(02)
                ,'01'                                        AS BOOKTYPE                  --账户类别                  固定值：01-银行账户
                ,'03'                                        AS REGUTRANTYPE             --监管交易类型              固定值：抵押贷款(03)
                ,'0'                                         AS REPOTRANFLAG             --回购交易标识               固定值：否(0)
                ,1                                           AS REVAFREQUENCY            --重估频率                  默认： 1
                ,'CNY'                                       AS CURRENCY                 --币种                         固定值：人民币
                ,WSY.WSYED                                   AS NORMALPRINCIPAL          --正常本金余额             当前余额
                ,0                                           AS OVERDUEBALANCE            --逾期余额                  默认：0
                ,0                                           AS NONACCRUALBALANCE        --非应计余额                默认：0
                ,WSY.WSYED                                       AS ONSHEETBALANCE           --表内余额                   表内余额=正常本金余额+逾期余额+非应计余额
                ,0                                            AS NORMALINTEREST           --正常利息                  默认：0
                ,0                                           AS ONDEBITINTEREST          --表内欠息                  帐单利息余额+利息昨日余额
                ,0                                           AS OFFDEBITINTEREST         --表外欠息                  复利余额
                ,0                                           AS EXPENSERECEIVABLE        --应收费用                  默认：0
                ,WSY.WSYED                                       AS ASSETBALANCE             --资产余额                 表内资产总余额=表内余额+应收费用+正常利息+表内欠息
                ,'71190000'                                  AS ACCSUBJECT1               --科目一
                ,''                                           AS ACCSUBJECT2               --科目二
                ,''                                            AS ACCSUBJECT3               --科目三
                ,T1.DAY_OPENED                                AS STARTDATE                 --起始日期
                ,p_data_dt_str                                AS DUEDATE                    --到期日期
                ,0                                            AS ORIGINALMATURITY         --原始期限
                ,0                                           AS RESIDUALM                 --剩余期限
                ,CASE WHEN T3.MTHS_ODUE=0 THEN '01'
                      WHEN T3.MTHS_ODUE>=1 AND T3.MTHS_ODUE<=3 THEN '02'
                      WHEN T3.MTHS_ODUE=4 THEN '03'
                      WHEN T3.MTHS_ODUE>=5 AND T3.MTHS_ODUE<=6 THEN '04'
                      ELSE '05'
                      END                                     AS RISKCLASSIFY             --风险分类                  根据逾期期数判断，具体规则如下：0、正常；1-3、关注；4、次级；5-6、可疑；>=7、损失
                ,CASE WHEN T3.MTHS_ODUE=0 THEN '01'
                      WHEN T3.MTHS_ODUE>=1 AND T3.MTHS_ODUE<=3 THEN '02'
                      ELSE '03'
                END                                          AS EXPOSURESTATUS           --风险暴露状态              根据逾期期数判断，具体规则如下：0、正常；1-3、逾期；>=4、其他
                ,T3.MTHS_ODUE                                AS OVERDUEDAYS              --逾期天数
                ,0                                           AS SPECIALPROVISION         --专项准备金               RWA计算
                ,0                                           AS GENERALPROVISION         --一般准备金               RWA计算
                ,0                                           AS ESPECIALPROVISION        --特别准备金               RWA计算
                ,0                                           AS WRITTENOFFAMOUNT         --已核销金额               默认：0
                ,'01'                                        AS OFFEXPOSOURCE            --表外暴露来源             默认：空
                ,CASE WHEN T2.FLAG1 = '0' AND T2.FLAG2 = '1' AND T2.ZED < 1000000 THEN '04'  --04  符合标准的未使用的信用卡授信额度
                      ELSE '03'  --03 一般未使用的信用卡授信额度
                 END                                         AS OFFBUSINESSTYPE          --表外业务类型
                ,CASE WHEN T2.FLAG1 = '0' AND T2.FLAG2 = '1' AND T2.ZED < 1000000 THEN '0401' --0401  符合标准的未使用的信用卡授信额度
                      ELSE '0301'  --0301  其他信用卡授信额度
                 END                                         AS OFFBUSINESSSDVSSTD       --权重法表外业务类型细分
                ,'0'                                         AS UNCONDCANCELFLAG         --是否可随时无条件撤销     默认：'否'
                ,''                                          AS CCFLEVEL                 --信用转换系数级别         默认：空
                ,T5.CCFVALUE                                 AS CCFAIRB                  --高级法信用转换系数       默认：空
                ,'01'                                        AS CLAIMSLEVEL              --债权级别                 默认：高级债权(01)
                ,'0'                                         AS BONDFLAG                 --是否为债券               默认：否(0)
                ,'0'                                         AS BONDISSUEINTENT          --债券发行目的             默认：否(0)
                ,'0'                                         AS NSUREALPROPERTYFLAG      --是否非自用不动产         默认：否(0)
                ,''                                          AS REPASSETTERMTYPE         --抵债资产期限类型         默认：空
                ,'0'                                         AS DEPENDONFPOBFLAG         --是否依赖于银行未来盈利   默认：否(0)
                ,''                                          AS IRATING                  --内部评级                 待定
                ,T5.PDVALUE                                  AS PD                       --违约概率                 待定
                ,''                                          AS LGDLEVEL                 --违约损失率级别           待定
                ,T5.LGDVALUE                                 AS LGDAIRB                  --高级法违约损失率         待定
                ,NULL                                        AS MAIRB                    --高级法有效期限           待定
                ,WSY.WSYED*NVL(T5.CCFVALUE,1)                   AS EADAIRB                  --高级法违约风险暴露
                ,CASE WHEN T5.DEFAULTFLAG='1' THEN '1'
                      ELSE '0'
                 END                                         AS DEFAULTFLAG              --违约标识                 待定
                ,NVL(T5.BEELVALUE,0)                                AS BEEL                     --已违约暴露预期损失比率   待定
                ,NVL(T5.LGDVALUE,0)                          AS DEFAULTLGD               --已违约暴露违约损失率     待定
                ,'0'                                         AS EQUITYEXPOFLAG           --股权暴露标识
                ,''                                          AS EQUITYINVESTTYPE         --股权投资对象类型
                ,''                                          AS EQUITYINVESTCAUSE        --股权投资形成原因
                ,'1'                                         AS SLFLAG                   --专业贷款标识
                ,'02030302'                                  AS SLTYPE                   --专业贷款类型
                ,''                                          AS PFPHASE                   --项目融资阶段
                ,'01'                                        AS REGURATING               --监管评级
                ,'0'                                         AS CBRCMPRATINGFLAG         --银监会认定评级是否更为审慎
                ,'0'                                         AS LARGEFLUCFLAG            --是否波动性较大
                ,'0'                                         AS LIQUEXPOFLAG             --是否清算过程中风险暴露
                ,'0'                                         AS PAYMENTDEALFLAG          --是否货款对付模式
                ,0                                           AS DELAYTRADINGDAYS         --延迟交易天数
                ,'0'                                         AS SECURITIESFLAG           --有价证券标识
                ,''                                          AS SECUISSUERID             --证券发行人ID
                ,''                                          AS RATINGDURATIONTYPE       --评级期限类型
                ,''                                          AS SECUISSUERATING          --证券发行等级
                ,0                                           AS SECURESIDUALM            --证券剩余期限
                ,1                                           AS SECUREVAFREQUENCY        --证券重估频率
                ,'0'                                         AS CCPTRANFLAG              --是否中央交易对手相关交易
                ,''                                          AS CCPID                    --中央交易对手ID
                ,'0'                                         AS QUALCCPFLAG              --是否合格中央交易对手
                ,'02'                                        AS BANKROLE                 --银行角色
                ,'02'                                        AS CLEARINGMETHOD           --清算方式
                ,'0'                                         AS BANKASSETFLAG            --是否银行提交资产
                ,'01'                                        AS MATCHCONDITIONS          --符合条件情况
                ,'0'                                         AS SFTFLAG                  --证券融资交易标识
                ,'0'                                         AS MASTERNETAGREEFLAG       --净额结算主协议标识
                ,''                                          AS MASTERNETAGREEID         --净额结算主协议ID
                ,'01'                                        AS SFTTYPE                  --证券融资交易类型
                ,'0'                                         AS SECUOWNERTRANSFLAG       --证券所有权是否转移
                ,'0'                                         AS OTCFLAG                  --场外衍生工具标识
                ,'0'                                         AS VALIDNETTINGFLAG         --有效净额结算协议标识
                ,''                                          AS VALIDNETAGREEMENTID      --有效净额结算协议ID
                ,'01'                                        AS OTCTYPE                  --场外衍生工具类型
                ,0                                           AS DEPOSITRISKPERIOD        --保证金风险期间
                ,0                                           AS MTM                      --重置成本
                ,'01'                                        AS MTMCURRENCY              --重置成本币种
                ,'01'                                        AS BUYERORSELLER            --买方卖方
                ,'0'                                         AS QUALROFLAG               --合格参照资产标识
                ,'0'                                         AS ROISSUERPERFORMFLAG      --参照资产发行人是否能履约
                ,'0'                                         AS BUYERINSOLVENCYFLAG      --信用保护买方是否破产
                ,0                                           AS NONPAYMENTFEES           --尚未支付费用
                ,'1'                                         AS RETAILEXPOFLAG           --零售暴露标识             默认：是(1)
                ,CASE WHEN T2.FLAG1 = '0' AND T2.FLAG2 = '1' AND T2.ZED < 1000000 THEN '020402'  --020402  合格循环零售风险暴露
                      ELSE '020403'  --020403  其他零售风险暴露
                 END                                         AS RETAILCLAIMTYPE          --零售债权类型             默认：空
                ,'0'                                         AS MORTGAGETYPE             --住房抵押贷款类型         默认：否(0)
                ,1                                           AS EXPONUMBER               --风险暴露个数             默认：空
                ,0.8                                         AS LTV                     --贷款价值比  统一更新
                ,T5.MOB                                      AS AGING                   --账龄
                ,T1.NEWDEFAULTFLAG                           AS NEWDEFAULTDEBTFLAG      --新增违约债项标识
                ,T5.PDMODELCODE                              AS PDPOOLMODELID           -- PD分池模型ID
                ,T5.LGDMODELCODE                             AS LGDPOOLMODELID          -- LGD分池模型ID
                ,T5.CCFMODELCODE                             AS CCFPOOLMODELID          -- CCF分池模型ID
                ,T5.PDCODE                                   AS PDPOOLID                -- 所属PD池ID
                ,T5.LGDCODE                                  AS LGDPOOLID               -- 所属LGD池ID
                ,T5.CCFCODE                                  AS CCFPOOLID               -- 所属CCF池ID
                ,'0'                                         AS ABSUAFLAG                --资产证券化基础资产标识   默认：否(0)
                ,''                                          AS ABSPOOLID                --证券化资产池ID           默认：空
                ,''                                          AS GROUPID                  --分组编号                 RWA系统赋值
                ,''                                          AS DEFAULTDATE              --违约时点
                ,NULL                                        AS ABSPROPORTION           --资产证券化比重
                ,NULL                                        AS DEBTORNUMBER            --借款人个数
                ,''                             AS CLIENTNAME               --客户名称
        FROM (
                SELECT C4.XACCOUNT,
                       0 AS WSYED  --未使用额度
                  FROM TMP_XYK_YSY C4
                 WHERE (C4.ZED - C4.DK - C4.DKLX - C4.FY) < 0
                   AND TRIM(CANCL_CODE) NOT IN ('关闭', '核销') --卡片注销代码
                GROUP BY C4.XACCOUNT
                
                UNION ALL  
                SELECT C4.XACCOUNT, 
                       SUM(C4.ZED - C4.DK - C4.DKLX - C4.FY) AS WSYED --未使用额度
                  FROM TMP_XYK_YSY C4
                 WHERE (C4.ZED - C4.DK - C4.DKLX - C4.FY) >= 0
                   AND TRIM(CANCL_CODE) NOT IN ('关闭', '核销') --卡片注销代码  
                 GROUP BY C4.XACCOUNT
              ) WSY  --信用卡未使用额度明细数据
    LEFT JOIN RWA_DEV.CCS_ACCT T1
           ON WSY.XACCOUNT = T1.XACCOUNT
    LEFT JOIN RWA_DEV.RWA_TEMP_LGDLEVEL T5
    ON T1.XACCOUNT=T5.BUSINESSID
    AND T5.BUSINESSTYPE='CREDITCARD'
    LEFT JOIN TMPE_HGFLAG T2  --符合标准的表外业务
         ON T1.XACCOUNT = T2.XACCOUNT
    LEFT JOIN RWA_DEV.TMP_XYK_WJFL_VIEW T3  --五级分类期数
         ON  T1.CUSTR_NBR = T3.CUSTR_NBR         
    WHERE T1.DATANO = p_data_dt_str    
      AND WSY.WSYED <> 0
      ;

    COMMIT;

    DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'RWA_DEV',TABNAME => 'RWA_XYK_EXPOSURE',CASCADE => TRUE);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_XYK_EXPOSURE;

    P_PO_RTNCODE := '1';
    P_PO_RTNMSG  := '成功'||'-'||V_COUNT;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||SQLCODE||';错误信息为:'||SQLERRM||';错误行数为:'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
          ROLLBACK;
          P_PO_RTNCODE := SQLCODE;
          P_PO_RTNMSG  := '信用风险暴露('|| V_PRO_NAME ||')ETL转换失败！'|| SQLERRM||';错误行数为:'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
         RETURN;
END PRO_RWA_XYK_EXPOSURE;
/

