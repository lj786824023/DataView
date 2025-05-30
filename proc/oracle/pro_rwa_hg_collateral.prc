CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_HG_COLLATERAL(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_HG_COLLATERAL
    实现功能:核心系统-回购-抵质押品(从数据源核心系统将回购类相关信息全量导入RWA回购类接口表抵质押品表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-21
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.BRD_REPO_PORTFOLIO|债券回购质押信息
    源  表2 :RWA_DEV.BRD_SECURITY_POSI|债券头寸
    源  表3 :RWA_DEV.BRD_BOND|债券信息
    源  表4 :RWA_DEV.BRD_REPO|债券回购
    源  表5 :RWA_DEV.BRD_BILL_REPO_PORTF|票据回购质押信息
    源  表5 :RWA_DEV.BRD_BILL_REPO|票据回购信息 
    源  表5 :RWA_DEV.BRD_BILL|票据信息 
    源  表5 :RWA.RWA_WP_COUNTRYRATING|国家评级信息表      
    
    目标表  :RWA_DEV.RWA_HG_COLLATERAL|核心系统回购类抵质押品表
    变更记录(修改人|修改时间|修改内容):
    pxl 2019/04/15 除去补录信息、老核心相关表
    
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_HG_COLLATERAL';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  --v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_HG_COLLATERAL';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 买入返售债券回购-质押式
    INSERT INTO RWA_DEV.RWA_HG_COLLATERAL(
                 DataDate                              --数据日期
                ,DataNo                                --数据流水号
                ,CollateralID                       	 --抵质押品ID
                ,SSysID                             	 --源系统ID
                ,SGuarContractID                    	 --源担保合同ID
                ,SCollateralID                      	 --源抵质押品ID
                ,CollateralName                     	 --抵质押品名称
                ,IssuerID                           	 --发行人ID
                ,ProviderID                         	 --提供人ID
                ,CreditRiskDataType                 	 --信用风险数据类型
                ,GuaranteeWay                       	 --担保方式
                ,SourceColType                      	 --源抵质押品大类
                ,SourceColSubType                   	 --源抵质押品小类
                ,SpecPurpBondFlag                   	 --是否为收购国有银行不良贷款而发行的债券
                ,QualFlagSTD                        	 --权重法合格标识
                ,QualFlagFIRB                 				 --内评初级法合格标识
                ,CollateralTypeSTD                  	 --权重法抵质押品类型
                ,CollateralSdvsSTD                  	 --权重法抵质押品细分
                ,CollateralTypeIRB                  	 --内评法抵质押品类型
                ,CollateralAmount                   	 --抵押总额
                ,Currency                           	 --币种
                ,StartDate                          	 --起始日期
                ,DueDate                            	 --到期日期
                ,OriginalMaturity                   	 --原始期限
                ,ResidualM                          	 --剩余期限
                ,InteHaircutsFlag                   	 --自行估计折扣系数标识
                ,InternalHc                         	 --内部折扣系数
                ,FCType                             	 --金融质押品类型
                ,ABSFlag                            	 --资产证券化标识
                ,RatingDurationType                 	 --评级期限类型
                ,FCIssueRating     										 --金融质押品发行等级
                ,FCIssuerType                          --金融质押品发行人类别
                ,FCIssuerState                         --金融质押品发行人注册国家
                ,FCResidualM                           --金融质押品剩余期限
                ,RevaFrequency                         --重估频率
                ,GroupID                               --分组编号
                ,RCERating														 --发行人境外注册地外部评级
    )
  SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --数据日期        
         p_data_dt_str, --数据流水号       
         T1.REPO_REFERENCE || T1.SECURITY_REFERENCE, --抵质押品ID        
         'HG', --源系统ID       
         'MRFSZQ' || T3.ACCT_NO, --源担保合同ID       
         T1.REPO_REFERENCE || T1.SECURITY_REFERENCE, --源抵质押品ID       
         T4.BOND_FULL_NAME, --抵质押品名称        
         T4.ISSUER_CODE, --发行人ID       
         T4.ISSUER_CODE, --提供人ID       未找到
         '01', --信用风险数据类型        默认 01 一般非零售
         '060', --担保方式        默认 060 质押担保 码表：CODENO=GuarantyType
         '001003', --源抵质押品大类       默认 001003 债券 码表：CMS_COLLATERALTYPE_INFO
         '001003', --源抵质押品小类       默认 001004 债券 码表：CMS_COLLATERALTYPE_INFO
         '0', --是否为收购国有银行不良贷款而发行的债券       未找到债券发行目的，默认：否
         '', --权重法合格标识       
         '', --内评初级法合格标识       
         '', --权重法抵质押品类型       
         '', --权重法抵质押品细分       
         '', --内评法抵质押品类型       
         T1.FACE_AMOUNT, --抵押总额        
         T1.CCY_CD, --币种        
         T4.ISSUE_DATE, --起始日期        
         T4.MATU_DT, --到期日期        
         CASE
           WHEN (TO_DATE(T4.MATU_DT, 'YYYYMMDD') -
                TO_DATE(T4.ISSUE_DATE, 'YYYYMMDD')) / 365 < 0 THEN
            0
           ELSE
            (TO_DATE(T4.MATU_DT, 'YYYYMMDD') -
            TO_DATE(T4.ISSUE_DATE, 'YYYYMMDD')) / 365
         END, --原始期限        
         CASE
           WHEN (TO_DATE(T4.MATU_DT, 'YYYYMMDD') -
                TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
            0
           ELSE
            (TO_DATE(T4.MATU_DT, 'YYYYMMDD') -
            TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
         END, --剩余期限        
         '0', --自行估计折扣系数标识        
         NULL, --内部折扣系数        
         '', --金融质押品类型       
         '0', --资产证券化标识       默认 否
         '01', --评级期限类型        默认：01 长期信用评级 长期信用评级或短期信用评级
         '01', --金融质押品发行等级       匹配普华的评级结果
         '02', --金融质押品发行人类别        金融质押品发行人类别：01 主权02 其他发行人
         '01', --金融质押品发行人注册国家        金融质押品发行人注册国家：01 中国 02 非中国
         CASE
           WHEN (TO_DATE(T4.MATU_DT, 'YYYYMMDD') -
                TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
            0
           ELSE
            (TO_DATE(T4.MATU_DT, 'YYYYMMDD') -
            TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
         END, --金融质押品剩余期限       
         1, --重估频率        
         '',
         T5.RATINGRESULT --发行人境外注册地外部评级        通过发行人注册地国家匹配      
    FROM BRD_REPO_PORTFOLIO T1 --债券回购质押信息
   INNER JOIN BRD_REPO T3 --债券回购
      ON T1.REPO_REFERENCE = T3.ACCT_NO --回购交易编号
     AND T1.DATANO = T3.DATANO
    LEFT JOIN BRD_BOND T4 --债券信息
      ON T1.SECURITY_REFERENCE = T4.BOND_ID
     AND T1.DATANO = T4.DATANO
    LEFT JOIN RWA.RWA_WP_COUNTRYRATING T5
      ON '发行人注册地国家' = T5.COUNTRYCODE
     AND T5.ISINUSE = '1'
  
   WHERE T3.CASH_NOMINAL <> 0
        --AND T3.CLIENT_PROPRIETARY = 'F'  --质押式
     AND T3.REPO_TYPE IN ('4', 'RB') --买入返售
     AND T3.PRINCIPAL_GLNO LIKE '111103%' --买入返售债券回购-质押式-债券
     AND T4.ISSUER_CODE IS NOT NULL
     AND T3.ACCT_NO IS NOT NULL
     AND T1.DATANO = p_data_dt_str
    ;
  
    COMMIT;

    --2.2 买入返售票据回购-质押式
    INSERT INTO RWA_DEV.RWA_HG_COLLATERAL(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,COLLATERALID                          --抵质押品ID
                ,SSYSID                                --源系统ID
                ,SGUARCONTRACTID                       --源担保合同ID
                ,SCOLLATERALID                         --源抵质押品ID
                ,COLLATERALNAME                        --抵质押品名称
                ,ISSUERID                              --发行人ID
                ,PROVIDERID                            --提供人ID
                ,CREDITRISKDATATYPE                    --信用风险数据类型
                ,QUALFLAGSTD                           --权重法合格标识
                ,QUALFLAGFIRB                          --内评初级法合格标识
                ,COLLATERALAMOUNT                      --抵押总额
                ,CURRENCY                              --币种
                ,GUARANTEEWAY                          --担保方式
                ,SOURCECOLTYPE                         --源抵质押品大类
                ,SOURCECOLSUBTYPE                      --源抵质押品小类
                ,COLLATERALTYPEIRB                     --内评法抵质押品类型
                ,COLLATERALTYPESTD                     --权重法抵质押品类型
                ,COLLATERALSDVSSTD                     --权重法抵质押品细分
                ,SPECPURPBONDFLAG                      --是否为收购国有银行不良贷款而发行的债券
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,INTEHAIRCUTSFLAG                      --自行估计折扣系数标识
                ,INTERNALHC                            --内部折扣系数
                ,FCTYPE                                --金融质押品类型
                ,ABSFLAG                               --资产证券化标识
                ,RATINGDURATIONTYPE                    --评级期限类型
                ,FCISSUERATING                         --金融质押品发行等级
                ,FCISSUERTYPE                          --金融质押品发行人类别
                ,FCISSUERSTATE                         --金融质押品发行人注册国家
                ,FCRESIDUALM                           --金融质押品剩余期限
                ,REVAFREQUENCY                         --重估频率
                ,GROUPID                               --分组编号
                ,RCERating                             --发行人境外注册地外部评级
    )
    SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --数据日期        
           p_data_dt_str, --数据流水号       
           T1.REPO_REFERENCE, --抵质押品ID        
           'HG', --源系统ID       
           'MRFSPJ' || T2.ACCT_NO, --源担保合同ID       
           T1.SECURITY_REFERENCE, --源抵质押品ID       
           CASE
             WHEN T3.SBJT_CD IN
                  ('11110201', '11110203', '11110206', '11110208') THEN
              '银行承兑汇票'
             ELSE
              '商业承兑汇票'
           END, --抵质押品名称        票据名称，待数据验证
           --T2.CUST_NO          , --发行人ID       
           N.CUSTOMERID,
           T1.REMITTER_CUST_NO, --提供人ID       出票人，待数据验证
           '01', --信用风险数据类型        默认 01 一般非零售
           NULL, --权重法合格标识
           NULL, --内评初级法合格标识
           T1.BILL_AMOUNT, --抵押总额        
           T2.CASH_CCY_CD, --币种   
           '060', --担保方式        默认 060 质押担保 码表：CODENO=GuarantyType
           '001004', --源抵质押品大类       默认：票据(001004)
           '001004', --源抵质押品小类       默认：票据(001004)
           NULL, --内评法抵质押品类型
           NULL, --权重法抵质押品类型
           NULL, --权重法抵质押品细分
           '0', --是否为收购国有银行不良贷款而发行的债券       默认：否
           T1.OUT_BILL_DT, --起始日期        
           T1.END_BILL_DT, --到期日期        
           CASE
             WHEN (TO_DATE(T1.END_BILL_DT, 'YYYYMMDD') -
                  TO_DATE(T1.OUT_BILL_DT, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T1.END_BILL_DT, 'YYYYMMDD') -
              TO_DATE(T1.OUT_BILL_DT, 'YYYYMMDD')) / 365
           END, --原始期限        
           CASE
             WHEN (TO_DATE(T1.END_BILL_DT, 'YYYYMMDD') -
                  TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T1.END_BILL_DT, 'YYYYMMDD') -
              TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
           END, --剩余期限        
           '0', --自行估计折扣系数标识        
           NULL, --内部折扣系数        
           '09', --金融质押品类型       默认：其他(09)
           '0', --资产证券化标识       默认 否
           '', --评级期限类型        默认：空
           '', --金融质押品发行等级       默认：空
           '02', --金融质押品发行人类别        需要明确：承兑行客户类型判断规则，01 主权 02 其他发行人
           '01', --金融质押品发行人注册国家        需要明确：承兑行国别判断规则
           CASE
             WHEN (TO_DATE(T1.END_BILL_DT, 'YYYYMMDD') -
                  TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T1.END_BILL_DT, 'YYYYMMDD') -
              TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
           END, --金融质押品剩余期限       
           1, --重估频率        
           '', --分组编号        
           '' --发行人境外注册地外部评级        目前无
      FROM BRD_BILL_REPO_PORTF T1 --票据回购质押信息   
     INNER JOIN BRD_BILL_REPO T2 --票据回购信息
        ON T1.REPO_REFERENCE = T2.ACCT_NO
        AND T1.DATANO=T2.DATANO
       AND T1.SECURITY_REFERENCE = T2.SECURITY_REFERENCE
      LEFT JOIN NCM_CUSTOMER_INFO N
        ON T2.CUST_NO = N.MFCUSTOMERID
        AND T1.DATANO=N.DATANO
      LEFT JOIN BRD_BILL T3 --票据信息 
        ON T1.REPO_REFERENCE = T3.ACCT_NO
        AND T1.DATANO=T3.DATANO
       AND T1.SECURITY_REFERENCE = T3.BILL_NO
      LEFT JOIN RWA.RWA_WP_COUNTRYRATING T4
        ON '发行人注册地国家' = T4.COUNTRYCODE
       AND T4.ISINUSE = '1'
     WHERE T2.CASH_NOMINAL <> 0
          --AND T2.CLIENT_PROPRIETARY = 'Y'  --质押式
       AND T2.REPO_TYPE IN ('4', 'RB') --买入返售 
       AND SUBSTR(T2.PRINCIPAL_GLNO, 1, 6) = '111102' --买入返售金融资产-买入返售票据    
       AND T1.DATANO = p_data_dt_str
    ;

    COMMIT;
    
    --2.2 买入返售票据回购-交易对手不是银行的需要用票据作为缓释
    INSERT INTO RWA_DEV.RWA_HG_COLLATERAL(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,COLLATERALID                          --抵质押品ID
                ,SSYSID                                --源系统ID
                ,SGUARCONTRACTID                       --源担保合同ID
                ,SCOLLATERALID                         --源抵质押品ID
                ,COLLATERALNAME                        --抵质押品名称
                ,ISSUERID                              --发行人ID
                ,PROVIDERID                            --提供人ID
                ,CREDITRISKDATATYPE                    --信用风险数据类型
                ,QUALFLAGSTD                           --权重法合格标识
                ,QUALFLAGFIRB                          --内评初级法合格标识
                ,COLLATERALAMOUNT                      --抵押总额
                ,CURRENCY                              --币种
                ,GUARANTEEWAY                          --担保方式
                ,SOURCECOLTYPE                         --源抵质押品大类
                ,SOURCECOLSUBTYPE                      --源抵质押品小类
                ,COLLATERALTYPEIRB                     --内评法抵质押品类型
                ,COLLATERALTYPESTD                     --权重法抵质押品类型
                ,COLLATERALSDVSSTD                     --权重法抵质押品细分
                ,SPECPURPBONDFLAG                      --是否为收购国有银行不良贷款而发行的债券
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,INTEHAIRCUTSFLAG                      --自行估计折扣系数标识
                ,INTERNALHC                            --内部折扣系数
                ,FCTYPE                                --金融质押品类型
                ,ABSFLAG                               --资产证券化标识
                ,RATINGDURATIONTYPE                    --评级期限类型
                ,FCISSUERATING                         --金融质押品发行等级
                ,FCISSUERTYPE                          --金融质押品发行人类别
                ,FCISSUERSTATE                         --金融质押品发行人注册国家
                ,FCRESIDUALM                           --金融质押品剩余期限
                ,REVAFREQUENCY                         --重估频率
                ,GROUPID                               --分组编号
                ,RCERating                             --发行人境外注册地外部评级
    )
   SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --数据日期        
          p_data_dt_str, --数据流水号       
          T1.SECURITY_REFERENCE, --抵质押品ID        
          'HG', --源系统ID       
          'MRFSPJ' || T1.ACCT_NO, --源担保合同ID       
          T1.SECURITY_REFERENCE, --源抵质押品ID       
          CASE
            WHEN T1.PRINCIPAL_GLNO IN
                 ('11110201', '11110203', '11110206', '11110208') THEN
             '银行承兑汇票'
            ELSE
             '商业承兑汇票'
          END, --抵质押品名称        票据名称，待数据验证
          --T2.CUST_NO          , --发行人ID       
          T1.CUST_NO,
          T1.CUST_NO, --提供人ID       出票人，待数据验证
          '01', --信用风险数据类型        默认 01 一般非零售
          NULL, --权重法合格标识
          NULL, --内评初级法合格标识
          T1.CASH_NOMINAL, --抵押总额        
          T1.CASH_CCY_CD, --币种   
          '060', --担保方式        默认 060 质押担保 码表：CODENO=GuarantyType
          '001004', --源抵质押品大类       默认：票据(001004)
          '001004', --源抵质押品小类       默认：票据(001004)
          NULL, --内评法抵质押品类型
          NULL, --权重法抵质押品类型
          NULL, --权重法抵质押品细分
          '0', --是否为收购国有银行不良贷款而发行的债券       默认：否
          T1.START_DT, --起始日期        
          T1.MATU_DT, --到期日期        
          CASE
            WHEN (TO_DATE(T1.MATU_DT, 'YYYYMMDD') -
                 TO_DATE(T1.START_DT, 'YYYYMMDD')) / 365 < 0 THEN
             0
            ELSE
             (TO_DATE(T1.MATU_DT, 'YYYYMMDD') -
             TO_DATE(T1.START_DT, 'YYYYMMDD')) / 365
          END, --原始期限        
          CASE
            WHEN (TO_DATE(T1.MATU_DT, 'YYYYMMDD') -
                 TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
             0
            ELSE
             (TO_DATE(T1.MATU_DT, 'YYYYMMDD') -
             TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
          END, --剩余期限        
          '0', --自行估计折扣系数标识        
          NULL, --内部折扣系数        
          '09', --金融质押品类型       默认：其他(09)
          '0', --资产证券化标识       默认 否
          '', --评级期限类型        默认：空
          '', --金融质押品发行等级       默认：空
          '02', --金融质押品发行人类别        需要明确：承兑行客户类型判断规则，01 主权 02 其他发行人
          '01', --金融质押品发行人注册国家        需要明确：承兑行国别判断规则
          CASE
            WHEN (TO_DATE(T1.MATU_DT, 'YYYYMMDD') -
                 TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
             0
            ELSE
             (TO_DATE(T1.MATU_DT, 'YYYYMMDD') -
             TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
          END, --金融质押品剩余期限       
          1, --重估频率        
          '', --分组编号        
          '' --发行人境外注册地外部评级        目前无
     FROM RWA_DEV.BRD_BILL_REPO T1 --票据回购            
     LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO T3
       ON T1.CUST_NO = T3.MFCUSTOMERID
       AND T1.DATANO=T3.DATANO
      AND T3.CUSTOMERTYPE NOT LIKE '03%' --对公客户                   
    WHERE T1.CASH_NOMINAL <> 0 --过滤无效数据
      AND T1.PRINCIPAL_GLNO IS NOT NULL --经ALM集市反馈  科目为空的数据不计帐为历史数据
      AND SUBSTR(T1.PRINCIPAL_GLNO, 1, 6) = '111102' --买入返售金融资产-买入返售票据
      AND (T1.CLIENT_PROPRIETARY <> 'N' OR T1.CLIENT_PROPRIETARY IS NULL) --是否可以再质押 N为买断式  非N质押式
      AND T3.CUSTOMERNAME NOT LIKE '%银行%' --交易对手不是银行才需要那票据做缓释
      AND T1.DATANO = p_data_dt_str;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_HG_COLLATERAL',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_HG_COLLATERAL;
    --Dbms_output.Put_line('RWA_DEV.RWA_HG_COLLATERAL表当前插入的核心系统-债券回购数据记录为: ' || (v_count1 - v_count) || ' 条');




    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '抵质押品('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_HG_COLLATERAL;
/

