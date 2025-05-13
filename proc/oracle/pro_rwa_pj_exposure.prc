CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_PJ_EXPOSURE(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_PJ_EXPOSURE
    实现功能:核心系统-票据转贴现-信用风险暴露(从数据源核心系统将业务相关信息全量导入RWA票据转贴现接口表风险暴露表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-21
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.BRD_BILL|票据信息
    源  表2 :RWA.ORG_INFO|机构信息表
    源	 表3 :RWA.CODE_LIBRARY|RWA码表
    源  表4 :RWA_DEV.IRS_CR_CUSTOMER_RATE
    源  表5 :RWA_DEV.NCM_BREAKDEFINEDREMARK
    
    目标表1 :RWA_DEV.RWA_PJ_EXPOSURE|票据贴现类信用风险暴露表
    变更记录(修改人|修改时间|修改内容):
    pxl 2019/04/16 去除补录数据、调整核心相关表
    chengang 2019/04/23 更新RWA_DEV.BRD_BILL|票据信息的机构号
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_PJ_EXPOSURE';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_PJ_EXPOSURE';
    

    --2.将满足条件的数据从源表插入到目标表中  外部转帖现-银票,商票
    INSERT INTO RWA_DEV.RWA_PJ_EXPOSURE(
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
               ,DefaultDate                     --违约时点
               ,ABSPROPORTION                   --资产证券化比重
               ,DEBTORNUMBER                    --借款人个数
    )
    SELECT
          TO_DATE(p_data_dt_str,'YYYYMMDD') , --数据日期        
          p_data_dt_str , --数据流水号       
          T1.ACCT_NO  , --风险暴露ID        
          T1.ACCT_NO  , --债项ID        
          'PJ'  , --源系统ID       
          T1.ACCT_NO  , --合同ID        
          CASE WHEN T1.SBJT_CD='13010511' THEN substr(T1.BILL_NO,2,12)   --如果是内转取承兑行，承兑行通过票号关联到联行表获取承兑行名称
               ELSE T3.CUSTOMERID  
          END AS CLIENTID,  --参与主体ID        
          T1.ORG_CD , --源机构ID       
          T2.ORGNAME  , --源机构名称       
          T2.SORTNO , --所属机构排序号       
          T1.ORG_CD , --所属机构ID        
          T2.ORGNAME  , --所属机构名称        
          T1.ORG_CD , --账务机构ID        
          T2.ORGNAME  , --账务机构名称        
          NVL(T3.INDUSTRYTYPE,'J6621') , --所属行业代码        
          T4.ITEMNAME , --所属行业名称        
          '0401'  , --业务条线        默认：同业(04)  0401:同业-金融市场部
          ''  , --资产大类        RWA映射规则，待补充
          ''  , --资产小类        RWA映射规则，待补充
          CASE WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130101' THEN '10302020' --银承贴现
                     WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130103' THEN '10303010'--转贴现
                       WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130105' THEN '10303011'--内转（三个月内）
                       WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130102' THEN '10302015'   --- 商业汇票贴现
                     ELSE '未知' END  , --业务品种代码        
          CASE WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130101' THEN '银承汇票贴现' 
               WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130103' THEN '银承汇票转贴现业务'
               WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130105' THEN '内转-银承汇票转贴现'
               WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130102' THEN '商业汇票贴现'
               ELSE '未知' END, --业务品种名称       
          '01'  , --信用风险数据类型        默认：一般非零售(01)
          '01'  , --折扣系数对应资产类别        默认: 现金及现金等价物(01)
          '07' , --权重法业务类型       07:一般资产
          ''  , --权重法暴露大类       RWA规则映射
          ''  , --权重法暴露小类       RWA规则映射
          ''  , --内评法暴露大类       
          ''  , --内评法暴露小类       
          '01'  , --暴露所属标识        默认：表内(01)
          '01'  , --账户类别        默认：01-银行账户
          '03'  , --监管交易类型        默认：抵押贷款(03)
          '0' , --回购交易标识        默认：否(0)
          1   , --重估频率        默认： 1
          NVL(T1.CCY_CD,'CNY') , --币种        
          NVL(T1.ATL_PAY_AMT, 0)  , --正常本金余额        
          0 , --逾期余额        默认：0
          0 , --非应计余额       默认：0
          NVL(T1.ATL_PAY_AMT, 0)  , --表内余额        表内余额=正常本金余额+逾期余额+非应计余额
          0, --正常利息        正常利息=表内欠息+表外欠息
          0 , --表内欠息        默认：0
          0 , --表外欠息        默认：0
          0 , --应收费用        默认：0
          NVL(T1.ATL_PAY_AMT, 0)  , --资产余额        表内资产总余额=表内余额+应收费用+正常利息+表内欠息, 对于证券融资交易，即为证券市值或回购金额
          --NVL(T1.ATL_PAY_AMT, 0)+NVL(T1.INTR_AMT, 0),  ---20191010 WZB 不算利息
          T1.SBJT_CD  , --科目一       
          ''  , --科目二       默认： 空
          ''  , --科目三       默认： 空
          CASE  WHEN SUBSTR(T1.SBJT_CD, 1, 6) = '130103' 
                THEN T1.DISC_DT--转贴现 贴现日
          ELSE T1.ISSUE_DT--内转（三个月内）--银承贴现 起始日
					   END , --起始日期        
          T1.MATU_DT  , --到期日期        
          CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - 
                TO_DATE(DECODE(SUBSTR(T1.SBJT_CD, 1, 6),'130103',T1.DISC_DT,'130102',T1.ISSUE_DT,'130101',T1.ISSUE_DT,TO_CHAR(TO_DATE(T1.MATU_DT,'YYYY-MM-DD')-100,'YYYY-MM-DD')),'YYYY-MM-DD')) / 365<0
                                THEN 0
                                ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - 
                TO_DATE(DECODE(SUBSTR(T1.SBJT_CD, 1, 6),'130103',T1.DISC_DT,'130102',T1.ISSUE_DT,'130101',T1.ISSUE_DT,TO_CHAR(TO_DATE(T1.MATU_DT,'YYYY-MM-DD')-100,'YYYY-MM-DD')),'YYYY-MM-DD')) / 365
                          END , --内转（三个月上）  --原始期限        单位：年
          CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                        ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                  END  , --剩余期限        单位：年
          '01'  , --风险分类        默认：正常(01)
          '01'  , --风险暴露状态        默认：正常(01)
          ''  , --逾期天数        
          0 , --专项准备金       RWA映射规则
          0 , --一般准备金       待补充，从I9获取
          0 , --特别准备金       RWA映射规则
          0 , --已核销金额       
          ''  , --表外暴露来源        
          ''  , --表外业务类型        
          ''  , --权重法表外业务类型细分       
          ''  , --是否可随时无条件撤销        
          ''  , --信用转换系数级别        
          NULL  , --高级法信用转换系数       
          '01'  , --债权级别     01:高级债权       
          '0' , --是否为债券       
          '02'  , --债券发行目的  02:其他      
          '0' , --是否非自用不动产        
          ''  , --抵债资产期限类型        
          '0' , --是否依赖于银行未来盈利       
          T5.PDADJLEVEL , --内部评级        交易对手的内部评级-客户评级
          T5.PD , --违约概率        交易对手的内部评级-客户评级
          NULL  , --违约损失率级别       
          NULL  , --高级法违约损失率        
          NULL  , --高级法有效期限       
          NULL  , --高级法违约风险暴露       
          CASE WHEN T6.BREAKDATE IS NOT NULL THEN '1' ELSE '0' END  , --违约标识        
          ''  , --已违约暴露预期损失比率       
          ''  , --已违约暴露违约损失率        
          '0' , --股权暴露标识        
          ''  , --股权投资对象类型        
          ''  , --股权投资形成原因        
          '0' , --专业贷款标识        
          ''  , --专业贷款类型        
          ''  , --项目融资阶段        
          '01'  , --监管评级    01:优     
          ''  , --银监会认定评级是否更为审慎       
          ''  , --是否波动性较大       
          '0' , --是否清算过程中风险暴露       
          '0' , --是否货款对付模式        
          NULL  , --延迟交易天数        
          '0' , --有价证券标识        
          ''  , --证券发行人ID       
          ''  , --评级期限类型        
          ''  , --证券发行等级        
          NULL  , --证券剩余期限        
          1 , --证券重估频率        
          '0' , --是否中央交易对手相关交易        
          ''  , --中央交易对手ID        
          ''  , --是否合格中央交易对手        
          ''  , --银行角色        
          ''  , --清算方式        
          '0' , --是否银行提交资产        
          ''  , --符合条件情况        
          '0' , --证券融资交易标识        
          '0' , --净额结算主协议标识       
          ''  , --净额结算主协议ID       
          ''  , --证券融资交易类型        默认：正回购(01) 逆回购(02)
          '0' , --证券所有权是否转移       
          '0' , --场外衍生工具标识        
          '0' , --有效净额结算协议标识        
          ''  , --有效净额结算协议ID        
          ''  , --场外衍生工具类型        
          ''  , --保证金风险期间       
          ''  , --重置成本        
          ''  , --重置成本币种        
          ''  , --买方卖方        
          '0' , --合格参照资产标识        
          ''  , --参照资产发行人是否能履约        
          ''  , --信用保护买方是否破产        
          ''  , --尚未支付费用        
          '0' , --零售暴露标识        
          ''  , --零售债权类型        
          ''  , --住房抵押贷款类型        
          1 , --风险暴露个数        
          0.8 , --贷款价值比       
          NULL  , --账龄        
          ''  , --新增违约债项标识        
          ''  , --PD分池模型ID        
          ''  , --LGD分池模型ID       
          ''  , --CCF分池模型ID       
          ''  , --所属PD池ID       
          ''  , --所属LGD池ID        
          ''  , --所属CCF池ID        
          '0' , --资产证券化基础资产标识       
          ''  , --证券化资产池ID        
          ''  , --分组编号        RWA计算
          CASE WHEN T5.PDADJLEVEL = '0116' THEN TO_DATE(T5.PDVAVLIDDATE,'YYYYMMDD')
                   ELSE NULL
                   END , --违约时点        交易对手的内部评级-客户评级
          ''  , --资产证券化比重       
          ''    --借款人个数       
      FROM  BRD_BILL T1      
      LEFT JOIN RWA.ORG_INFO T2
             ON T1.ORG_CD = T2.ORGID
            AND T2.STATUS = '1' 
      LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO T3
             ON T1.CUST_NO = T3.MFCUSTOMERID
             AND T1.DATANO=T3.DATANO
            AND T3.CUSTOMERTYPE NOT LIKE '03%' --对公客户   
      LEFT JOIN RWA.CODE_LIBRARY T4
             ON T3.INDUSTRYTYPE = T4.ITEMNO
            AND T4.CODENO = 'IndustryType'
                /*LEFT JOIN RWA_DEV.IRS_CR_CUSTOMER_RATE T5
             ON T1.CUST_NO = T5.T_IT_CUSTOMER_ID   */
      LEFT JOIN RWA_TEMP_PDLEVEL T5
             ON T5.CUSTID = T3.CUSTOMERID
      LEFT JOIN RWA_DEV.NCM_BREAKDEFINEDREMARK T6
             ON T1.CUST_NO = T6.CUSTOMERID   
             AND T1.DATANO=T6.DATANO
      WHERE T1.ATL_PAY_AMT <> 0	--取的都是本金
        AND SUBSTR(T1.SBJT_CD, 1, 6) IN (
            '130101',	--贴现资产-银行承兑汇票贴现
            '130103',	--贴现资产-银行承兑汇票转贴现
            '130105',	--贴现资产-内部转贴现
            '130102'  --商业汇票贴现
        )
        AND T1.DATANO=p_data_dt_str;

    COMMIT;

		dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_PJ_EXPOSURE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_PJ_EXPOSURE;

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '信用风险暴露('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_PJ_EXPOSURE;
/

