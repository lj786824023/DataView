CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_SMZ_EXPOSURE(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:PRO_RWA_SMZ_EXPOSURE
    实现功能:私募债-信用风险暴露
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2016-07-08
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_WS_PRIVATE_BOND|私募债业务补录模板
    目标表  :RWA_SMZ_EXPOSURE|私募债信用风险暴露表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'PRO_RWA_SMZ_EXPOSURE';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_SMZ_EXPOSURE';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 更新补录表的债券ID
    UPDATE RWA.RWA_WS_PRIVATE_BOND T1 set T1.ZQID =
        (SELECT T2.ZQID FROM
           (SELECT ZQMC,p_data_dt_str || 'SMZ' || lpad(min(rownum), 10, '0') as ZQID
                FROM RWA.RWA_WS_PRIVATE_BOND
                WHERE DATADATE = TO_DATE(p_data_dt_str,'yyyymmdd') GROUP BY ZQMC) T2
         WHERE T1.ZQMC = T2.ZQMC
        )
    WHERE T1.DATADATE = TO_DATE(p_data_dt_str,'yyyymmdd')
    ;
    COMMIT;
    --2.2 私募债-信用风险暴露
    INSERT INTO RWA_SMZ_EXPOSURE(
                DATADATE                               --数据日期
                ,DATANO                              	 --数据流水号
                ,EXPOSUREID                          	 --风险暴露ID
                ,DUEID                               	 --债项ID
                ,SSYSID                              	 --源系统ID
                ,CONTRACTID                          	 --合同ID
                ,CLIENTID                            	 --参与主体ID
                ,SORGID                              	 --源机构ID
                ,SORGNAME                            	 --源机构名称
                ,ORGID                               	 --所属机构ID
                ,ORGNAME                             	 --所属机构名称
                ,ACCORGID                            	 --账务机构ID
                ,ACCORGNAME                          	 --账务机构名称
                ,INDUSTRYID                          	 --所属行业代码
                ,INDUSTRYNAME                  			 	 --所属行业名称
                ,BUSINESSLINE                        	 --条线
                ,ASSETTYPE                           	 --资产大类
                ,ASSETSUBTYPE                        	 --资产小类
                ,BUSINESSTYPEID                      	 --业务品种代码
                ,BUSINESSTYPENAME                    	 --业务品种名称
                ,CREDITRISKDATATYPE                  	 --信用风险数据类型
                ,ASSETTYPEOFHAIRCUTS                 	 --折扣系数对应资产类别
                ,BUSINESSTYPESTD                     	 --权重法业务类型
                ,EXPOCLASSSTD                        	 --权重法暴露大类
                ,EXPOSUBCLASSSTD                     	 --权重法暴露小类
                ,EXPOCLASSIRB                        	 --内评法暴露大类
                ,EXPOSUBCLASSIRB                     	 --内评法暴露小类
                ,EXPOBELONG                          	 --暴露所属标识
                ,BOOKTYPE                            	 --账户类别
                ,REGUTRANTYPE                        	 --监管交易类型
                ,REPOTRANFLAG                        	 --回购交易标识
                ,REVAFREQUENCY                       	 --重估频率
                ,CURRENCY                            	 --币种
                ,NORMALPRINCIPAL                     	 --正常本金余额
                ,OVERDUEBALANCE     				   		   	 --逾期余额
                ,NONACCRUALBALANCE                   	 --非应计余额
                ,ONSHEETBALANCE                      	 --表内余额
                ,NORMALINTEREST                      	 --正常利息
                ,ONDEBITINTEREST                     	 --表内欠息
                ,OFFDEBITINTEREST                    	 --表外欠息
                ,EXPENSERECEIVABLE                   	 --应收费用
                ,ASSETBALANCE                        	 --资产余额
                ,ACCSUBJECT1                         	 --科目一
                ,ACCSUBJECT2                         	 --科目二
                ,ACCSUBJECT3                         	 --科目三
                ,STARTDATE                           	 --起始日期
                ,DUEDATE                             	 --到期日期
                ,ORIGINALMATURITY                    	 --原始期限
                ,RESIDUALM                           	 --剩余期限
                ,RISKCLASSIFY                        	 --风险分类
                ,EXPOSURESTATUS                      	 --风险暴露状态
                ,OVERDUEDAYS                         	 --逾期天数
                ,SPECIALPROVISION                    	 --专项准备金
                ,GENERALPROVISION                    	 --一般准备金
                ,ESPECIALPROVISION                   	 --特别准备金
                ,WRITTENOFFAMOUNT                    	 --已核销金额
                ,OFFEXPOSOURCE                       	 --表外暴露来源
                ,OFFBUSINESSTYPE                     	 --表外业务类型
                ,OFFBUSINESSSDVSSTD                  	 --权重法表外业务类型细分
                ,UNCONDCANCELFLAG                    	 --是否可随时无条件撤销
                ,CCFLEVEL                            	 --信用转换系数级别
                ,CCFAIRB                             	 --高级法信用转换系数
                ,CLAIMSLEVEL                         	 --债权级别
                ,BONDFLAG                            	 --是否为债券
                ,BONDISSUEINTENT                     	 --债券发行目的
                ,NSUREALPROPERTYFLAG                 	 --是否非自用不动产
                ,REPASSETTERMTYPE                    	 --抵债资产期限类型
                ,DEPENDONFPOBFLAG                    	 --是否依赖于银行未来盈利
                ,IRATING                             	 --内部评级
                ,PD                                  	 --违约概率
                ,LGDLEVEL                               --违约损失率级别
                ,LGDAIRB                                --高级法违约损失率
                ,MAIRB                                  --高级法有效期限
                ,EADAIRB                                --高级法违约风险暴露
                ,DEFAULTFLAG                            --违约标识
                ,BEEL                                   --已违约暴露预期损失比率
                ,DEFAULTLGD                             --已违约暴露违约损失率
                ,EQUITYEXPOFLAG                         --股权暴露标识
                ,EQUITYINVESTTYPE                       --股权投资对象类型
                ,EQUITYINVESTCAUSE                      --股权投资形成原因
                ,SLFLAG                                 --专业贷款标识
                ,SLTYPE                                 --专业贷款类型
                ,PFPHASE                                --项目融资阶段
                ,REGURATING                             --监管评级
                ,CBRCMPRATINGFLAG                       --银监会认定评级是否更为审慎
                ,LARGEFLUCFLAG                          --是否波动性较大
                ,LIQUEXPOFLAG                           --是否清算过程中风险暴露
                ,PAYMENTDEALFLAG                        --是否货款对付模式
                ,DELAYTRADINGDAYS                       --延迟交易天数
                ,SECURITIESFLAG                         --有价证券标识
                ,SECUISSUERID                           --证券发行人ID
                ,RATINGDURATIONTYPE                     --评级期限类型
                ,SECUISSUERATING                        --证券发行等级
                ,SECURESIDUALM                          --证券剩余期限
                ,SECUREVAFREQUENCY                      --证券重估频率
                ,CCPTRANFLAG                            --是否中央交易对手相关交易
                ,CCPID                                  --中央交易对手ID
                ,QUALCCPFLAG                            --是否合格中央交易对手
                ,BANKROLE                               --银行角色
                ,CLEARINGMETHOD                         --清算方式
                ,BANKASSETFLAG                          --是否银行提交资产
                ,MATCHCONDITIONS                        --符合条件情况
                ,SFTFLAG                                --证券融资交易标识
                ,MASTERNETAGREEFLAG                     --净额结算主协议标识
                ,MASTERNETAGREEID                       --净额结算主协议ID
                ,SFTTYPE                                --证券融资交易类型
                ,SECUOWNERTRANSFLAG                     --证券所有权是否转移
                ,OTCFLAG                                --场外衍生工具标识
                ,VALIDNETTINGFLAG                       --有效净额结算协议标识
                ,VALIDNETAGREEMENTID                    --有效净额结算协议ID
                ,OTCTYPE                                --场外衍生工具类型
                ,DEPOSITRISKPERIOD                      --保证金风险期间
                ,MTM                                    --重置成本
                ,MTMCURRENCY                            --重置成本币种
                ,BUYERORSELLER                          --买方卖方
                ,QUALROFLAG                             --合格参照资产标识
                ,ROISSUERPERFORMFLAG                    --参照资产发行人是否能履约
                ,BUYERINSOLVENCYFLAG                    --信用保护买方是否破产
                ,NONPAYMENTFEES                         --尚未支付费用
                ,RETAILEXPOFLAG                         --零售暴露标识
                ,RETAILCLAIMTYPE                        --零售债权类型
                ,MORTGAGETYPE                           --住房抵押贷款类型
                ,DEBTORNUMBER                           --借款人个数
                ,EXPONUMBER                             --风险暴露个数
                ,PDPOOLMODELID                          --PD分池模型ID
                ,LGDPOOLMODELID                         --LGD分池模型ID
                ,CCFPOOLMODELID                         --CCF分池模型ID
                ,PDPOOLID                               --所属PD池ID
                ,LGDPOOLID                              --所属LGD池ID
                ,CCFPOOLID                              --所属CCF池ID
                ,ABSUAFLAG                              --资产证券化基础资产标识
                ,ABSPOOLID                              --证券化资产池ID
                ,ABSPROPORTION                          --资产证券化比重
                ,GROUPID                                --分组编号
                ,ORGSORTNO                             --所属机构排序号
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')                       AS DATADATE                 --数据日期
                ,p_data_dt_str                                          AS DATANO                   --数据流水号
                ,T1.ZQID                                                AS EXPOSUREID               --风险暴露ID
                ,T1.ZQID                                                AS DUEID                     --债项ID
                ,'SMZ'                                                  AS SSYSID                   --源系统代号
                ,T1.ZQID                                                AS CONTRACTID               --合同ID
                ,T1.CUSTID1                                             AS CLIENTID                 --参与主体ID
                ,T1.YWSSJGDM                                             AS SORGID                   --源机构ID
                ,T3.ORGNAME                                             AS SORGNAME                 --源机构名称
                ,T1.YWSSJGDM                                            AS ORGID                    --所属机构代码
                ,T3.ORGNAME                                             AS ORGNAME                  --所属机构名称
                ,T1.YWSSJGDM                                            AS ACCORGID                 --账务机构ID
                ,T3.ORGNAME                                              AS ACCORGNAME               --账务机构名称
                ,T1.RZZJHYTXDM                                          AS INDUSTRYID               --所属行业代码
                ,T4.ITEMNAME                                             AS INDUSTRYNAME             --所属行业名称
                ,''                                                     AS BUSINESSLINE             --条线
                ,''                                                     AS ASSETTYPE                --资产大类
                ,''                                                     AS ASSETSUBTYPE             --资产小类
                ,'112010'                                               AS BUSINESSTYPEID            --业务品种代码
                ,'私募债'                                               AS BUSINESSTYPENAME         --业务品种名称
                ,'06'                                                   AS CREDITRISKDATATYPE       --信用风险数据类型
                ,'01'                                                   AS ASSETTYPEOFHAIRCUTS      --折扣系数对应资产类别
                ,'07'                                                   AS BUSINESSTYPESTD          --权重法业务类型
                ,''                                                     AS EXPOCLASSSTD             --权重法暴露大类
                ,''                                                     AS EXPOSUBCLASSSTD           --权重法暴露小类
                ,''                                                     AS EXPOCLASSIRB             --内评法暴露大类
                ,''                                                     AS EXPOSUBCLASSIRB           --内评法暴露小类
                ,'01'                                                   AS EXPOBELONG               --暴露所属标识
                ,'01'                                                   AS BOOKTYPE                 --账户类别
                ,'03'                                                   AS REGUTRANTYPE             --监管交易类型
                ,'0'                                                    AS REPOTRANFLAG              --回购交易标识           默认：0-否
                ,1                                                      AS REVAFREQUENCY            --重估频率
                ,NVL(T1.YWBZDM,'CNY')                                    AS CURRENCY                 --币种
                ,ROUND(TO_NUMBER(T1.YWYE),6)                             AS NORMALPRINCIPAL          --正常本金余额
                ,0                                                      AS OVERDUEBALANCE           --逾期余额
                ,0                                                      AS NONACCRUALBALANCE        --非应计余额
                ,ROUND(TO_NUMBER(T1.YWYE),6)                             AS ONSHEETBALANCE           --表内余额
                ,0                                                       AS NORMALINTEREST           --正常利息
                ,ROUND(TO_NUMBER(T1.YSWSLX),6)                          AS ONDEBITINTEREST          --表内欠息
                ,0                                                      AS OFFDEBITINTEREST         --表外欠息
                ,T1.YSWSSXF                                             AS EXPENSERECEIVABLE        --应收费用
                ,ROUND(TO_NUMBER(T1.YWYE),6) + ROUND(TO_NUMBER(T1.YSWSLX),6) + ROUND(TO_NUMBER(T1.YSWSSXF),6)
                                                                        AS ASSETBALANCE             --资产余额
                ,T1.KMDM                                                AS ACCSUBJECT1               --科目一
                ,''                                                      AS ACCSUBJECT2               --科目二
                ,''                                                       AS ACCSUBJECT3               --科目三
                ,REPLACE(T1.ZQFXQSRQ,'-','')                            AS STARTDATE                 --起始日期
                ,REPLACE(T1.ZQDQRQ,'-','')                              AS DUEDATE                    --到期日期
                ,CASE WHEN (to_date(T1.ZQDQRQ,'yyyy-mm-dd') - to_date(T1.ZQFXQSRQ,'yyyy-mm-dd')) / 365<0
                      THEN 0
                      ELSE (to_date(T1.ZQDQRQ,'yyyy-mm-dd') - to_date(T1.ZQFXQSRQ,'yyyy-mm-dd')) / 365
                END                                                      AS ORIGINALMATURITY         --原始期限                  单位：年
                ,CASE WHEN (to_date(T1.ZQDQRQ,'yyyy-mm-dd') - to_date(p_data_dt_str,'yyyymmdd')) / 365<0
                      THEN 0
                      ELSE (to_date(T1.ZQDQRQ,'yyyy-mm-dd') - to_date(p_data_dt_str,'yyyymmdd')) / 365
                END                                                      AS RESIDUALM                 --剩余期限
                ,'01'                                                    AS RISKCLASSIFY             --风险分类
                ,''                                                     AS EXPOSURESTATUS           --风险暴露状态
                ,0                                                      AS OVERDUEDAYS              --逾期天数
                ,0                                                      AS SPECIALPROVISION         --专项准备金
                ,0                                                      AS GENERALPROVISION         --一般准备金
                ,0                                                      AS ESPECIALPROVISION        --特别准备金
                ,0                                                      AS WRITTENOFFAMOUNT         --已核销金额
                ,''                                                     AS OFFEXPOSOURCE            --表外暴露来源
                ,''                                                     AS OFFBUSINESSTYPE          --表外业务类型
                ,''                                                     AS OFFBUSINESSSDVSSTD       --权重法表外业务类型细分
                ,'0'                                                    AS UNCONDCANCELFLAG         --是否可随时无条件撤销
                ,''                                                     AS CCFLEVEL                 --信用转换系数级别
                ,''                                                     AS CCFAIRB                  --高级法信用转换系数
                ,'01'                                                   AS CLAIMSLEVEL              --债权级别
                ,'0'                                                    AS BONDFLAG                 --是否为债券
                ,'02'                                                   AS BONDISSUEINTENT          --债券发行目的
                ,'0'                                                    AS NSUREALPROPERTYFLAG      --是否非自用不动产
                ,'0'                                                    AS REPASSETTERMTYPE         --抵债资产期限类型
                ,'0'                                                    AS DEPENDONFPOBFLAG         --是否依赖于银行未来盈利
                ,''                                                     AS IRATING                  --内部评级
                ,''                                                     AS PD                       --违约概率
                ,''                                                     AS LGDLEVEL                 --违约损失率级别
                ,''                                                     AS LGDAIRB                  --高级法违约损失率
                ,NULL                                                   AS MAIRB                    --高级法有效期限
                ,''                                                      AS EADAIRB                  --高级法违约风险暴露
                ,'0'                                                    AS DEFAULTFLAG              --违约标识
                ,''                                                     AS BEEL                     --已违约暴露预期损失比率
                ,''                                                     AS DEFAULTLGD               --已违约暴露违约损失率
                ,'0'                                                    AS EQUITYEXPOFLAG           --股权暴露标识
                ,''                                                     AS EQUITYINVESTTYPE         --股权投资对象类型
                ,''                                                     AS EQUITYINVESTCAUSE        --股权投资形成原因
                ,'0'                                                    AS SLFLAG                   --专业贷款标识
                ,''                                                     AS SLTYPE                   --专业贷款类型
                ,''                                                     AS PFPHASE                   --项目融资阶段
                ,'01'                                                   AS REGURATING               --监管评级
                ,'0'                                                    AS CBRCMPRATINGFLAG         --银监会认定评级是否更为审慎
                ,'0'                                                    AS LARGEFLUCFLAG            --是否波动性较大
                ,'0'                                                    AS LIQUEXPOFLAG             --是否清算过程中风险暴露
                ,'0'                                                    AS PAYMENTDEALFLAG          --是否货款对付模式
                ,0                                                      AS DELAYTRADINGDAYS         --延迟交易天数
                ,'0'                                                    AS SECURITIESFLAG           --有价证券标识
                ,''                                                      AS SECUISSUERID             --证券发行人ID
                ,''                                                      AS RATINGDURATIONTYPE       --评级期限类型
                ,''                                                      AS SECUISSUERATING          --证券发行等级
                ,0                                                      AS SECURESIDUALM            --证券剩余期限
                ,1                                                      AS SECUREVAFREQUENCY        --证券重估频率
                ,'0'                                                    AS CCPTRANFLAG              --是否中央交易对手相关交易
                ,''                                                     AS CCPID                    --中央交易对手ID
                ,'0'                                                    AS QUALCCPFLAG              --是否合格中央交易对手
                ,''                                                     AS BANKROLE                 --银行角色
                ,''                                                     AS CLEARINGMETHOD           --清算方式
                ,'0'                                                    AS BANKASSETFLAG            --是否银行提交资产
                ,''                                                     AS MATCHCONDITIONS          --符合条件情况
                ,'0'                                                    AS SFTFLAG                  --证券融资交易标识
                ,'0'                                                    AS MASTERNETAGREEFLAG       --净额结算主协议标识
                ,''                                                     AS MASTERNETAGREEID         --净额结算主协议ID
                ,''                                                     AS SFTTYPE                  --证券融资交易类型
                ,'0'                                                    AS SECUOWNERTRANSFLAG       --证券所有权是否转移
                ,'0'                                                    AS OTCFLAG                  --场外衍生工具标识
                ,'0'                                                    AS VALIDNETTINGFLAG         --有效净额结算协议标识
                ,''                                                     AS VALIDNETAGREEMENTID      --有效净额结算协议ID
                ,''                                                     AS OTCTYPE                  --场外衍生工具类型
                ,0                                                      AS DEPOSITRISKPERIOD        --保证金风险期间
                ,0                                                      AS MTM                      --重置成本
                ,''                                                     AS MTMCURRENCY              --重置成本币种
                ,''                                                     AS BUYERORSELLER            --买方卖方
                ,'0'                                                    AS QUALROFLAG               --合格参照资产标识
                ,'0'                                                    AS ROISSUERPERFORMFLAG      --参照资产发行人是否能履约
                ,'0'                                                    AS BUYERINSOLVENCYFLAG      --信用保护买方是否破产
                ,0                                                      AS NONPAYMENTFEES           --尚未支付费用
                ,'0'                                                    AS RETAILEXPOFLAG           --零售暴露标识                   默认：0-否
                ,''                                                     AS RETAILCLAIMTYPE          --零售债权类型
                ,'0'                                                    AS MORTGAGETYPE             --住房抵押贷款类型
                ,0                                                      AS DEBTORNUMBER             --借款人个数
                ,1                                                      AS EXPONUMBER               --风险暴露个数
                ,''                                                     AS PDPOOLMODELID            --PD分池模型ID
                ,''                                                     AS LGDPOOLMODELID           --LGD分池模型ID
                ,''                                                     AS CCFPOOLMODELID           --CCF分池模型ID
                ,''                                                     AS PDPOOLID                 --所属PD池ID
                ,''                                                     AS LGDPOOLID                --所属LGD池ID
                ,''                                                     AS CCFPOOLID                --所属CCF池ID
                ,'0'                                                    AS ABSUAFLAG                --资产证券化基础资产标识
                ,''                                                     AS ABSPOOLID                --证券化资产池ID
                ,0                                                      AS ABSPROPORTION            --资产证券化比重
                ,''                                                     AS GROUPID                  --分组编号
                ,T3.SORTNO                                              AS ORGSORTNO                --所属机构排序号

    FROM        RWA.RWA_WS_PRIVATE_BOND T1
    INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T5
    ON          T1.SUPPORGID=T5.ORGID
    AND         T5.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T5.SUPPTMPLID='M-0110'
    AND         T5.SUBMITFLAG='1'
    LEFT  JOIN   RWA.ORG_INFO T3
    ON           T1.YWSSJGDM = T3.ORGID
    LEFT  JOIN   RWA.CODE_LIBRARY T4
    ON           T1.RZZJHYTXDM = T4.ITEMNO
    AND         T4.CODENO = 'IndustryType'
    WHERE       T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T1.ROWID IN(
                    SELECT MAX(BOND.ROWID)
                      FROM RWA.RWA_WS_PRIVATE_BOND BOND
                INNER JOIN RWA.RWA_WP_DATASUPPLEMENT T5
                        ON BOND.SUPPORGID=T5.ORGID
                       AND T5.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
                       AND T5.SUPPTMPLID='M-0110'
                       AND T5.SUBMITFLAG='1'
                     WHERE BOND.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
                  GROUP BY BOND.ZQID
                )
    ;
    COMMIT;


    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_SMZ_EXPOSURE;
    --Dbms_output.Put_line('RWA_SMZ_EXPOSURE表当前插入的信用卡系统数据记录为: ' ||TO_CHAR( v_count1 - v_count) || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '私募债-信用风险暴露('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_SMZ_EXPOSURE;
/

