CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_MARKETEXPOSURESTD(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_MARKETEXPOSURESTD
    实现功能:汇总标准法暴露表，插入所有标准法暴露信息
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2016-07-07
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_LC_MARKETEXPOSURESTD|理财债券标准法暴露表
    源  表2 :RWA_DEV.RWA_ZQ_MARKETEXPOSURESTD|债券标准法暴露表
    源  表3 :RWA_DEV.RWA_WH_MARKETEXPOSURESTD|外汇标准法暴露表
    源  表4 :RWA_DEV.RWA_ZJ_MARKETEXPOSURESTD|资金标准法暴露表
    源  表5 :RWA_DEV.RWA_EI_CLIENT|客户汇总表
    
    
    目标表1 :RWA_DEV.RWA_EI_MARKETEXPOSURESTD|汇总标准法暴露表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_MARKETEXPOSURESTD';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_MARKETEXPOSURESTD DROP PARTITION MARKETEXPOSURESTD' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --首次分区truncate会出现2149异常
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '汇总参与主体表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --新增一个当前日期下的分区
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_MARKETEXPOSURESTD ADD PARTITION MARKETEXPOSURESTD' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*插入理财债券的标准法暴露信息*/
   /*INSERT INTO RWA_DEV.RWA_EI_MARKETEXPOSURESTD(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,EXPOSUREID                            --风险暴露ID
                ,BOOKTYPE                              --账户类别
                ,INSTRUMENTSID                         --金融工具ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ORGID                                 --所属机构ID
                ,ORGNAME                               --所属机构名称
                ,ORGTYPE                               --所属机构类型
                ,MARKETRISKTYPE                        --市场风险类型
                ,INTERATERISKTYPE                      --利率风险类型
                ,EQUITYRISKTYPE                        --股票风险类型
                ,EXCHANGERISKTYPE                      --外汇风险类型
                ,COMMODITYNAME                         --商品种类名称
                ,OPTIONRISKTYPE                        --期权风险类型
                ,ISSUERID                              --发行人ID
                ,ISSUERNAME                            --发行人名称
                ,ISSUERTYPE                            --发行人大类
                ,ISSUERSUBTYPE                         --发行人小类
                ,ISSUERREGISTSTATE                     --发行人注册国家
                ,ISSUERRCERATING                       --发行人境外注册地外部评级
                ,SMBFLAG                               --小微企业标识
                ,UNDERBONDFLAG                         --是否承销债券
                ,PAYMENTDATE                           --缴款日
                ,SECURITIESTYPE                        --证券类别
                ,BONDISSUEINTENT     									 --债券发行目的
                ,CLAIMSLEVEL                           --债权级别
                ,REABSFLAG                             --再资产证券化标识
                ,ORIGINATORFLAG                        --是否发起机构
                ,SECURITIESERATING                     --证券外部评级
                ,STOCKCODE                             --股票/股指代码
                ,STOCKMARKET                           --交易市场
                ,EXCHANGEAREA                          --交易地区
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,OPTIONUNDERLYINGFLAG                  --是否期权基础工具
                ,OPTIONUNDERLYINGTYPE                  --期权基础工具类型
                ,OPTIONID                              --期权工具ID
                ,VOLATILITY                            --波动率
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,NEXTREPRICEDATE                       --下次重定价日
                ,NEXTREPRICEM                          --下次重定价期限
                ,RATETYPE                              --利率类型
                ,COUPONRATE                            --票面利率
                ,MODIFIEDDURATION                      --修正久期
                ,POSITIONTYPE                          --头寸属性
                ,POSITION                              --头寸
                ,CURRENCY                              --币种
                ,OPTIONUNDERLYINGNAME									 --期权基础工具名称
                ,ORGSORTNO														 --机构排序号

    )
    SELECT
                DATADATE                                     AS DATADATE                 --数据日期
                ,DATANO												     	         AS DATANO                   --数据流水号
                ,EXPOSUREID                    				 	     AS EXPOSUREID               --风险暴露ID
                ,BOOKTYPE					                           AS BOOKTYPE                 --账户类别                					 默认：交易账户(02)
                ,INSTRUMENTSID         	                     AS INSTRUMENTSID            --金融工具ID
                ,INSTRUMENTSTYPE                    			   AS INSTRUMENTSTYPE          --金融工具类型
                ,ORGID		 						                       AS ORGID                    --所属机构ID
                ,ORGNAME										                 AS ORGNAME                  --所属机构名称
                ,ORGTYPE						                         AS ORGTYPE                  --所属机构类型            					 默认：境内机构(01)
                ,MARKETRISKTYPE			                         AS MARKETRISKTYPE           --市场风险类型            					 默认：空
                ,INTERATERISKTYPE								             AS INTERATERISKTYPE         --利率风险类型            					 默认：债券(01)
                ,EQUITYRISKTYPE												       AS EQUITYRISKTYPE           --股票风险类型            					 默认：空
                ,EXCHANGERISKTYPE							               AS EXCHANGERISKTYPE         --外汇风险类型            					 默认：空
                ,COMMODITYNAME															 AS COMMODITYNAME       		 --商品种类名称            					 默认：空
                ,OPTIONRISKTYPE                              AS OPTIONRISKTYPE           --期权风险类型            					 默认：空
                ,ISSUERID                            		     AS ISSUERID                 --发行人ID
                ,ISSUERNAME	                                 AS ISSUERNAME               --发行人名称
                ,ISSUERTYPE	                                 AS ISSUERTYPE               --发行人大类
                ,ISSUERSUBTYPE                               AS ISSUERSUBTYPE            --发行人小类
                ,ISSUERREGISTSTATE                           AS ISSUERREGISTSTATE        --发行人注册国家
                ,ISSUERRCERATING	                           AS ISSUERRCERATING          --发行人境外注册地外部评级
                ,SMBFLAG	                                   AS SMBFLAG                  --小微企业标识
                ,UNDERBONDFLAG                               AS UNDERBONDFLAG            --是否承销债券            					 默认：否(0)
                ,PAYMENTDATE	                               AS PAYMENTDATE              --缴款日                  					 默认：空
                ,SECURITIESTYPE                           	 AS SECURITIESTYPE           --证券类别
                ,BONDISSUEINTENT													   AS BONDISSUEINTENT     		 --债券发行目的
                ,CLAIMSLEVEL	                               AS CLAIMSLEVEL              --债权级别                					 债券种类2＝次级债券(20)，则债权级别＝次级债权(02)；否则为高级债权(01)
                ,REABSFLAG                                   AS REABSFLAG                --再资产证券化标识
                ,ORIGINATORFLAG                              AS ORIGINATORFLAG           --是否发起机构
                ,SECURITIESERATING                           AS SECURITIESERATING        --证券外部评级
                ,STOCKCODE                                   AS STOCKCODE                --股票/股指代码           					 默认：空
                ,STOCKMARKET                                 AS STOCKMARKET              --交易市场                					 默认：空
                ,EXCHANGEAREA                                AS EXCHANGEAREA             --交易地区                					 默认：空
                ,STRUCTURALEXPOFLAG                          AS STRUCTURALEXPOFLAG       --是否结构性敞口          					 默认：空
                ,OPTIONUNDERLYINGFLAG                        AS OPTIONUNDERLYINGFLAG     --是否期权基础工具        					 默认：否(0)
                ,OPTIONUNDERLYINGTYPE                        AS OPTIONUNDERLYINGTYPE     --期权基础工具类型        					 默认：空
                ,OPTIONID                                    AS OPTIONID                 --期权工具ID              					 默认：空
                ,VOLATILITY                                  AS VOLATILITY               --波动率                  					 默认：空
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS DUEDATE                  --到期日期
                ,ORIGINALMATURITY                            AS ORIGINALMATURITY         --原始期限
                ,RESIDUALM                                   AS RESIDUALM                --剩余期限
                ,TO_CHAR(TO_DATE(NEXTREPRICEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                								                             AS NEXTREPRICEDATE          --下次重定价日
                ,NEXTREPRICEM                                AS NEXTREPRICEM             --下次重定价期限
                ,RATETYPE                                    AS RATETYPE                 --利率类型
                ,COUPONRATE                                  AS COUPONRATE               --票面利率
                ,MODIFIEDDURATION                            AS MODIFIEDDURATION         --修正久期
                ,POSITIONTYPE                                AS POSITIONTYPE             --头寸属性                					 默认：多头(01)
                ,POSITION                                    AS POSITION                 --头寸
                ,CURRENCY                                    AS CURRENCY                 --币种
                ,OPTIONUNDERLYINGNAME									 			 AS OPTIONUNDERLYINGNAME		 --期权基础工具名称
                ,ORGSORTNO														 			 AS ORGSORTNO								 --机构排序号

    FROM				RWA_DEV.RWA_LC_MARKETEXPOSURESTD
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  ;

    COMMIT;*/


    /*插入债券的标准法暴露信息*/
   /* INSERT INTO RWA_DEV.RWA_EI_MARKETEXPOSURESTD(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,EXPOSUREID                            --风险暴露ID
                ,BOOKTYPE                              --账户类别
                ,INSTRUMENTSID                         --金融工具ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ORGID                                 --所属机构ID
                ,ORGNAME                               --所属机构名称
                ,ORGTYPE                               --所属机构类型
                ,MARKETRISKTYPE                        --市场风险类型
                ,INTERATERISKTYPE                      --利率风险类型
                ,EQUITYRISKTYPE                        --股票风险类型
                ,EXCHANGERISKTYPE                      --外汇风险类型
                ,COMMODITYNAME                         --商品种类名称
                ,OPTIONRISKTYPE                        --期权风险类型
                ,ISSUERID                              --发行人ID
                ,ISSUERNAME                            --发行人名称
                ,ISSUERTYPE                            --发行人大类
                ,ISSUERSUBTYPE                         --发行人小类
                ,ISSUERREGISTSTATE                     --发行人注册国家
                ,ISSUERRCERATING                       --发行人境外注册地外部评级
                ,SMBFLAG                               --小微企业标识
                ,UNDERBONDFLAG                         --是否承销债券
                ,PAYMENTDATE                           --缴款日
                ,SECURITIESTYPE                        --证券类别
                ,BONDISSUEINTENT     									 --债券发行目的
                ,CLAIMSLEVEL                           --债权级别
                ,REABSFLAG                             --再资产证券化标识
                ,ORIGINATORFLAG                        --是否发起机构
                ,SECURITIESERATING                     --证券外部评级
                ,STOCKCODE                             --股票/股指代码
                ,STOCKMARKET                           --交易市场
                ,EXCHANGEAREA                          --交易地区
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,OPTIONUNDERLYINGFLAG                  --是否期权基础工具
                ,OPTIONUNDERLYINGTYPE                  --期权基础工具类型
                ,OPTIONID                              --期权工具ID
                ,VOLATILITY                            --波动率
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,NEXTREPRICEDATE                       --下次重定价日
                ,NEXTREPRICEM                          --下次重定价期限
                ,RATETYPE                              --利率类型
                ,COUPONRATE                            --票面利率
                ,MODIFIEDDURATION                      --修正久期
                ,POSITIONTYPE                          --头寸属性
                ,POSITION                              --头寸
                ,CURRENCY                              --币种
                ,OPTIONUNDERLYINGNAME									 --期权基础工具名称
                ,ORGSORTNO														 --机构排序号

    )
    SELECT
                DATADATE                                     AS DATADATE                 --数据日期
                ,DATANO												     	         AS DATANO                   --数据流水号
                ,EXPOSUREID                    				 	     AS EXPOSUREID               --风险暴露ID
                ,BOOKTYPE					                           AS BOOKTYPE                 --账户类别                					 默认：交易账户(02)
                ,INSTRUMENTSID         	                     AS INSTRUMENTSID            --金融工具ID
                ,INSTRUMENTSTYPE                    			   AS INSTRUMENTSTYPE          --金融工具类型
                ,ORGID		 						                       AS ORGID                    --所属机构ID
                ,ORGNAME										                 AS ORGNAME                  --所属机构名称
                ,ORGTYPE						                         AS ORGTYPE                  --所属机构类型            					 默认：境内机构(01)
                ,MARKETRISKTYPE			                         AS MARKETRISKTYPE           --市场风险类型            					 默认：空
                ,INTERATERISKTYPE								             AS INTERATERISKTYPE         --利率风险类型            					 默认：债券(01)
                ,EQUITYRISKTYPE												       AS EQUITYRISKTYPE           --股票风险类型            					 默认：空
                ,EXCHANGERISKTYPE							               AS EXCHANGERISKTYPE         --外汇风险类型            					 默认：空
                ,COMMODITYNAME															 AS COMMODITYNAME       		 --商品种类名称            					 默认：空
                ,OPTIONRISKTYPE                              AS OPTIONRISKTYPE           --期权风险类型            					 默认：空
                ,ISSUERID                            		     AS ISSUERID                 --发行人ID
                ,ISSUERNAME	                                 AS ISSUERNAME               --发行人名称
                ,ISSUERTYPE	                                 AS ISSUERTYPE               --发行人大类
                ,ISSUERSUBTYPE                               AS ISSUERSUBTYPE            --发行人小类
                ,ISSUERREGISTSTATE                           AS ISSUERREGISTSTATE        --发行人注册国家
                ,ISSUERRCERATING	                           AS ISSUERRCERATING          --发行人境外注册地外部评级
                ,SMBFLAG	                                   AS SMBFLAG                  --小微企业标识
                ,UNDERBONDFLAG                               AS UNDERBONDFLAG            --是否承销债券            					 默认：否(0)
                ,PAYMENTDATE	                               AS PAYMENTDATE              --缴款日                  					 默认：空
                ,SECURITIESTYPE                           	 AS SECURITIESTYPE           --证券类别
                ,BONDISSUEINTENT													   AS BONDISSUEINTENT     		 --债券发行目的
                ,CLAIMSLEVEL	                               AS CLAIMSLEVEL              --债权级别                					 债券种类2＝次级债券(20)，则债权级别＝次级债权(02)；否则为高级债权(01)
                ,REABSFLAG                                   AS REABSFLAG                --再资产证券化标识
                ,ORIGINATORFLAG                              AS ORIGINATORFLAG           --是否发起机构
                ,SECURITIESERATING                           AS SECURITIESERATING        --证券外部评级
                ,STOCKCODE                                   AS STOCKCODE                --股票/股指代码           					 默认：空
                ,STOCKMARKET                                 AS STOCKMARKET              --交易市场                					 默认：空
                ,EXCHANGEAREA                                AS EXCHANGEAREA             --交易地区                					 默认：空
                ,STRUCTURALEXPOFLAG                          AS STRUCTURALEXPOFLAG       --是否结构性敞口          					 默认：空
                ,OPTIONUNDERLYINGFLAG                        AS OPTIONUNDERLYINGFLAG     --是否期权基础工具        					 默认：否(0)
                ,OPTIONUNDERLYINGTYPE                        AS OPTIONUNDERLYINGTYPE     --期权基础工具类型        					 默认：空
                ,OPTIONID                                    AS OPTIONID                 --期权工具ID              					 默认：空
                ,VOLATILITY                                  AS VOLATILITY               --波动率                  					 默认：空
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS DUEDATE                  --到期日期
                ,ORIGINALMATURITY                            AS ORIGINALMATURITY         --原始期限
                ,RESIDUALM                                   AS RESIDUALM                --剩余期限
                ,TO_CHAR(TO_DATE(NEXTREPRICEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                								                             AS NEXTREPRICEDATE          --下次重定价日
                ,NEXTREPRICEM                                AS NEXTREPRICEM             --下次重定价期限
                ,RATETYPE                                    AS RATETYPE                 --利率类型
                ,COUPONRATE                                  AS COUPONRATE               --票面利率
                ,MODIFIEDDURATION                            AS MODIFIEDDURATION         --修正久期
                ,POSITIONTYPE                                AS POSITIONTYPE             --头寸属性                					 默认：多头(01)
                ,POSITION                                    AS POSITION                 --头寸
                ,CURRENCY                                    AS CURRENCY                 --币种
                ,OPTIONUNDERLYINGNAME									 			 AS OPTIONUNDERLYINGNAME		 --期权基础工具名称
                ,ORGSORTNO														 			 AS ORGSORTNO								 --机构排序号

    FROM				RWA_DEV.RWA_ZQ_MARKETEXPOSURESTD
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  ;

    COMMIT;*/


    /*插入外汇的标准法暴露信息*/
    INSERT INTO RWA_DEV.RWA_EI_MARKETEXPOSURESTD(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,EXPOSUREID                            --风险暴露ID
                ,BOOKTYPE                              --账户类别
                ,INSTRUMENTSID                         --金融工具ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ORGID                                 --所属机构ID
                ,ORGNAME                               --所属机构名称
                ,ORGTYPE                               --所属机构类型
                ,MARKETRISKTYPE                        --市场风险类型
                ,INTERATERISKTYPE                      --利率风险类型
                ,EQUITYRISKTYPE                        --股票风险类型
                ,EXCHANGERISKTYPE                      --外汇风险类型
                ,COMMODITYNAME                         --商品种类名称
                ,OPTIONRISKTYPE                        --期权风险类型
                ,ISSUERID                              --发行人ID
                ,ISSUERNAME                            --发行人名称
                ,ISSUERTYPE                            --发行人大类
                ,ISSUERSUBTYPE                         --发行人小类
                ,ISSUERREGISTSTATE                     --发行人注册国家
                ,ISSUERRCERATING                       --发行人境外注册地外部评级
                ,SMBFLAG                               --小微企业标识
                ,UNDERBONDFLAG                         --是否承销债券
                ,PAYMENTDATE                           --缴款日
                ,SECURITIESTYPE                        --证券类别
                ,BONDISSUEINTENT     									 --债券发行目的
                ,CLAIMSLEVEL                           --债权级别
                ,REABSFLAG                             --再资产证券化标识
                ,ORIGINATORFLAG                        --是否发起机构
                ,SECURITIESERATING                     --证券外部评级
                ,STOCKCODE                             --股票/股指代码
                ,STOCKMARKET                           --交易市场
                ,EXCHANGEAREA                          --交易地区
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,OPTIONUNDERLYINGFLAG                  --是否期权基础工具
                ,OPTIONUNDERLYINGTYPE                  --期权基础工具类型
                ,OPTIONID                              --期权工具ID
                ,VOLATILITY                            --波动率
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,NEXTREPRICEDATE                       --下次重定价日
                ,NEXTREPRICEM                          --下次重定价期限
                ,RATETYPE                              --利率类型
                ,COUPONRATE                            --票面利率
                ,MODIFIEDDURATION                      --修正久期
                ,POSITIONTYPE                          --头寸属性
                ,POSITION                              --头寸
                ,CURRENCY                              --币种
                ,OPTIONUNDERLYINGNAME									 --期权基础工具名称
                ,ORGSORTNO														 --机构排序号

    )
    SELECT
                DATADATE                                     AS DATADATE                 --数据日期
                ,DATANO												     	         AS DATANO                   --数据流水号
                ,EXPOSUREID                    				 	     AS EXPOSUREID               --风险暴露ID
                ,BOOKTYPE					                           AS BOOKTYPE                 --账户类别                					 默认：交易账户(02)
                ,INSTRUMENTSID         	                     AS INSTRUMENTSID            --金融工具ID
                ,INSTRUMENTSTYPE                    			   AS INSTRUMENTSTYPE          --金融工具类型
                ,ORGID		 						                       AS ORGID                    --所属机构ID
                ,ORGNAME										                 AS ORGNAME                  --所属机构名称
                ,ORGTYPE						                         AS ORGTYPE                  --所属机构类型            					 默认：境内机构(01)
                ,MARKETRISKTYPE			                         AS MARKETRISKTYPE           --市场风险类型            					 默认：空
                ,INTERATERISKTYPE								             AS INTERATERISKTYPE         --利率风险类型            					 默认：债券(01)
                ,EQUITYRISKTYPE												       AS EQUITYRISKTYPE           --股票风险类型            					 默认：空
                ,EXCHANGERISKTYPE							               AS EXCHANGERISKTYPE         --外汇风险类型            					 默认：空
                ,COMMODITYNAME															 AS COMMODITYNAME       		 --商品种类名称            					 默认：空
                ,OPTIONRISKTYPE                              AS OPTIONRISKTYPE           --期权风险类型            					 默认：空
                ,ISSUERID                            		     AS ISSUERID                 --发行人ID
                ,ISSUERNAME	                                 AS ISSUERNAME               --发行人名称
                ,ISSUERTYPE	                                 AS ISSUERTYPE               --发行人大类
                ,ISSUERSUBTYPE                               AS ISSUERSUBTYPE            --发行人小类
                ,ISSUERREGISTSTATE                           AS ISSUERREGISTSTATE        --发行人注册国家
                ,ISSUERRCERATING	                           AS ISSUERRCERATING          --发行人境外注册地外部评级
                ,SMBFLAG	                                   AS SMBFLAG                  --小微企业标识
                ,UNDERBONDFLAG                               AS UNDERBONDFLAG            --是否承销债券            					 默认：否(0)
                ,PAYMENTDATE	                               AS PAYMENTDATE              --缴款日                  					 默认：空
                ,SECURITIESTYPE                           	 AS SECURITIESTYPE           --证券类别
                ,BONDISSUEINTENT													   AS BONDISSUEINTENT     		 --债券发行目的
                ,CLAIMSLEVEL	                               AS CLAIMSLEVEL              --债权级别                					 债券种类2＝次级债券(20)，则债权级别＝次级债权(02)；否则为高级债权(01)
                ,REABSFLAG                                   AS REABSFLAG                --再资产证券化标识
                ,ORIGINATORFLAG                              AS ORIGINATORFLAG           --是否发起机构
                ,SECURITIESERATING                           AS SECURITIESERATING        --证券外部评级
                ,STOCKCODE                                   AS STOCKCODE                --股票/股指代码           					 默认：空
                ,STOCKMARKET                                 AS STOCKMARKET              --交易市场                					 默认：空
                ,EXCHANGEAREA                                AS EXCHANGEAREA             --交易地区                					 默认：空
                ,STRUCTURALEXPOFLAG                          AS STRUCTURALEXPOFLAG       --是否结构性敞口          					 默认：空
                ,OPTIONUNDERLYINGFLAG                        AS OPTIONUNDERLYINGFLAG     --是否期权基础工具        					 默认：否(0)
                ,OPTIONUNDERLYINGTYPE                        AS OPTIONUNDERLYINGTYPE     --期权基础工具类型        					 默认：空
                ,OPTIONID                                    AS OPTIONID                 --期权工具ID              					 默认：空
                ,VOLATILITY                                  AS VOLATILITY               --波动率                  					 默认：空
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS DUEDATE                  --到期日期
                ,ORIGINALMATURITY                            AS ORIGINALMATURITY         --原始期限
                ,RESIDUALM                                   AS RESIDUALM                --剩余期限
                ,TO_CHAR(TO_DATE(NEXTREPRICEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                								                             AS NEXTREPRICEDATE          --下次重定价日
                ,NEXTREPRICEM                                AS NEXTREPRICEM             --下次重定价期限
                ,RATETYPE                                    AS RATETYPE                 --利率类型
                ,COUPONRATE                                  AS COUPONRATE               --票面利率
                ,MODIFIEDDURATION                            AS MODIFIEDDURATION         --修正久期
                ,POSITIONTYPE                                AS POSITIONTYPE             --头寸属性                					 默认：多头(01)
                ,POSITION                                    AS POSITION                 --头寸
                ,CURRENCY                                    AS CURRENCY                 --币种
                ,OPTIONUNDERLYINGNAME									 			 AS OPTIONUNDERLYINGNAME		 --期权基础工具名称
                ,ORGSORTNO														 			 AS ORGSORTNO								 --机构排序号

    FROM				RWA_DEV.RWA_WH_MARKETEXPOSURESTD
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  ;

		COMMIT;
    
    
    /*插入外汇的标准法暴露信息*/
    INSERT INTO RWA_DEV.RWA_EI_MARKETEXPOSURESTD(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,EXPOSUREID                            --风险暴露ID
                ,BOOKTYPE                              --账户类别
                ,INSTRUMENTSID                         --金融工具ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ORGID                                 --所属机构ID
                ,ORGNAME                               --所属机构名称
                ,ORGTYPE                               --所属机构类型
                ,MARKETRISKTYPE                        --市场风险类型
                ,INTERATERISKTYPE                      --利率风险类型
                ,EQUITYRISKTYPE                        --股票风险类型
                ,EXCHANGERISKTYPE                      --外汇风险类型
                ,COMMODITYNAME                         --商品种类名称
                ,OPTIONRISKTYPE                        --期权风险类型
                ,ISSUERID                              --发行人ID
                ,ISSUERNAME                            --发行人名称
                ,ISSUERTYPE                            --发行人大类
                ,ISSUERSUBTYPE                         --发行人小类
                ,ISSUERREGISTSTATE                     --发行人注册国家
                ,ISSUERRCERATING                       --发行人境外注册地外部评级
                ,SMBFLAG                               --小微企业标识
                ,UNDERBONDFLAG                         --是否承销债券
                ,PAYMENTDATE                           --缴款日
                ,SECURITIESTYPE                        --证券类别
                ,BONDISSUEINTENT                       --债券发行目的
                ,CLAIMSLEVEL                           --债权级别
                ,REABSFLAG                             --再资产证券化标识
                ,ORIGINATORFLAG                        --是否发起机构
                ,SECURITIESERATING                     --证券外部评级
                ,STOCKCODE                             --股票/股指代码
                ,STOCKMARKET                           --交易市场
                ,EXCHANGEAREA                          --交易地区
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,OPTIONUNDERLYINGFLAG                  --是否期权基础工具
                ,OPTIONUNDERLYINGTYPE                  --期权基础工具类型
                ,OPTIONID                              --期权工具ID
                ,VOLATILITY                            --波动率
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,NEXTREPRICEDATE                       --下次重定价日
                ,NEXTREPRICEM                          --下次重定价期限
                ,RATETYPE                              --利率类型
                ,COUPONRATE                            --票面利率
                ,MODIFIEDDURATION                      --修正久期
                ,POSITIONTYPE                          --头寸属性
                ,POSITION                              --头寸
                ,CURRENCY                              --币种
                ,OPTIONUNDERLYINGNAME                  --期权基础工具名称
                ,ORGSORTNO                             --机构排序号

    )
    SELECT
                T1.DATADATE                                     AS DATADATE                 --数据日期
                ,T1.DATANO                                      AS DATANO                   --数据流水号
                ,T1.EXPOSUREID                                  AS EXPOSUREID               --风险暴露ID
                ,T1.BOOKTYPE                                    AS BOOKTYPE                 --账户类别                          默认：交易账户(02)
                ,T1.INSTRUMENTSID                               AS INSTRUMENTSID            --金融工具ID
                ,T1.INSTRUMENTSTYPE                             AS INSTRUMENTSTYPE          --金融工具类型
                ,T1.ORGID                                       AS ORGID                    --所属机构ID
                ,T1.ORGNAME                                     AS ORGNAME                  --所属机构名称
                ,T1.ORGTYPE                                     AS ORGTYPE                  --所属机构类型                      默认：境内机构(01)
                ,T1.MARKETRISKTYPE                              AS MARKETRISKTYPE           --市场风险类型                      默认：空
                ,T1.INTERATERISKTYPE                            AS INTERATERISKTYPE         --利率风险类型                      默认：债券(01)
                ,T1.EQUITYRISKTYPE                              AS EQUITYRISKTYPE           --股票风险类型                      默认：空
                ,T1.EXCHANGERISKTYPE                            AS EXCHANGERISKTYPE         --外汇风险类型                      默认：空
                ,T1.COMMODITYNAME                               AS COMMODITYNAME            --商品种类名称                      默认：空
                ,T1.OPTIONRISKTYPE                              AS OPTIONRISKTYPE           --期权风险类型                      默认：空
                ,T1.ISSUERID                                    AS ISSUERID                 --发行人ID
                ,T1.ISSUERNAME                                  AS ISSUERNAME               --发行人名称
                ,T1.ISSUERTYPE                                  AS ISSUERTYPE               --发行人大类
                ,T1.ISSUERSUBTYPE                               AS ISSUERSUBTYPE            --发行人小类
                ,T1.ISSUERREGISTSTATE                           AS ISSUERREGISTSTATE        --发行人注册国家
                ,T1.ISSUERRCERATING                             AS ISSUERRCERATING          --发行人境外注册地外部评级
                ,T1.SMBFLAG                                     AS SMBFLAG                  --小微企业标识
                ,T1.UNDERBONDFLAG                               AS UNDERBONDFLAG            --是否承销债券                      默认：否(0)
                ,T1.PAYMENTDATE                                 AS PAYMENTDATE              --缴款日                             默认：空
                ,T1.SECURITIESTYPE                              AS SECURITIESTYPE           --证券类别
                ,T1.BONDISSUEINTENT                             AS BONDISSUEINTENT          --债券发行目的
                ,T1.CLAIMSLEVEL                                 AS CLAIMSLEVEL              --债权级别                          债券种类2＝次级债券(20)，则债权级别＝次级债权(02)；否则为高级债权(01)
                ,T1.REABSFLAG                                   AS REABSFLAG                --再资产证券化标识
                ,T1.ORIGINATORFLAG                              AS ORIGINATORFLAG           --是否发起机构
                ,T1.SECURITIESERATING                           AS SECURITIESERATING        --证券外部评级
                ,T1.STOCKCODE                                   AS STOCKCODE                --股票/股指代码                     默认：空
                ,T1.STOCKMARKET                                 AS STOCKMARKET              --交易市场                          默认：空
                ,T1.EXCHANGEAREA                                AS EXCHANGEAREA             --交易地区                          默认：空
                ,T1.STRUCTURALEXPOFLAG                          AS STRUCTURALEXPOFLAG       --是否结构性敞口                     默认：空
                ,T1.OPTIONUNDERLYINGFLAG                        AS OPTIONUNDERLYINGFLAG     --是否期权基础工具                  默认：否(0)
                ,T1.OPTIONUNDERLYINGTYPE                        AS OPTIONUNDERLYINGTYPE     --期权基础工具类型                  默认：空
                ,T1.OPTIONID                                    AS OPTIONID                 --期权工具ID                        默认：空
                ,T1.VOLATILITY                                  AS VOLATILITY               --波动率                             默认：空
                ,TO_CHAR(TO_DATE(T1.STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --起始日期
                ,TO_CHAR(TO_DATE(T1.DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS DUEDATE                  --到期日期
                ,T1.ORIGINALMATURITY                            AS ORIGINALMATURITY         --原始期限
                ,T1.RESIDUALM                                   AS RESIDUALM                --剩余期限
                ,TO_CHAR(TO_DATE(T1.NEXTREPRICEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS NEXTREPRICEDATE          --下次重定价日
                ,T1.NEXTREPRICEM                                AS NEXTREPRICEM             --下次重定价期限
                ,T1.RATETYPE                                    AS RATETYPE                 --利率类型
                ,T1.COUPONRATE                                  AS COUPONRATE               --票面利率
                ,T1.MODIFIEDDURATION                            AS MODIFIEDDURATION         --修正久期
                ,T1.POSITIONTYPE                                AS POSITIONTYPE             --头寸属性                          默认：多头(01)
                ,T1.POSITION                                    AS POSITION                 --头寸
                ,T1.CURRENCY                                    AS CURRENCY                 --币种
                ,T1.OPTIONUNDERLYINGNAME                        AS OPTIONUNDERLYINGNAME     --期权基础工具名称
                ,T1.ORGSORTNO                                   AS ORGSORTNO                --机构排序号

    FROM        RWA_DEV.RWA_ZJ_MARKETEXPOSURESTD T1   --资金标准法暴露表
    WHERE       T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;

    COMMIT;
    
    /*插入外汇的标准法暴露信息*/
    INSERT INTO RWA_DEV.RWA_EI_MARKETEXPOSURESTD(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,EXPOSUREID                            --风险暴露ID
                ,BOOKTYPE                              --账户类别
                ,INSTRUMENTSID                         --金融工具ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ORGID                                 --所属机构ID
                ,ORGNAME                               --所属机构名称
                ,ORGTYPE                               --所属机构类型
                ,MARKETRISKTYPE                        --市场风险类型
                ,INTERATERISKTYPE                      --利率风险类型
                ,EQUITYRISKTYPE                        --股票风险类型
                ,EXCHANGERISKTYPE                      --外汇风险类型
                ,COMMODITYNAME                         --商品种类名称
                ,OPTIONRISKTYPE                        --期权风险类型
                ,ISSUERID                              --发行人ID
                ,ISSUERNAME                            --发行人名称
                ,ISSUERTYPE                            --发行人大类
                ,ISSUERSUBTYPE                         --发行人小类
                ,ISSUERREGISTSTATE                     --发行人注册国家
                ,ISSUERRCERATING                       --发行人境外注册地外部评级
                ,SMBFLAG                               --小微企业标识
                ,UNDERBONDFLAG                         --是否承销债券
                ,PAYMENTDATE                           --缴款日
                ,SECURITIESTYPE                        --证券类别
                ,BONDISSUEINTENT                       --债券发行目的
                ,CLAIMSLEVEL                           --债权级别
                ,REABSFLAG                             --再资产证券化标识
                ,ORIGINATORFLAG                        --是否发起机构
                ,SECURITIESERATING                     --证券外部评级
                ,STOCKCODE                             --股票/股指代码
                ,STOCKMARKET                           --交易市场
                ,EXCHANGEAREA                          --交易地区
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,OPTIONUNDERLYINGFLAG                  --是否期权基础工具
                ,OPTIONUNDERLYINGTYPE                  --期权基础工具类型
                ,OPTIONID                              --期权工具ID
                ,VOLATILITY                            --波动率
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,NEXTREPRICEDATE                       --下次重定价日
                ,NEXTREPRICEM                          --下次重定价期限
                ,RATETYPE                              --利率类型
                ,COUPONRATE                            --票面利率
                ,MODIFIEDDURATION                      --修正久期
                ,POSITIONTYPE                          --头寸属性
                ,POSITION                              --头寸
                ,CURRENCY                              --币种
                ,OPTIONUNDERLYINGNAME                  --期权基础工具名称
                ,ORGSORTNO                             --机构排序号

    )
    SELECT
                T1.DATADATE                                     AS DATADATE                 --数据日期
                ,T1.DATANO                                      AS DATANO                   --数据流水号
                ,T1.EXPOSUREID                                  AS EXPOSUREID               --风险暴露ID
                ,T1.BOOKTYPE                                    AS BOOKTYPE                 --账户类别                          默认：交易账户(02)
                ,T1.INSTRUMENTSID                               AS INSTRUMENTSID            --金融工具ID
                ,T1.INSTRUMENTSTYPE                             AS INSTRUMENTSTYPE          --金融工具类型
                ,T1.ORGID                                       AS ORGID                    --所属机构ID
                ,T1.ORGNAME                                     AS ORGNAME                  --所属机构名称
                ,T1.ORGTYPE                                     AS ORGTYPE                  --所属机构类型                      默认：境内机构(01)
                ,T1.MARKETRISKTYPE                              AS MARKETRISKTYPE           --市场风险类型                      默认：空
                ,T1.INTERATERISKTYPE                            AS INTERATERISKTYPE         --利率风险类型                      默认：债券(01)
                ,T1.EQUITYRISKTYPE                              AS EQUITYRISKTYPE           --股票风险类型                      默认：空
                ,T1.EXCHANGERISKTYPE                            AS EXCHANGERISKTYPE         --外汇风险类型                      默认：空
                ,T1.COMMODITYNAME                               AS COMMODITYNAME            --商品种类名称                      默认：空
                ,T1.OPTIONRISKTYPE                              AS OPTIONRISKTYPE           --期权风险类型                      默认：空
                ,T1.ISSUERID                                    AS ISSUERID                 --发行人ID
                ,T1.ISSUERNAME                                  AS ISSUERNAME               --发行人名称
                ,T1.ISSUERTYPE                                  AS ISSUERTYPE               --发行人大类
                ,T1.ISSUERSUBTYPE                               AS ISSUERSUBTYPE            --发行人小类
                ,T1.ISSUERREGISTSTATE                           AS ISSUERREGISTSTATE        --发行人注册国家
                ,T1.ISSUERRCERATING                             AS ISSUERRCERATING          --发行人境外注册地外部评级
                ,T1.SMBFLAG                                     AS SMBFLAG                  --小微企业标识
                ,T1.UNDERBONDFLAG                               AS UNDERBONDFLAG            --是否承销债券                      默认：否(0)
                ,T1.PAYMENTDATE                                 AS PAYMENTDATE              --缴款日                             默认：空
                ,T1.SECURITIESTYPE                              AS SECURITIESTYPE           --证券类别
                ,T1.BONDISSUEINTENT                             AS BONDISSUEINTENT          --债券发行目的
                ,T1.CLAIMSLEVEL                                 AS CLAIMSLEVEL              --债权级别                          债券种类2＝次级债券(20)，则债权级别＝次级债权(02)；否则为高级债权(01)
                ,T1.REABSFLAG                                   AS REABSFLAG                --再资产证券化标识
                ,T1.ORIGINATORFLAG                              AS ORIGINATORFLAG           --是否发起机构
                ,T1.SECURITIESERATING                           AS SECURITIESERATING        --证券外部评级
                ,T1.STOCKCODE                                   AS STOCKCODE                --股票/股指代码                     默认：空
                ,T1.STOCKMARKET                                 AS STOCKMARKET              --交易市场                          默认：空
                ,T1.EXCHANGEAREA                                AS EXCHANGEAREA             --交易地区                          默认：空
                ,T1.STRUCTURALEXPOFLAG                          AS STRUCTURALEXPOFLAG       --是否结构性敞口                     默认：空
                ,T1.OPTIONUNDERLYINGFLAG                        AS OPTIONUNDERLYINGFLAG     --是否期权基础工具                  默认：否(0)
                ,T1.OPTIONUNDERLYINGTYPE                        AS OPTIONUNDERLYINGTYPE     --期权基础工具类型                  默认：空
                ,T1.OPTIONID                                    AS OPTIONID                 --期权工具ID                        默认：空
                ,T1.VOLATILITY                                  AS VOLATILITY               --波动率                             默认：空
                ,TO_CHAR(TO_DATE(T1.STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --起始日期
                ,TO_CHAR(TO_DATE(T1.DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS DUEDATE                  --到期日期
                ,T1.ORIGINALMATURITY                            AS ORIGINALMATURITY         --原始期限
                ,T1.RESIDUALM                                   AS RESIDUALM                --剩余期限
                ,TO_CHAR(TO_DATE(T1.NEXTREPRICEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS NEXTREPRICEDATE          --下次重定价日
                ,T1.NEXTREPRICEM                                AS NEXTREPRICEM             --下次重定价期限
                ,T1.RATETYPE                                    AS RATETYPE                 --利率类型
                ,T1.COUPONRATE                                  AS COUPONRATE               --票面利率
                ,T1.MODIFIEDDURATION                            AS MODIFIEDDURATION         --修正久期
                ,T1.POSITIONTYPE                                AS POSITIONTYPE             --头寸属性                          默认：多头(01)
                ,T1.POSITION                                    AS POSITION                 --头寸
                ,T1.CURRENCY                                    AS CURRENCY                 --币种
                ,T1.OPTIONUNDERLYINGNAME                        AS OPTIONUNDERLYINGNAME     --期权基础工具名称
                ,T1.ORGSORTNO                                   AS ORGSORTNO                --机构排序号

    FROM        RWA_DEV.RWA_YSP_MARKETEXPOSURESTD T1   --资金标准法暴露表
    WHERE       T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;

    COMMIT;
    

		----------------------------------------------更新市场风险暴露表发行人大小类---------------------------------------------------------
    UPDATE RWA_DEV.RWA_EI_MARKETEXPOSURESTD T1
      SET T1.ISSUERTYPE = (
                           SELECT T2.CLIENTTYPE
                           FROM RWA_DEV.RWA_EI_CLIENT T2
                           WHERE T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                           AND T1.ISSUERID=T2.CLIENTID
                          )
          ,T1.ISSUERSUBTYPE = (
                               SELECT T2.CLIENTSUBTYPE
                               FROM RWA_DEV.RWA_EI_CLIENT T2
                               WHERE T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                               AND T1.ISSUERID=T2.CLIENTID
                              )
    WHERE   T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND   T1.ISSUERTYPE    IS NULL
      AND   T1.ISSUERSUBTYPE IS NULL
    ;
    COMMIT;

    --整理表信息
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_MARKETEXPOSURESTD',partname => 'MARKETEXPOSURESTD'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_MARKETEXPOSURESTD WHERE DATANO = p_data_dt_str;
    
    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功'||'-'||v_count;
		--定义异常
EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '市场风险标准法暴露信息('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_MARKETEXPOSURESTD;
/

