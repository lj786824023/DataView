CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_COLLATERAL(
			 											P_DATA_DT_STR	IN	VARCHAR2,		--数据日期
       											P_PO_RTNCODE	OUT	VARCHAR2,		--返回编号
														P_PO_RTNMSG		OUT	VARCHAR2		--返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_COLLATERAL
    实现功能:汇总抵质押品表,插入所有抵质押品信息
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2014-06-01
    单  位	:上海安硕信息技术股份有限公司
    源  表1	:RWA_DEV.RWA_XD_COLLATERAL|信贷抵质押品表
    源  表2	:RWA_DEV.RWA_PJ_COLLATERAL|票据抵质押品表
    源  表3	:RWA_DEV.RWA_TZ_COLLATERAL|投资抵质押品表
    源  表4	:RWA_DEV.RWA_LC_COLLATERAL|理财抵质押品表
    源  表5	:RWA_DEV.RWA_HG_COLLATERAL|回购抵质押品表
    源  表6	:RWA_DEV.RWA_ABS_ISSURE_COLLATERAL|资产证券化抵质押品表
    目标表	:RWA_DEV.RWA_EI_COLLATERAL|汇总抵质押品表
    辅助表	:无
    变更记录(修改人|修改时间|修改内容)：
  */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_COLLATERAL';
  --定义判断值变量
  v_count INTEGER;
  --定义异常变量
  v_raise EXCEPTION;
  --定义临时表名
  v_tabname VARCHAR2(200);
  --定义创建语句
  v_create VARCHAR2(1000) ;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_COLLATERAL DROP PARTITION COLLATERAL' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --首次分区truncate会出现2149异常
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '汇总抵质押品表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --新增一个当前日期下的分区
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_COLLATERAL ADD PARTITION COLLATERAL' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*插入信贷的抵质押品信息*/
    INSERT INTO RWA_DEV.RWA_EI_COLLATERAL(
								 DATADATE          										  --数据日期
								,DATANO                 								--数据流水号
								,COLLATERALID           								--抵质押品ID
								,SSYSID                 								--源系统ID
								,SGUARCONTRACTID        								--源担保合同ID
								,SCOLLATERALID          								--源抵质押品ID
								,COLLATERALNAME         								--抵质押品名称
								,ISSUERID               								--发行人ID
								,PROVIDERID             								--提供人ID
								,CREDITRISKDATATYPE     								--信用风险数据类型
								,GUARANTEEWAY            								--担保方式
								,SOURCECOLTYPE      										--源抵质押品大类
								,SOURCECOLSUBTYPE       								--源抵质押品小类
								,SPECPURPBONDFLAG       								--是否为收购国有银行不良贷款而发行的债券
								,QUALFLAGSTD            								--权重法合格标识
								,QUALFLAGFIRB           								--内评初级法合格标识
								,COLLATERALTYPESTD      								--权重法抵质押品类型
								,COLLATERALSDVSSTD      								--权重法抵质押品细分
								,COLLATERALTYPEIRB      								--内评法抵质押品类型
								,COLLATERALAMOUNT        								--抵押总额
								,CURRENCY               								--币种
								,STARTDATE              								--起始日期
								,DUEDATE                								--到期日期
								,ORIGINALMATURITY       								--原始期限
								,RESIDUALM              								--剩余期限
								,INTEHAIRCUTSFLAG       								--自行估计折扣系数标识
								,INTERNALHC             								--内部折扣系数
								,FCTYPE                 								--金融质押品类型
								,ABSFLAG                								--资产证券化标识
								,RATINGDURATIONTYPE     								--评级期限类型
								,FCISSUERATING          								--金融质押品发行等级
								,FCISSUERTYPE           								--金融质押品发行人类别
								,FCISSUERSTATE          								--金融质押品发行人注册国家
								,FCRESIDUALM            								--金融质押品剩余期限
								,REVAFREQUENCY          								--重估频率
								,GROUPID                								--分组编号
								,RCERating                              --发行人境外注册地外部评级
		)
		SELECT
								 DATADATE          																							    AS DATADATE          	  --数据日期
								,DATANO             																						    AS DATANO              	--数据流水号
								,COLLATERALID       																						    AS COLLATERALID        	--抵质押品ID
								,SSYSID             																								AS SSYSID              	--源系统ID
								,SGUARCONTRACTID    																							  AS SGUARCONTRACTID     	--源担保合同ID
								,SCOLLATERALID      																						    AS SCOLLATERALID       	--源抵质押品ID
								,COLLATERALNAME     																							  AS COLLATERALNAME      	--抵质押品名称
								,ISSUERID           														                    AS ISSUERID            	--发行人ID
								,PROVIDERID         																					      AS PROVIDERID          	--提供人ID
								,CREDITRISKDATATYPE 																						    AS CREDITRISKDATATYPE  	--信用风险数据类型
								,GUARANTEEWAY       																								AS GUARANTEEWAY      	  --担保方式
								,SOURCECOLTYPE      																								AS SOURCECOLTYPE     	  --源抵质押品大类
								,SOURCECOLSUBTYPE   																								AS SOURCECOLSUBTYPE    	--源抵质押品小类
								,SPECPURPBONDFLAG   																								AS SPECPURPBONDFLAG    	--是否为收购国有银行不良贷款而发行的债券
								,QUALFLAGSTD        																								AS QUALFLAGSTD         	--权重法合格标识
								,QUALFLAGFIRB       																	              AS QUALFLAGFIRB        	--内评初级法合格标识
								,COLLATERALTYPESTD  																		            AS COLLATERALTYPESTD   	--权重法抵质押品类型
								,COLLATERALSDVSSTD  																		            AS COLLATERALSDVSSTD   	--权重法抵质押品细分
								,COLLATERALTYPEIRB  																								AS COLLATERALTYPEIRB   	--内评法抵质押品类型
								,COLLATERALAMOUNT   																								AS COLLATERALAMOUNT     --抵押总额                              -
								,CURRENCY           																								AS CURRENCY            	--币种
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')          		AS STARTDATE           	--起始日期
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')            		AS DUEDATE             	--到期日期
								,ORIGINALMATURITY                                                   AS ORIGINALMATURITY    	--原始期限
								,RESIDUALM                                                          AS RESIDUALM           	--剩余期限
								,INTEHAIRCUTSFLAG   																			          AS INTEHAIRCUTSFLAG    	--自行估计折扣系数标识
								,INTERNALHC         																							  AS INTERNALHC          	--内部折扣系数
								,FCTYPE             																								AS FCTYPE              	--金融质押品类型
								,ABSFLAG            																							  AS ABSFLAG             	--资产证券化标识
								,RATINGDURATIONTYPE 																							  AS RATINGDURATIONTYPE  	--评级期限类型
								,FCISSUERATING      																				        AS FCISSUERATING       	--金融质押品发行等级
								,FCISSUERTYPE                                    		                AS FCISSUERTYPE        	--金融质押品发行人类别
								,FCISSUERSTATE                            													AS FCISSUERSTATE       	--金融质押品发行人注册国家
								,FCRESIDUALM                                                        AS FCRESIDUALM         	--金融质押品剩余期限
								,REVAFREQUENCY      																	 				      AS REVAFREQUENCY       	--重估频率
								,GROUPID                                                   					AS GROUPID             	--分组编号
								,RCERating                                                          AS RCERating            --发行人境外注册地外部评级
		FROM   			RWA_DEV.RWA_XD_COLLATERAL
		WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;
		COMMIT;


    /*插入票据的抵质押品信息*/
    INSERT INTO RWA_DEV.RWA_EI_COLLATERAL(
								 DATADATE          										  --数据日期
								,DATANO                 								--数据流水号
								,COLLATERALID           								--抵质押品ID
								,SSYSID                 								--源系统ID
								,SGUARCONTRACTID        								--源担保合同ID
								,SCOLLATERALID          								--源抵质押品ID
								,COLLATERALNAME         								--抵质押品名称
								,ISSUERID               								--发行人ID
								,PROVIDERID             								--提供人ID
								,CREDITRISKDATATYPE     								--信用风险数据类型
								,GUARANTEEWAY            								--担保方式
								,SOURCECOLTYPE      										--源抵质押品大类
								,SOURCECOLSUBTYPE       								--源抵质押品小类
								,SPECPURPBONDFLAG       								--是否为收购国有银行不良贷款而发行的债券
								,QUALFLAGSTD            								--权重法合格标识
								,QUALFLAGFIRB           								--内评初级法合格标识
								,COLLATERALTYPESTD      								--权重法抵质押品类型
								,COLLATERALSDVSSTD      								--权重法抵质押品细分
								,COLLATERALTYPEIRB      								--内评法抵质押品类型
								,COLLATERALAMOUNT        								--抵押总额
								,CURRENCY               								--币种
								,STARTDATE              								--起始日期
								,DUEDATE                								--到期日期
								,ORIGINALMATURITY       								--原始期限
								,RESIDUALM              								--剩余期限
								,INTEHAIRCUTSFLAG       								--自行估计折扣系数标识
								,INTERNALHC             								--内部折扣系数
								,FCTYPE                 								--金融质押品类型
								,ABSFLAG                								--资产证券化标识
								,RATINGDURATIONTYPE     								--评级期限类型
								,FCISSUERATING          								--金融质押品发行等级
								,FCISSUERTYPE           								--金融质押品发行人类别
								,FCISSUERSTATE          								--金融质押品发行人注册国家
								,FCRESIDUALM            								--金融质押品剩余期限
								,REVAFREQUENCY          								--重估频率
								,GROUPID                								--分组编号
								,RCERating                              --发行人境外注册地外部评级
		)
		SELECT
								 DATADATE          																							    AS DATADATE          	  --数据日期
								,DATANO             																						    AS DATANO              	--数据流水号
								,COLLATERALID       																						    AS COLLATERALID        	--抵质押品ID
								,SSYSID             																								AS SSYSID              	--源系统ID
								,SGUARCONTRACTID    																							  AS SGUARCONTRACTID     	--源担保合同ID
								,SCOLLATERALID      																						    AS SCOLLATERALID       	--源抵质押品ID
								,COLLATERALNAME     																							  AS COLLATERALNAME      	--抵质押品名称
								,ISSUERID           														                    AS ISSUERID            	--发行人ID
								,PROVIDERID         																					      AS PROVIDERID          	--提供人ID
								,CREDITRISKDATATYPE 																						    AS CREDITRISKDATATYPE  	--信用风险数据类型
								,GUARANTEEWAY       																								AS GUARANTEEWAY      	  --担保方式
								,SOURCECOLTYPE      																								AS SOURCECOLTYPE     	  --源抵质押品大类
								,SOURCECOLSUBTYPE   																								AS SOURCECOLSUBTYPE    	--源抵质押品小类
								,SPECPURPBONDFLAG   																								AS SPECPURPBONDFLAG    	--是否为收购国有银行不良贷款而发行的债券
								,QUALFLAGSTD        																								AS QUALFLAGSTD         	--权重法合格标识
								,QUALFLAGFIRB       																	              AS QUALFLAGFIRB        	--内评初级法合格标识
								,COLLATERALTYPESTD  																		            AS COLLATERALTYPESTD   	--权重法抵质押品类型
								,COLLATERALSDVSSTD  																		            AS COLLATERALSDVSSTD   	--权重法抵质押品细分
								,COLLATERALTYPEIRB  																								AS COLLATERALTYPEIRB   	--内评法抵质押品类型
								,COLLATERALAMOUNT   																								AS COLLATERALAMOUNT     --抵押总额                              -
								,CURRENCY           																								AS CURRENCY            	--币种
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')          		AS STARTDATE           	--起始日期
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')            		AS DUEDATE             	--到期日期
								,ORIGINALMATURITY                                                   AS ORIGINALMATURITY    	--原始期限
								,RESIDUALM                                                          AS RESIDUALM           	--剩余期限
								,INTEHAIRCUTSFLAG   																			          AS INTEHAIRCUTSFLAG    	--自行估计折扣系数标识
								,INTERNALHC         																							  AS INTERNALHC          	--内部折扣系数
								,FCTYPE             																								AS FCTYPE              	--金融质押品类型
								,ABSFLAG            																							  AS ABSFLAG             	--资产证券化标识
								,RATINGDURATIONTYPE 																							  AS RATINGDURATIONTYPE  	--评级期限类型
								,FCISSUERATING      																				        AS FCISSUERATING       	--金融质押品发行等级
								,FCISSUERTYPE                                    		                AS FCISSUERTYPE        	--金融质押品发行人类别
								,FCISSUERSTATE                            													AS FCISSUERSTATE       	--金融质押品发行人注册国家
								,FCRESIDUALM                                                        AS FCRESIDUALM         	--金融质押品剩余期限
								,REVAFREQUENCY      																	 				      AS REVAFREQUENCY       	--重估频率
								,GROUPID                                                   					AS GROUPID             	--分组编号
								,RCERating                                                          AS RCERating            --发行人境外注册地外部评级
		FROM   			RWA_DEV.RWA_PJ_COLLATERAL
		WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;
		COMMIT;


		/*插入理财的抵质押品信息*/
    INSERT INTO RWA_DEV.RWA_EI_COLLATERAL(
								 DATADATE          										  --数据日期
								,DATANO                 								--数据流水号
								,COLLATERALID           								--抵质押品ID
								,SSYSID                 								--源系统ID
								,SGUARCONTRACTID        								--源担保合同ID
								,SCOLLATERALID          								--源抵质押品ID
								,COLLATERALNAME         								--抵质押品名称
								,ISSUERID               								--发行人ID
								,PROVIDERID             								--提供人ID
								,CREDITRISKDATATYPE     								--信用风险数据类型
								,GUARANTEEWAY            								--担保方式
								,SOURCECOLTYPE      										--源抵质押品大类
								,SOURCECOLSUBTYPE       								--源抵质押品小类
								,SPECPURPBONDFLAG       								--是否为收购国有银行不良贷款而发行的债券
								,QUALFLAGSTD            								--权重法合格标识
								,QUALFLAGFIRB           								--内评初级法合格标识
								,COLLATERALTYPESTD      								--权重法抵质押品类型
								,COLLATERALSDVSSTD      								--权重法抵质押品细分
								,COLLATERALTYPEIRB      								--内评法抵质押品类型
								,COLLATERALAMOUNT        								--抵押总额
								,CURRENCY               								--币种
								,STARTDATE              								--起始日期
								,DUEDATE                								--到期日期
								,ORIGINALMATURITY       								--原始期限
								,RESIDUALM              								--剩余期限
								,INTEHAIRCUTSFLAG       								--自行估计折扣系数标识
								,INTERNALHC             								--内部折扣系数
								,FCTYPE                 								--金融质押品类型
								,ABSFLAG                								--资产证券化标识
								,RATINGDURATIONTYPE     								--评级期限类型
								,FCISSUERATING          								--金融质押品发行等级
								,FCISSUERTYPE           								--金融质押品发行人类别
								,FCISSUERSTATE          								--金融质押品发行人注册国家
								,FCRESIDUALM            								--金融质押品剩余期限
								,REVAFREQUENCY          								--重估频率
								,GROUPID                								--分组编号
								,RCERating                              --发行人境外注册地外部评级
		)
		SELECT
								 DATADATE          																							    AS DATADATE          	  --数据日期
								,DATANO             																						    AS DATANO              	--数据流水号
								,COLLATERALID       																						    AS COLLATERALID        	--抵质押品ID
								,SSYSID             																								AS SSYSID              	--源系统ID
								,SGUARCONTRACTID    																							  AS SGUARCONTRACTID     	--源担保合同ID
								,SCOLLATERALID      																						    AS SCOLLATERALID       	--源抵质押品ID
								,COLLATERALNAME     																							  AS COLLATERALNAME      	--抵质押品名称
								,ISSUERID           														                    AS ISSUERID            	--发行人ID
								,PROVIDERID         																					      AS PROVIDERID          	--提供人ID
								,CREDITRISKDATATYPE 																						    AS CREDITRISKDATATYPE  	--信用风险数据类型
								,GUARANTEEWAY       																								AS GUARANTEEWAY      	  --担保方式
								,SOURCECOLTYPE      																								AS SOURCECOLTYPE     	  --源抵质押品大类
								,SOURCECOLSUBTYPE   																								AS SOURCECOLSUBTYPE    	--源抵质押品小类
								,SPECPURPBONDFLAG   																								AS SPECPURPBONDFLAG    	--是否为收购国有银行不良贷款而发行的债券
								,QUALFLAGSTD        																								AS QUALFLAGSTD         	--权重法合格标识
								,QUALFLAGFIRB       																	              AS QUALFLAGFIRB        	--内评初级法合格标识
								,COLLATERALTYPESTD  																		            AS COLLATERALTYPESTD   	--权重法抵质押品类型
								,COLLATERALSDVSSTD  																		            AS COLLATERALSDVSSTD   	--权重法抵质押品细分
								,COLLATERALTYPEIRB  																								AS COLLATERALTYPEIRB   	--内评法抵质押品类型
								,COLLATERALAMOUNT   																								AS COLLATERALAMOUNT     --抵押总额                              -
								,CURRENCY           																								AS CURRENCY            	--币种
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')          		AS STARTDATE           	--起始日期
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')            		AS DUEDATE             	--到期日期
								,ORIGINALMATURITY                                                   AS ORIGINALMATURITY    	--原始期限
								,RESIDUALM                                                          AS RESIDUALM           	--剩余期限
								,INTEHAIRCUTSFLAG   																			          AS INTEHAIRCUTSFLAG    	--自行估计折扣系数标识
								,INTERNALHC         																							  AS INTERNALHC          	--内部折扣系数
								,FCTYPE             																								AS FCTYPE              	--金融质押品类型
								,ABSFLAG            																							  AS ABSFLAG             	--资产证券化标识
								,RATINGDURATIONTYPE 																							  AS RATINGDURATIONTYPE  	--评级期限类型
								,FCISSUERATING      																				        AS FCISSUERATING       	--金融质押品发行等级
								,FCISSUERTYPE                                    		                AS FCISSUERTYPE        	--金融质押品发行人类别
								,FCISSUERSTATE                            													AS FCISSUERSTATE       	--金融质押品发行人注册国家
								,FCRESIDUALM                                                        AS FCRESIDUALM         	--金融质押品剩余期限
								,REVAFREQUENCY      																	 				      AS REVAFREQUENCY       	--重估频率
								,GROUPID                                                   					AS GROUPID             	--分组编号
								,RCERating                                                          AS RCERating            --发行人境外注册地外部评级
		FROM   			RWA_DEV.RWA_LC_COLLATERAL
		WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;
		COMMIT;


		/*插入投资的抵质押品信息*/
    INSERT INTO RWA_DEV.RWA_EI_COLLATERAL(
								 DATADATE          										  --数据日期
								,DATANO                 								--数据流水号
								,COLLATERALID           								--抵质押品ID
								,SSYSID                 								--源系统ID
								,SGUARCONTRACTID        								--源担保合同ID
								,SCOLLATERALID          								--源抵质押品ID
								,COLLATERALNAME         								--抵质押品名称
								,ISSUERID               								--发行人ID
								,PROVIDERID             								--提供人ID
								,CREDITRISKDATATYPE     								--信用风险数据类型
								,GUARANTEEWAY            								--担保方式
								,SOURCECOLTYPE      										--源抵质押品大类
								,SOURCECOLSUBTYPE       								--源抵质押品小类
								,SPECPURPBONDFLAG       								--是否为收购国有银行不良贷款而发行的债券
								,QUALFLAGSTD            								--权重法合格标识
								,QUALFLAGFIRB           								--内评初级法合格标识
								,COLLATERALTYPESTD      								--权重法抵质押品类型
								,COLLATERALSDVSSTD      								--权重法抵质押品细分
								,COLLATERALTYPEIRB      								--内评法抵质押品类型
								,COLLATERALAMOUNT        								--抵押总额
								,CURRENCY               								--币种
								,STARTDATE              								--起始日期
								,DUEDATE                								--到期日期
								,ORIGINALMATURITY       								--原始期限
								,RESIDUALM              								--剩余期限
								,INTEHAIRCUTSFLAG       								--自行估计折扣系数标识
								,INTERNALHC             								--内部折扣系数
								,FCTYPE                 								--金融质押品类型
								,ABSFLAG                								--资产证券化标识
								,RATINGDURATIONTYPE     								--评级期限类型
								,FCISSUERATING          								--金融质押品发行等级
								,FCISSUERTYPE           								--金融质押品发行人类别
								,FCISSUERSTATE          								--金融质押品发行人注册国家
								,FCRESIDUALM            								--金融质押品剩余期限
								,REVAFREQUENCY          								--重估频率
								,GROUPID                								--分组编号
								,RCERating                              --发行人境外注册地外部评级
		)
		SELECT
								 DATADATE          																							    AS DATADATE          	  --数据日期
								,DATANO             																						    AS DATANO              	--数据流水号
								,COLLATERALID       																						    AS COLLATERALID        	--抵质押品ID
								,SSYSID             																								AS SSYSID              	--源系统ID
								,SGUARCONTRACTID    																							  AS SGUARCONTRACTID     	--源担保合同ID
								,SCOLLATERALID      																						    AS SCOLLATERALID       	--源抵质押品ID
								,COLLATERALNAME     																							  AS COLLATERALNAME      	--抵质押品名称
								,ISSUERID           														                    AS ISSUERID            	--发行人ID
								,PROVIDERID         																					      AS PROVIDERID          	--提供人ID
								,CREDITRISKDATATYPE 																						    AS CREDITRISKDATATYPE  	--信用风险数据类型
								,GUARANTEEWAY       																								AS GUARANTEEWAY      	  --担保方式
								,SOURCECOLTYPE      																								AS SOURCECOLTYPE     	  --源抵质押品大类
								,SOURCECOLSUBTYPE   																								AS SOURCECOLSUBTYPE    	--源抵质押品小类
								,SPECPURPBONDFLAG   																								AS SPECPURPBONDFLAG    	--是否为收购国有银行不良贷款而发行的债券
								,QUALFLAGSTD        																								AS QUALFLAGSTD         	--权重法合格标识
								,QUALFLAGFIRB       																	              AS QUALFLAGFIRB        	--内评初级法合格标识
								,COLLATERALTYPESTD  																		            AS COLLATERALTYPESTD   	--权重法抵质押品类型
								,COLLATERALSDVSSTD  																		            AS COLLATERALSDVSSTD   	--权重法抵质押品细分
								,COLLATERALTYPEIRB  																								AS COLLATERALTYPEIRB   	--内评法抵质押品类型
								,COLLATERALAMOUNT   																								AS COLLATERALAMOUNT     --抵押总额                              -
								,CURRENCY           																								AS CURRENCY            	--币种
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')          		AS STARTDATE           	--起始日期
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')            		AS DUEDATE             	--到期日期
								,ORIGINALMATURITY                                                   AS ORIGINALMATURITY    	--原始期限
								,RESIDUALM                                                          AS RESIDUALM           	--剩余期限
								,INTEHAIRCUTSFLAG   																			          AS INTEHAIRCUTSFLAG    	--自行估计折扣系数标识
								,INTERNALHC         																							  AS INTERNALHC          	--内部折扣系数
								,FCTYPE             																								AS FCTYPE              	--金融质押品类型
								,ABSFLAG            																							  AS ABSFLAG             	--资产证券化标识
								,RATINGDURATIONTYPE 																							  AS RATINGDURATIONTYPE  	--评级期限类型
								,FCISSUERATING      																				        AS FCISSUERATING       	--金融质押品发行等级
								,FCISSUERTYPE                                    		                AS FCISSUERTYPE        	--金融质押品发行人类别
								,FCISSUERSTATE                            													AS FCISSUERSTATE       	--金融质押品发行人注册国家
								,FCRESIDUALM                                                        AS FCRESIDUALM         	--金融质押品剩余期限
								,REVAFREQUENCY      																	 				      AS REVAFREQUENCY       	--重估频率
								,GROUPID                                                   					AS GROUPID             	--分组编号
								,RCERating                                                          AS RCERating            --发行人境外注册地外部评级
		FROM   			RWA_DEV.RWA_TZ_COLLATERAL
		WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;
		COMMIT;

		/*插入回购的抵质押品信息*/
    INSERT INTO RWA_DEV.RWA_EI_COLLATERAL(
								 DATADATE          										  --数据日期
								,DATANO                 								--数据流水号
								,COLLATERALID           								--抵质押品ID
								,SSYSID                 								--源系统ID
								,SGUARCONTRACTID        								--源担保合同ID
								,SCOLLATERALID          								--源抵质押品ID
								,COLLATERALNAME         								--抵质押品名称
								,ISSUERID               								--发行人ID
								,PROVIDERID             								--提供人ID
								,CREDITRISKDATATYPE     								--信用风险数据类型
								,GUARANTEEWAY            								--担保方式
								,SOURCECOLTYPE      										--源抵质押品大类
								,SOURCECOLSUBTYPE       								--源抵质押品小类
								,SPECPURPBONDFLAG       								--是否为收购国有银行不良贷款而发行的债券
								,QUALFLAGSTD            								--权重法合格标识
								,QUALFLAGFIRB           								--内评初级法合格标识
								,COLLATERALTYPESTD      								--权重法抵质押品类型
								,COLLATERALSDVSSTD      								--权重法抵质押品细分
								,COLLATERALTYPEIRB      								--内评法抵质押品类型
								,COLLATERALAMOUNT        								--抵押总额
								,CURRENCY               								--币种
								,STARTDATE              								--起始日期
								,DUEDATE                								--到期日期
								,ORIGINALMATURITY       								--原始期限
								,RESIDUALM              								--剩余期限
								,INTEHAIRCUTSFLAG       								--自行估计折扣系数标识
								,INTERNALHC             								--内部折扣系数
								,FCTYPE                 								--金融质押品类型
								,ABSFLAG                								--资产证券化标识
								,RATINGDURATIONTYPE     								--评级期限类型
								,FCISSUERATING          								--金融质押品发行等级
								,FCISSUERTYPE           								--金融质押品发行人类别
								,FCISSUERSTATE          								--金融质押品发行人注册国家
								,FCRESIDUALM            								--金融质押品剩余期限
								,REVAFREQUENCY          								--重估频率
								,GROUPID                								--分组编号
								,RCERating                              --发行人境外注册地外部评级
		)
		SELECT
								 DATADATE          																							    AS DATADATE          	  --数据日期
								,DATANO             																						    AS DATANO              	--数据流水号
								,COLLATERALID       																						    AS COLLATERALID        	--抵质押品ID
								,SSYSID             																								AS SSYSID              	--源系统ID
								,SGUARCONTRACTID    																							  AS SGUARCONTRACTID     	--源担保合同ID
								,SCOLLATERALID      																						    AS SCOLLATERALID       	--源抵质押品ID
								,COLLATERALNAME     																							  AS COLLATERALNAME      	--抵质押品名称
								,ISSUERID           														                    AS ISSUERID            	--发行人ID
								,PROVIDERID         																					      AS PROVIDERID          	--提供人ID
								,CREDITRISKDATATYPE 																						    AS CREDITRISKDATATYPE  	--信用风险数据类型
								,GUARANTEEWAY       																								AS GUARANTEEWAY      	  --担保方式
								,SOURCECOLTYPE      																								AS SOURCECOLTYPE     	  --源抵质押品大类
								,SOURCECOLSUBTYPE   																								AS SOURCECOLSUBTYPE    	--源抵质押品小类
								,SPECPURPBONDFLAG   																								AS SPECPURPBONDFLAG    	--是否为收购国有银行不良贷款而发行的债券
								,QUALFLAGSTD        																								AS QUALFLAGSTD         	--权重法合格标识
								,QUALFLAGFIRB       																	              AS QUALFLAGFIRB        	--内评初级法合格标识
								,COLLATERALTYPESTD  																		            AS COLLATERALTYPESTD   	--权重法抵质押品类型
								,COLLATERALSDVSSTD  																		            AS COLLATERALSDVSSTD   	--权重法抵质押品细分
								,COLLATERALTYPEIRB  																								AS COLLATERALTYPEIRB   	--内评法抵质押品类型
								,COLLATERALAMOUNT   																								AS COLLATERALAMOUNT     --抵押总额                              -
								,CURRENCY           																								AS CURRENCY            	--币种
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')          		AS STARTDATE           	--起始日期
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')            		AS DUEDATE             	--到期日期
								,ORIGINALMATURITY                                                   AS ORIGINALMATURITY    	--原始期限
								,RESIDUALM                                                          AS RESIDUALM           	--剩余期限
								,INTEHAIRCUTSFLAG   																			          AS INTEHAIRCUTSFLAG    	--自行估计折扣系数标识
								,INTERNALHC         																							  AS INTERNALHC          	--内部折扣系数
								,FCTYPE             																								AS FCTYPE              	--金融质押品类型
								,ABSFLAG            																							  AS ABSFLAG             	--资产证券化标识
								,RATINGDURATIONTYPE 																							  AS RATINGDURATIONTYPE  	--评级期限类型
								,FCISSUERATING      																				        AS FCISSUERATING       	--金融质押品发行等级
								,FCISSUERTYPE                                    		                AS FCISSUERTYPE        	--金融质押品发行人类别
								,FCISSUERSTATE                            													AS FCISSUERSTATE       	--金融质押品发行人注册国家
								,FCRESIDUALM                                                        AS FCRESIDUALM         	--金融质押品剩余期限
								,REVAFREQUENCY      																	 				      AS REVAFREQUENCY       	--重估频率
								,GROUPID                                                   					AS GROUPID             	--分组编号
								,RCERating                                                          AS RCERating            --发行人境外注册地外部评级
		FROM   			RWA_DEV.RWA_HG_COLLATERAL
		WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;
		COMMIT;

		/*插入资产证券化的抵质押品信息*/
    INSERT INTO RWA_DEV.RWA_EI_COLLATERAL(
								 DATADATE          										  --数据日期
								,DATANO                 								--数据流水号
								,COLLATERALID           								--抵质押品ID
								,SSYSID                 								--源系统ID
								,SGUARCONTRACTID        								--源担保合同ID
								,SCOLLATERALID          								--源抵质押品ID
								,COLLATERALNAME         								--抵质押品名称
								,ISSUERID               								--发行人ID
								,PROVIDERID             								--提供人ID
								,CREDITRISKDATATYPE     								--信用风险数据类型
								,GUARANTEEWAY            								--担保方式
								,SOURCECOLTYPE      										--源抵质押品大类
								,SOURCECOLSUBTYPE       								--源抵质押品小类
								,SPECPURPBONDFLAG       								--是否为收购国有银行不良贷款而发行的债券
								,QUALFLAGSTD            								--权重法合格标识
								,QUALFLAGFIRB           								--内评初级法合格标识
								,COLLATERALTYPESTD      								--权重法抵质押品类型
								,COLLATERALSDVSSTD      								--权重法抵质押品细分
								,COLLATERALTYPEIRB      								--内评法抵质押品类型
								,COLLATERALAMOUNT        								--抵押总额
								,CURRENCY               								--币种
								,STARTDATE              								--起始日期
								,DUEDATE                								--到期日期
								,ORIGINALMATURITY       								--原始期限
								,RESIDUALM              								--剩余期限
								,INTEHAIRCUTSFLAG       								--自行估计折扣系数标识
								,INTERNALHC             								--内部折扣系数
								,FCTYPE                 								--金融质押品类型
								,ABSFLAG                								--资产证券化标识
								,RATINGDURATIONTYPE     								--评级期限类型
								,FCISSUERATING          								--金融质押品发行等级
								,FCISSUERTYPE           								--金融质押品发行人类别
								,FCISSUERSTATE          								--金融质押品发行人注册国家
								,FCRESIDUALM            								--金融质押品剩余期限
								,REVAFREQUENCY          								--重估频率
								,GROUPID                								--分组编号
								,RCERating                              --发行人境外注册地外部评级
		)
		SELECT
								 DATADATE          																							    AS DATADATE          	  --数据日期
								,DATANO             																						    AS DATANO              	--数据流水号
								,COLLATERALID       																						    AS COLLATERALID        	--抵质押品ID
								,SSYSID             																								AS SSYSID              	--源系统ID
								,SGUARCONTRACTID    																							  AS SGUARCONTRACTID     	--源担保合同ID
								,SCOLLATERALID      																						    AS SCOLLATERALID       	--源抵质押品ID
								,COLLATERALNAME     																							  AS COLLATERALNAME      	--抵质押品名称
								,ISSUERID           														                    AS ISSUERID            	--发行人ID
								,PROVIDERID         																					      AS PROVIDERID          	--提供人ID
								,CREDITRISKDATATYPE 																						    AS CREDITRISKDATATYPE  	--信用风险数据类型
								,GUARANTEEWAY       																								AS GUARANTEEWAY      	  --担保方式
								,SOURCECOLTYPE      																								AS SOURCECOLTYPE     	  --源抵质押品大类
								,SOURCECOLSUBTYPE   																								AS SOURCECOLSUBTYPE    	--源抵质押品小类
								,SPECPURPBONDFLAG   																								AS SPECPURPBONDFLAG    	--是否为收购国有银行不良贷款而发行的债券
								,QUALFLAGSTD        																								AS QUALFLAGSTD         	--权重法合格标识
								,QUALFLAGFIRB       																	              AS QUALFLAGFIRB        	--内评初级法合格标识
								,COLLATERALTYPESTD  																		            AS COLLATERALTYPESTD   	--权重法抵质押品类型
								,COLLATERALSDVSSTD  																		            AS COLLATERALSDVSSTD   	--权重法抵质押品细分
								,COLLATERALTYPEIRB  																								AS COLLATERALTYPEIRB   	--内评法抵质押品类型
								,COLLATERALAMOUNT   																								AS COLLATERALAMOUNT     --抵押总额                              -
								,CURRENCY           																								AS CURRENCY            	--币种
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')          		AS STARTDATE           	--起始日期
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')            		AS DUEDATE             	--到期日期
								,ORIGINALMATURITY                                                   AS ORIGINALMATURITY    	--原始期限
								,RESIDUALM                                                          AS RESIDUALM           	--剩余期限
								,INTEHAIRCUTSFLAG   																			          AS INTEHAIRCUTSFLAG    	--自行估计折扣系数标识
								,INTERNALHC         																							  AS INTERNALHC          	--内部折扣系数
								,FCTYPE             																								AS FCTYPE              	--金融质押品类型
								,ABSFLAG            																							  AS ABSFLAG             	--资产证券化标识
								,RATINGDURATIONTYPE 																							  AS RATINGDURATIONTYPE  	--评级期限类型
								,FCISSUERATING      																				        AS FCISSUERATING       	--金融质押品发行等级
								,FCISSUERTYPE                                    		                AS FCISSUERTYPE        	--金融质押品发行人类别
								,FCISSUERSTATE                            													AS FCISSUERSTATE       	--金融质押品发行人注册国家
								,FCRESIDUALM                                                        AS FCRESIDUALM         	--金融质押品剩余期限
								,REVAFREQUENCY      																	 				      AS REVAFREQUENCY       	--重估频率
								,GROUPID                                                   					AS GROUPID             	--分组编号
								,RCERating                                                          AS RCERating            --发行人境外注册地外部评级
		FROM   			RWA_DEV.RWA_ABS_ISSURE_COLLATERAL
		WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;
		COMMIT;
		--整理表信息
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_COLLATERAL',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_COLLATERAL',partname => 'COLLATERAL'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*目标表数据统计*/
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_COLLATERAL WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.rwa_ei_collateral表当前插入的数据记录为:' || v_count2 || '条');
		--Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

		p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功'||'-'||v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '汇总抵质押品(RWA_DEV.pro_rwa_ei_collateral)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;

END PRO_RWA_EI_COLLATERAL;
/

