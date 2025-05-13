CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_SMZ_COLLATERAL(
			 											P_DATA_DT_STR	IN	VARCHAR2,		--数据日期
       											P_PO_RTNCODE	OUT	VARCHAR2,		--返回编号
														P_PO_RTNMSG		OUT	VARCHAR2		--返回描述
)
  /*
    存储过程名称:PRO_RWA_SMZ_COLLATERAL
    实现功能:私募债-抵质押,表结构为抵质押品表
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2014-07-08
    单  位	:上海安硕信息技术股份有限公司
    源  表1	:RWA_WS_PRIVATE_BOND|私募债业务补录模板
    目标表	:RWA_SMZ_COLLATERAL|私募债-抵质押品表
    辅助表	:无
    变更记录(修改人|修改时间|修改内容)：
  */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'PRO_RWA_SMZ_COLLATERAL';
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

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_SMZ_COLLATERAL';

    /*有效借据下合同对应的抵质押品信息*/
    INSERT INTO RWA_DEV.RWA_SMZ_COLLATERAL(
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
		)
		SELECT
								TO_DATE(P_DATA_DT_STR,'YYYYMMDD')																  AS DATADATE          	  --数据日期
								,P_DATA_DT_STR																									  AS DATANO              	--数据流水号
								,T1.DBID                                                          AS COLLATERALID        	--抵质押品ID
								,'SMZ'																													  AS SSYSID              	--源系统ID
								,T1.DBID																											    AS SGUARCONTRACTID     	--源担保合同ID
								,T1.DBID										                                      AS SCOLLATERALID       	--源抵质押品ID
								,''																									              AS COLLATERALNAME      	--抵质押品名称
								,T1.CUSTID2																												AS ISSUERID            	--发行人ID
								,T1.CUSTID1																											  AS PROVIDERID          	--提供人ID
								,'06'																															AS CREDITRISKDATATYPE  	--信用风险数据类型
								,T1.DBLX																									        AS GUARANTEEWAY      	  --担保方式
								,''																			                          AS SOURCECOLTYPE     	  --源抵质押品大类
								,''																									              AS SOURCECOLSUBTYPE    	--源抵质押品小类
								,'0'																															AS SPECPURPBONDFLAG    	--是否为收购国有银行不良贷款而发行的债券
								,'0'																															AS QUALFLAGSTD         	--权重法合格标识
								,''																															  AS QUALFLAGFIRB        	--内评初级法合格标识
								,''																												        AS COLLATERALTYPESTD   	--权重法抵质押品类型
								,''																												        AS COLLATERALSDVSSTD   	--权重法抵质押品细分
								,''																																AS COLLATERALTYPEIRB   	--内评法抵质押品类型
								,ROUND(TO_NUMBER(T1.DBJZ),6)															        AS COLLATERALAMOUNT     --抵押总额
								,NVL(T1.DBBZDM,'CNY')																						  AS CURRENCY            	--币种
								,REPLACE(T1.DBQSR,'-','')																					AS STARTDATE           	--起始日期
								,REPLACE(T1.DBDQR,'-','')																					AS DUEDATE             	--到期日期
								,CASE WHEN (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(T1.DBQSR,'yyyy-mm-dd')) / 365<0
                      THEN 0
                      ELSE (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(T1.DBQSR,'yyyy-mm-dd')) / 365
                END																					                      AS ORIGINALMATURITY				 --原始期限									单位：年
								,CASE WHEN (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(p_data_dt_str,'yyyymmdd')) / 365<0
								      THEN 0
								      ELSE (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(p_data_dt_str,'yyyymmdd')) / 365
								END																					                      AS RESIDUALM								 --剩余期限
								,'0'																															AS INTEHAIRCUTSFLAG    	--自行估计折扣系数标识
								,1																																AS INTERNALHC          	--内部折扣系数
								,''																												        AS FCTYPE              	--金融质押品类型
								,'0'																															AS ABSFLAG             	--资产证券化标识
								,''																																AS RATINGDURATIONTYPE  	--评级期限类型
								,''																																AS FCISSUERATING       	--金融质押品发行等级
								,''                                                        		    AS FCISSUERTYPE        	--金融质押品发行人类别
								,''                                                 							AS FCISSUERSTATE       	--金融质押品发行人注册国家
								,NULL                                                             AS FCRESIDUALM         	--金融质押品剩余期限
								,1																															 	AS REVAFREQUENCY       	--重估频率
								,''                                                               AS GROUPID             	--分组编号
		FROM   RWA.RWA_WS_PRIVATE_BOND T1
		INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T2                    --数据补录表
    ON          T1.SUPPORGID=T2.ORGID
    AND         T2.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T2.SUPPTMPLID='M-0110'
    AND         T2.SUBMITFLAG='1'
    WHERE  T1.DBLX NOT IN ('030010','030020','030030','020080','020090')
    AND    T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
		COMMIT;


    /*目标表数据统计*/
    SELECT COUNT(1) INTO v_count FROM RWA_SMZ_COLLATERAL;
    --Dbms_output.Put_line('RWA_SMZ_COLLATERAL表当前插入的数据记录为:' || v_count1 || '条');

		--Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

		p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功'||'-'||v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '私募债-抵质押品(PRO_RWA_SMZ_COLLATERAL)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;

END PRO_RWA_SMZ_COLLATERAL;
/

