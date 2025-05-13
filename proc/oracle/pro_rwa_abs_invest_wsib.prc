CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_INVEST_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ABS_INVEST_WSIB
    实现功能:财务系统-债券投资(资产证券化-投资机构)-补录铺底(从数据源财务系统将业务相关信息全量导入RWA资产证券化-投资机构补录铺底表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-06-20
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.FNS_BND_INFO_B|财务系统债券信息表
    源  表2 :RWA_DEV.FNS_BND_BOOK_B|财务系统账面活动表
    源  表3 :RWA.RWA_WS_ABS_INVEST_EXPOSURE|资产证券化-投资机构-风险暴露补录表
    源  表4 :RWA.CODE_LIBRARY|代码配置表
    目标表1 :RWA.RWA_WSIB_ABS_INVEST_EXPOSURE|资产证券化-投资机构-风险暴露铺底表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_INVEST_WSIB';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    --清空资产证券化-投资机构补录数据铺底铺底表
    DELETE FROM RWA.RWA_WSIB_ABS_INVEST_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 财务系统-资产证券化投资
    INSERT INTO RWA.RWA_WSIB_ABS_INVEST_EXPOSURE(
                DATADATE                               --数据日期
                ,ORGID                             	 	 --机构ID
                ,ZQNM																	 --债券内码
                ,YHJS																	 --银行角色
                ,YWSSJG                 							 --业务所属机构
                ,ZCCDH                  							 --资产池代号
                ,ZCZQHMC                							 --资产证券化名称
                ,TX                     							 --条线
                ,FDSXH                  							 --分档顺序号
                ,DCMC                   							 --档次名称
                ,SFZYXDC                							 --是否最优先档次
                ,ZZCZQHBZ               							 --再资产证券化标识
                ,ZCYE                   							 --资产余额
                ,BZ                     							 --币种
                ,JZZB                   							 --减值准备
                ,FXR                    							 --发行日
                ,DQR                    							 --到期日
                ,WBZXJGDPJJG            							 --外部增信机构的评级机构
                ,FQSHSTGZWBPJ           							 --发起时缓释提供者外部评级
                ,DQHSTGZWBPJ            							 --当前缓释提供者外部评级
                ,XYFXHSSFTGGTBMDDJG     							 --信用风险缓释是否提供给特别目的机构
                ,SFTGXYZCBFYDWBPJ       							 --是否提供信用支持并反映到外部评级
                ,ZQWBPJJG               							 --债券外部评级机构
                ,ZQWBPJQX               							 --债券外部评级期限
                ,ZQWBPJDJ               							 --债券外部评级等级
                ,ZQWBPJRQ															 --债券外部评级日期
                ,ZQHFQRZZJGDM           							 --证券化发起人组织机构代码
                ,FQJGSFNPFYH            							 --发起机构是否内评法银行
                ,ZCZQHLX                							 --资产证券化类型
                ,JCZCYWLX               							 --基础资产业务类型
    )
    WITH TEMP_BND_BOOK AS (SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM RWA_DEV.FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND (NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
												   OR BOND_ID IN (SELECT ITEMNO FROM RWA.CODE_LIBRARY WHERE CODENO = 'FNS_ABS_BOND' AND ISINUSE = '1'))
		)
		, TMP_SUPPORG AS (
								SELECT T1.ORGID AS ORGID
										   ,CASE WHEN T3.ORGLEVEL > 2 THEN T4.SORTNO ELSE T3.SORTNO END AS SORTNO
								  FROM RWA.RWA_WP_SUPPTASKORG T1
						INNER JOIN RWA.RWA_WP_SUPPTASK T2
								    ON T1.SUPPTASKID = T2.SUPPTASKID
								   AND T2.ENABLEFLAG = '01'
						 LEFT JOIN RWA.ORG_INFO T3
								    ON T1.ORGID = T3.ORGID
						 LEFT JOIN RWA.ORG_INFO T4
	                	ON T3.BELONGORGID = T4.ORGID
								 WHERE T1.SUPPTMPLID = 'M-0140'
							ORDER BY T3.SORTNO
		)
		SELECT
								DATADATE                               --数据日期
                ,ORGID                             	 	 --机构ID
                ,ZQNM																	 --债券内码
                ,YHJS																	 --银行角色
                ,YWSSJG                 							 --业务所属机构
                ,ZCCDH                  							 --资产池代号
                ,ZCZQHMC                							 --资产证券化名称
                ,TX                     							 --条线
                ,FDSXH                  							 --分档顺序号
                ,DCMC                   							 --档次名称
                ,SFZYXDC                							 --是否最优先档次
                ,ZZCZQHBZ               							 --再资产证券化标识
                ,ZCYE                   							 --资产余额
                ,BZ                     							 --币种
                ,JZZB                   							 --减值准备
                ,FXR                    							 --发行日
                ,DQR                    							 --到期日
                ,WBZXJGDPJJG            							 --外部增信机构的评级机构
                ,FQSHSTGZWBPJ           							 --发起时缓释提供者外部评级
                ,DQHSTGZWBPJ            							 --当前缓释提供者外部评级
                ,XYFXHSSFTGGTBMDDJG     							 --信用风险缓释是否提供给特别目的机构
                ,SFTGXYZCBFYDWBPJ       							 --是否提供信用支持并反映到外部评级
                ,ZQWBPJJG               							 --债券外部评级机构
                ,ZQWBPJQX               							 --债券外部评级期限
                ,ZQWBPJDJ               							 --债券外部评级等级
                ,ZQWBPJRQ															 --债券外部评级日期
                ,ZQHFQRZZJGDM           							 --证券化发起人组织机构代码
                ,FQJGSFNPFYH            							 --发起机构是否内评法银行
                ,ZCZQHLX                							 --资产证券化类型
                ,JCZCYWLX               							 --基础资产业务类型
    FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1)
                						                     				 		 AS ORGID                		 --机构ID                     		默认：资产负债管理部(01050000)
								,T1.BOND_ID																	 AS ZQNM										 --债券内码
                ,T3.YHJS		                             		 AS YHJS										 --银行角色
                ,T1.DEPARTMENT															 AS YWSSJG                 	 --业务所属机构
                ,T3.ZCCDH																		 AS ZCCDH                  	 --资产池代号
                ,T1.BOND_NAME                          			 AS ZCZQHMC                	 --资产证券化名称
                ,T3.TX	                         				 		 AS TX                     	 --条线
                ,T3.FDSXH                            	 			 AS FDSXH                  	 --分档顺序号
                ,T3.DCMC				                             AS DCMC                   	 --档次名称
                ,T3.SFZYXDC																	 AS SFZYXDC                	 --是否最优先档次
                ,T3.ZZCZQHBZ                                 AS ZZCZQHBZ               	 --再资产证券化标识
                ,CASE WHEN T1.ASSET_CLASS = '40' AND T1.BOND_TYPE1 IN ('091','099') THEN
                			DECODE(T1.CLOSED,'1',nvl(T2.INITIAL_COST,0),0) +
                			DECODE(T1.CLOSED,'1',nvl(T2.INT_ADJUST,0),0) +
                			DECODE(T1.CLOSED,'1',nvl(T2.MKT_VALUE_CHANGE,0),0) +
                			DECODE(T1.CLOSED,'1',nvl(T2.ACCOUNTABLE_INT,0),0)
                 ELSE nvl(T2.INITIAL_COST,0) +
                 			nvl(T2.INT_ADJUST,0) +
                 			nvl(T2.MKT_VALUE_CHANGE,0) +
                 			nvl(T2.ACCOUNTABLE_INT,0)
                 END                                         AS ZCYE                   	 --资产余额
                ,NVL(T1.CURRENCY_CODE,'CNY')                 AS BZ                     	 --币种
                ,T3.JZZB                                     AS JZZB                   	 --减值准备
                ,TO_CHAR(TO_DATE(T1.EFFECT_DATE,'YYYYMMDD'),'YYYY-MM-DD')
                														                 AS FXR                    	 --发行日
                ,TO_CHAR(TO_DATE(T1.MATURITY_DATE,'YYYYMMDD'),'YYYY-MM-DD')
                								                             AS DQR                    	 --到期日
                ,T3.WBZXJGDPJJG                              AS WBZXJGDPJJG            	 --外部增信机构的评级机构
                ,T3.FQSHSTGZWBPJ                             AS FQSHSTGZWBPJ           	 --发起时缓释提供者外部评级
                ,T3.DQHSTGZWBPJ                              AS DQHSTGZWBPJ            	 --当前缓释提供者外部评级
                ,T3.XYFXHSSFTGGTBMDDJG                       AS XYFXHSSFTGGTBMDDJG     	 --信用风险缓释是否提供给特别目的机构
                ,T3.SFTGXYZCBFYDWBPJ                         AS SFTGXYZCBFYDWBPJ       	 --是否提供信用支持并反映到外部评级
                ,T3.ZQWBPJJG                                 AS ZQWBPJJG               	 --债券外部评级机构
                ,T3.ZQWBPJQX                                 AS ZQWBPJQX               	 --债券外部评级期限
                ,T3.ZQWBPJDJ                                 AS ZQWBPJDJ               	 --债券外部评级等级
                ,T3.ZQWBPJRQ																 AS ZQWBPJRQ								 --债券外部评级日期
                ,T3.ZQHFQRZZJGDM                             AS ZQHFQRZZJGDM           	 --证券化发起人组织机构代码
                ,T3.FQJGSFNPFYH                              AS FQJGSFNPFYH            	 --发起机构是否内评法银行
                ,T3.ZCZQHLX                                  AS ZCZQHLX                	 --资产证券化类型
                ,T3.JCZCYWLX                                 AS JCZCYWLX               	 --基础资产业务类型


    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T2
		ON 					T1.BOND_ID = T2.BOND_ID
		LEFT	JOIN	(SELECT ZQNM
											 ,YHJS
											 ,ZCCDH
											 ,TX
											 ,FDSXH
											 ,DCMC
											 ,SFZYXDC
											 ,ZZCZQHBZ
											 ,JZZB
											 ,WBZXJGDPJJG
											 ,FQSHSTGZWBPJ
											 ,DQHSTGZWBPJ
											 ,XYFXHSSFTGGTBMDDJG
											 ,SFTGXYZCBFYDWBPJ
											 ,ZQWBPJJG
											 ,ZQWBPJQX
											 ,ZQWBPJDJ
											 ,ZQWBPJRQ
											 ,ZQHFQRZZJGDM
											 ,FQJGSFNPFYH
											 ,ZCZQHLX
											 ,JCZCYWLX
									 FROM RWA.RWA_WS_ABS_INVEST_EXPOSURE
									WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_ABS_INVEST_EXPOSURE WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
								) T3																				 				--去最近一期补录数据铺底
		ON					T1.BOND_ID = T3.ZQNM
		WHERE 			T1.ASSET_CLASS IN ('10','20','40')									--通过资产分类来确定债券还是应收款投资。
																																		--10 交易性资产
																																		--20 持有至到期类资产
																																		--40 可供出售类资产
		AND					(T1.BOND_TYPE1 = '060'															--资产支持证券
								OR T1.BOND_ID IN (SELECT ITEMNO FROM RWA.CODE_LIBRARY WHERE CODENO = 'FNS_ABS_BOND' AND ISINUSE = '1')
								)																										--或者从配置表获取资产证券化的债券内码
		AND 				T1.DATANO = p_data_dt_str														--债券信息表,获取有效的债券信息
		AND NOT EXISTS (SELECT 1 FROM RWA.RWA_WS_ABS_ISSUE_EXPOSURE WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_ABS_ISSUE_EXPOSURE WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD')) AND T1.BOND_ID = ZQNM AND YHJS = '01')
		)
		WHERE				YHJS = '02' 																				--仅投资机构需要铺底
		OR					YHJS IS NULL
		ORDER BY		ZQNM,ZCCDH
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_ABS_INVEST_EXPOSURE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_ABS_INVEST_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_ABS_INVEST_EXPOSURE表当前插入的财务系统-资产证券化-投资机构铺底数据记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '财务系统-资产证券化-投资机构补录数据铺底('|| v_pro_name ||')处理失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ABS_INVEST_WSIB;
/

