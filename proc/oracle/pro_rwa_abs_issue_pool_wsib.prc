CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSUE_POOL_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ABS_ISSUE_POOL_WSIB
    实现功能:台账-资产证券化-发行机构-合约与池-补录铺底(从RWA资产证券化-发行机构-合约与池补录表中导入上期数据至RWA资产证券化-发行机构-合约与池补录铺底表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-12-08
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA.RWA_WS_ABS_ISSUE_POOL|资产证券化-发行机构-合约与池补录表
    目标表1 :RWA.RWA_WSIB_ABS_ISSUE_POOL|资产证券化-发行机构-合约与池铺底表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_ISSUE_POOL_WSIB';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    --清空资产证券化-发行机构-合约与池铺底表
    DELETE FROM RWA.RWA_WSIB_ABS_ISSUE_POOL WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.将满足条件的数据从源表插入到目标表中
    INSERT INTO RWA.RWA_WSIB_ABS_ISSUE_POOL(
                DATADATE                               --数据日期
                ,ORGID                             	 	 --机构ID
                ,YWSSJG                        	 		 	 --业务所属机构
                ,ZCCBH                    	 		 	 		 --资产池编号
                ,ZCCDH       										 		 	 --资产池代号
                ,ZCZQHMC                          		 --资产证券化名称
                ,ZQHFQRZZJGDM                  	 		 	 --证券化发起人组织机构代码
                ,ZCZQHLX                       	 		 	 --资产证券化类型
                ,JCZCYWLX                      	 		 	 --基础资产业务类型
                ,SFFHGLTJ                      	 		 	 --是否符合管理条件
                ,SFHGZCZQH   												 	 --是否合规资产证券化
                ,SFTGYXZC                              --是否提供隐性支持
                ,XSLD                                  --销售利得
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,T1.SUPPORGID						                     AS ORGID										 --机构ID
                ,T1.YWSSJG                        	 		 		 AS	YWSSJG                   --业务所属机构
                ,T1.ZCCBH       										 		 		 AS	ZCCBH       						 --资产池编号
                ,T1.ZCCDH       										 		 		 AS	ZCCDH       						 --资产池代号
                ,T1.ZCZQHMC                          	 		 	 AS	ZCZQHMC                  --资产证券化名称
                ,T1.ZQHFQRZZJGDM                  	 		 		 AS	ZQHFQRZZJGDM             --证券化发起人组织机构代码
                ,T1.ZCZQHLX                       	 		 		 AS	ZCZQHLX                  --资产证券化类型
                ,T1.JCZCYWLX                      	 		 		 AS	JCZCYWLX                 --基础资产业务类型
                ,T1.SFFHGLTJ                      	 		 		 AS	SFFHGLTJ                 --是否符合管理条件
                ,T1.SFHGZCZQH   												 		 AS	SFHGZCZQH   						 --是否合规资产证券化
                ,T1.SFTGYXZC                            		 AS	SFTGYXZC                 --是否提供隐性支持
                ,T1.XSLD                                		 AS	XSLD                     --销售利得

    FROM				RWA.RWA_WS_ABS_ISSUE_POOL T1            		 --资产证券化-发行机构-合约与池补录表，取最近一期补录数据铺底
		WHERE 			T1.DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_ABS_ISSUE_POOL WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
		ORDER BY		T1.SUPPSERIALNO
		;

    COMMIT;

		dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_ABS_ISSUE_POOL',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_ABS_ISSUE_POOL WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_ABS_ISSUE_POOL表当前插入的台账-资产证券化-发行机构-合约与池铺底数据记录为: ' || v_count || ' 条');


    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '资产证券化-发行机构-合约与池补录数据铺底('|| v_pro_name ||')处理失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ABS_ISSUE_POOL_WSIB;
/

