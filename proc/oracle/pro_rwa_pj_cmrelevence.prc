CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_PJ_CMRELEVENCE(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_PJ_CMRELEVENCE
    实现功能:核心系统-票据贴现-合同缓释物关联(从数据源核心系统将票据贴现相关信息全量导入RWA票据贴现接口表合同缓释物关联表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-21
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.BRD_BILL|票据信息

    目标表  :RWA_DEV.RWA_PJ_CMRELEVENCE|核心系统票据贴现类合同缓释物关联表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_PJ_CMRELEVENCE';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_PJ_CMRELEVENCE';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 票据转贴现
    INSERT INTO RWA_DEV.RWA_PJ_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                       	 	 --合同ID
                ,MITIGATIONID                     	 	 --缓释物ID
                ,MITIGCATEGORY                    	 	 --缓释物类型
                ,SGUARCONTRACTID                  	 	 --源担保合同ID
                ,GROUPID                          	 	 --分组编号
    )    
    SELECT
          TO_DATE(p_data_dt_str,'YYYYMMDD') , --数据日期
          p_data_dt_str , --数据流水号
          T1.ACCT_NO  , --合同ID
          'PJ' || T1.CRDT_BIZ_ID  , --缓释物ID
          '03'  , --缓释物类型
          'PJ' || T1.CRDT_BIZ_ID  , --源担保合同ID
          ''   --分组编号
    FROM  BRD_BILL T1
    WHERE T1.ATL_PAY_AMT <> 0 --取的都是本金
            AND SUBSTR(T1.SBJT_CD, 1, 6) IN (
                '130101', --贴现资产-银行承兑汇票贴现
                '130103', --贴现资产-银行承兑汇票转贴现
                '130102' --商业汇票贴现         转帖现已取到承兑行，不需要票据作为缓释
            )
            AND T1.DATANO=p_data_dt_str;
    COMMIT;    

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_PJ_CMRELEVENCE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_PJ_CMRELEVENCE;

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '合同缓释物关联('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_PJ_CMRELEVENCE;
/

