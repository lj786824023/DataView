CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_WS_GATHER(
														p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_WS_GATHER
    实现功能:RWA系统-工具-补录相关表整理
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-21
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA.RWA_WP_SUPPTMPL|补录配置表
    目标表  :无
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_WS_GATHER';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  --定义varchar2(100)数组
  type varchartab is table of varchar2(100) index by binary_integer;
  v_tab varchartab;
  --定义数组最大长度
  v_tab_len INTEGER := 1;
  --定义游标,源系统表清单
  cursor cc_stab is
  	select upper(suppTable) as tableName from RWA.RWA_WP_SUPPTMPL where enableFlag = '01' and suppTable is not null;

  v_cc_stab cc_stab%rowtype;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --1.获取待整理的表清单
    for v_cc_stab in cc_stab loop
    	v_tab(v_tab_len) := v_cc_stab.tableName;
    	v_tab_len := v_tab_len + 1;
    end loop;

    --2.执行整理命令
    for i in 1..v_tab.count loop
    	dbms_stats.gather_table_stats(ownname => 'RWA',tabname => v_tab(i),cascade => true);
    end loop;

		--分析补录分发表
		dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WP_DATASUPPLEMENT',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    v_count := v_tab.count;
    --Dbms_output.Put_line('整理源系统表信息数量为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '补录相关表信息整理('|| v_pro_name ||')处理失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_WS_GATHER;
/

