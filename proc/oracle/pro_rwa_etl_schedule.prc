CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ETL_SCHEDULE(
														p_stage				IN	VARCHAR2,		--调度阶段 根据RWA_DEV.RWA_PRO_SCHEDULE表的配置输入
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ETL_SCHEDULE
    实现功能:RWA系统-ETL调度
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-21
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_PRO_SCHEDULE|RWA存储过程调度配置表
    目标表  :RWA_DEV.RWA_PRO_RECORD|RWA存储过程调度记录表
    变更记录(修改人|修改时间|修改内容):
    */
  Authid Current_User
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ETL_SCHEDULE';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  --定义返回编号
  v_po_rtncode VARCHAR2(200);
  --定义返回描述
  v_po_rtnmsg VARCHAR2(4000);
  --定义待执行字符串
  v_call_str VARCHAR2(200);
  --定义varchar2(400)数组
  type varchartab is table of varchar2(400) index by binary_integer;
  v_owner_tab varchartab;
  v_proName_tab varchartab;
  --定义integer数组
  type integertab is table of integer index by binary_integer;
  v_sleepTime_tab integertab;
  --定义数组最大长度
  v_tab_len INTEGER := 1;
  --定义所有者
  v_owner VARCHAR2(60);
  --定义存储过程名称
  v_proName VARCHAR2(60);
  --定义睡眠时间
  v_sleepTime INTEGER;
  --定义存储过程是否存在
  v_pro_cnt INTEGER;
  --定义开始日期
  v_startDate VARCHAR2(10);
  --定义开始时间
  v_startTime VARCHAR2(10);
  --定义结束日期
  v_endDate VARCHAR2(10);
  --定义结束时间
  v_endTime VARCHAR2(10);
  --定义调度批次
  v_scheduleNo VARCHAR2(60);
  --定义调度记录流水号
  v_recordSerialNo VARCHAR2(60);
  --定义调度记录insert语句
  v_recordInsert VARCHAR2(200);
  --定义调度记录update语句
  v_recordUpdate VARCHAR2(200);
  --定义游标
  cursor cc(v_stage VARCHAR2) is
  	select trim(owner) as owner,trim(proName) as proName,sleepTime from RWA_DEV.RWA_PRO_SCHEDULE where stage = v_stage and isInUse = '1' and proName is not null order by priority;

  v_cc cc%rowtype;

  BEGIN
    Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --RWA_DEV.PRO_RWA_EI_GUARANTEE_STD(p_data_dt_str,v_po_rtncode,v_po_rtnmsg);

    --Dbms_output.Put_line('【执行 RWA_DEV.PRO_RWA_EI_GUARANTEE_STD 存储过程结束。Code = '|| v_po_rtncode ||',Msg = '|| v_po_rtnmsg ||' 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --获取UUID唯一标识
    select sys_guid() into v_scheduleNo from dual;

    --获取待执行的存储过程,以owner.proName,sleepTime;的形式拼接在字符串中
    for v_cc in cc(p_stage) loop
    	v_owner_tab(v_tab_len) := v_cc.owner;
    	v_proName_tab(v_tab_len) := v_cc.proName;
    	v_sleepTime_tab(v_tab_len) := case when v_cc.sleepTime is null or v_cc.sleepTime < 0 then 0 else v_cc.sleepTime end;
    	v_tab_len := v_tab_len + 1;
    end loop;

    --清理三月前的过期记录
    --delete from RWA_DEV.RWA_PRO_RECORD where substr(SERIALNO,1,8) < to_char(add_months(sysdate,-3),'yyyymmdd');

    --COMMIT;

    v_recordInsert := 'INSERT INTO RWA_DEV.RWA_PRO_RECORD(SERIALNO,OWNER,PRONAME,SCHEDULENO,STARTDATE,STARTTIME,STAGE,ATTRIBUTE1) VALUES(:d1,:d2,:d3,:d4,:d5,:d6,:d7,:d8)';

    v_recordUpdate := 'UPDATE RWA_DEV.RWA_PRO_RECORD SET ENDDATE = :d1,ENDTIME = :d2,RESULTCOUNT = :d3,RESULTCODE = :d4,RESULTMSG = :d5 WHERE SERIALNO = :d6';

    for i in 1..v_tab_len-1 loop
    	v_owner := v_owner_tab(i);
    	v_proName := v_proName_tab(i);
    	v_sleepTime := v_sleepTime_tab(i);
    	select count(1) into v_pro_cnt from user_procedures where object_name = upper(v_proName) and object_type = 'PROCEDURE';
    	if v_pro_cnt > 0 then
	    	v_call_str := 'CALL ' || case when v_owner is null then '' else v_owner || '.' end || v_proName || '(:param1,:param2,:param3)';
	    	--Dbms_output.Put_line('第' || i || '个存储过程执行:' || v_call_str);
	    	v_recordSerialNo := TO_CHAR(SYSDATE,'yyyymmdd') || lpad(SEQ_ETL_SCHEDULE_ID.NEXTVAL, 10, '0');
	  		v_startDate := TO_CHAR(SYSDATE,'yyyy/mm/dd');
	  		v_startTime := TO_CHAR(SYSDATE,'hh24:mi:ss');
	  		--插入调度记录开始状态
	  		EXECUTE IMMEDIATE v_recordInsert USING v_recordSerialNo,v_owner,v_proName,v_scheduleNo,v_startDate,v_startTime,p_stage,p_data_dt_str;

	  		COMMIT;

				BEGIN
	  			EXECUTE IMMEDIATE v_call_str USING IN p_data_dt_str, OUT v_po_rtncode, OUT v_po_rtnmsg;
	  		EXCEPTION
	  		WHEN OTHERS THEN
	  			v_po_rtncode := -1;
	  			v_po_rtnmsg := '存储过程执行出现异常，该存储过程未执行';
	  		END;
	  		v_endDate := TO_CHAR(SYSDATE,'yyyy/mm/dd');
	  		v_endTime := TO_CHAR(SYSDATE,'hh24:mi:ss');
	  		--获取执行存储过程影响的数据量
	  		if v_po_rtncode = '1' and regexp_like(substr(v_po_rtnmsg,instr(v_po_rtnmsg,'-') + 1),'^[0-9]+$') then
	  			v_count := to_number(substr(v_po_rtnmsg,instr(v_po_rtnmsg,'-') + 1));
	  			v_po_rtnmsg := substr(v_po_rtnmsg,1,instr(v_po_rtnmsg,'-') - 1);
	  		else
	  			v_count := 0;
	  		end if;
	  		--更新调度记录结束状态
	  		EXECUTE IMMEDIATE v_recordUpdate USING v_endDate,v_endTime,v_count,v_po_rtncode,v_po_rtnmsg,v_recordSerialNo;

	  		COMMIT;
	  		v_po_rtncode := '';
        v_po_rtnmsg := '';
	  		--睡眠，单位秒，需dba赋权：grant execute on dbms_lock to userName
	  		--dbms_lock.sleep(v_sleepTime);
  		else
  			Dbms_output.Put_line('第' || i || '个存储过程' || v_proName || '不存在，跳过执行下一个');
  		end if;
    end loop;
    --v_str := 'CALL RWA_DEV.PRO_RWA_EI_GUARANTEE_STD(:param1,:param2,:param3)';

    --EXECUTE IMMEDIATE v_str USING IN p_data_dt_str, OUT v_po_rtncode, OUT v_po_rtnmsg;

    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束。Code = '|| v_po_rtncode ||',Msg = '|| v_po_rtnmsg ||' 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_PRO_RECORD WHERE SCHEDULENO = v_scheduleNo;
    Dbms_output.Put_line('本次RWA_DEV.RWA_PRO_RECORD表共记录调度编号为' || v_scheduleNo || '的记录数为: ' || v_count || ' 条');

    Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := 'ETL调度('|| v_pro_name ||')执行失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ETL_SCHEDULE;
/

