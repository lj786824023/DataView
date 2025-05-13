CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ETL_SCHEDULE(
														p_stage				IN	VARCHAR2,		--���Ƚ׶� ����RWA_DEV.RWA_PRO_SCHEDULE�����������
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ETL_SCHEDULE
    ʵ�ֹ���:RWAϵͳ-ETL����
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-21
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_PRO_SCHEDULE|RWA�洢���̵������ñ�
    Ŀ���  :RWA_DEV.RWA_PRO_RECORD|RWA�洢���̵��ȼ�¼��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  Authid Current_User
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ETL_SCHEDULE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  --���巵�ر��
  v_po_rtncode VARCHAR2(200);
  --���巵������
  v_po_rtnmsg VARCHAR2(4000);
  --�����ִ���ַ���
  v_call_str VARCHAR2(200);
  --����varchar2(400)����
  type varchartab is table of varchar2(400) index by binary_integer;
  v_owner_tab varchartab;
  v_proName_tab varchartab;
  --����integer����
  type integertab is table of integer index by binary_integer;
  v_sleepTime_tab integertab;
  --����������󳤶�
  v_tab_len INTEGER := 1;
  --����������
  v_owner VARCHAR2(60);
  --����洢��������
  v_proName VARCHAR2(60);
  --����˯��ʱ��
  v_sleepTime INTEGER;
  --����洢�����Ƿ����
  v_pro_cnt INTEGER;
  --���忪ʼ����
  v_startDate VARCHAR2(10);
  --���忪ʼʱ��
  v_startTime VARCHAR2(10);
  --�����������
  v_endDate VARCHAR2(10);
  --�������ʱ��
  v_endTime VARCHAR2(10);
  --�����������
  v_scheduleNo VARCHAR2(60);
  --������ȼ�¼��ˮ��
  v_recordSerialNo VARCHAR2(60);
  --������ȼ�¼insert���
  v_recordInsert VARCHAR2(200);
  --������ȼ�¼update���
  v_recordUpdate VARCHAR2(200);
  --�����α�
  cursor cc(v_stage VARCHAR2) is
  	select trim(owner) as owner,trim(proName) as proName,sleepTime from RWA_DEV.RWA_PRO_SCHEDULE where stage = v_stage and isInUse = '1' and proName is not null order by priority;

  v_cc cc%rowtype;

  BEGIN
    Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --RWA_DEV.PRO_RWA_EI_GUARANTEE_STD(p_data_dt_str,v_po_rtncode,v_po_rtnmsg);

    --Dbms_output.Put_line('��ִ�� RWA_DEV.PRO_RWA_EI_GUARANTEE_STD �洢���̽�����Code = '|| v_po_rtncode ||',Msg = '|| v_po_rtnmsg ||' ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --��ȡUUIDΨһ��ʶ
    select sys_guid() into v_scheduleNo from dual;

    --��ȡ��ִ�еĴ洢����,��owner.proName,sleepTime;����ʽƴ�����ַ�����
    for v_cc in cc(p_stage) loop
    	v_owner_tab(v_tab_len) := v_cc.owner;
    	v_proName_tab(v_tab_len) := v_cc.proName;
    	v_sleepTime_tab(v_tab_len) := case when v_cc.sleepTime is null or v_cc.sleepTime < 0 then 0 else v_cc.sleepTime end;
    	v_tab_len := v_tab_len + 1;
    end loop;

    --��������ǰ�Ĺ��ڼ�¼
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
	    	--Dbms_output.Put_line('��' || i || '���洢����ִ��:' || v_call_str);
	    	v_recordSerialNo := TO_CHAR(SYSDATE,'yyyymmdd') || lpad(SEQ_ETL_SCHEDULE_ID.NEXTVAL, 10, '0');
	  		v_startDate := TO_CHAR(SYSDATE,'yyyy/mm/dd');
	  		v_startTime := TO_CHAR(SYSDATE,'hh24:mi:ss');
	  		--������ȼ�¼��ʼ״̬
	  		EXECUTE IMMEDIATE v_recordInsert USING v_recordSerialNo,v_owner,v_proName,v_scheduleNo,v_startDate,v_startTime,p_stage,p_data_dt_str;

	  		COMMIT;

				BEGIN
	  			EXECUTE IMMEDIATE v_call_str USING IN p_data_dt_str, OUT v_po_rtncode, OUT v_po_rtnmsg;
	  		EXCEPTION
	  		WHEN OTHERS THEN
	  			v_po_rtncode := -1;
	  			v_po_rtnmsg := '�洢����ִ�г����쳣���ô洢����δִ��';
	  		END;
	  		v_endDate := TO_CHAR(SYSDATE,'yyyy/mm/dd');
	  		v_endTime := TO_CHAR(SYSDATE,'hh24:mi:ss');
	  		--��ȡִ�д洢����Ӱ���������
	  		if v_po_rtncode = '1' and regexp_like(substr(v_po_rtnmsg,instr(v_po_rtnmsg,'-') + 1),'^[0-9]+$') then
	  			v_count := to_number(substr(v_po_rtnmsg,instr(v_po_rtnmsg,'-') + 1));
	  			v_po_rtnmsg := substr(v_po_rtnmsg,1,instr(v_po_rtnmsg,'-') - 1);
	  		else
	  			v_count := 0;
	  		end if;
	  		--���µ��ȼ�¼����״̬
	  		EXECUTE IMMEDIATE v_recordUpdate USING v_endDate,v_endTime,v_count,v_po_rtncode,v_po_rtnmsg,v_recordSerialNo;

	  		COMMIT;
	  		v_po_rtncode := '';
        v_po_rtnmsg := '';
	  		--˯�ߣ���λ�룬��dba��Ȩ��grant execute on dbms_lock to userName
	  		--dbms_lock.sleep(v_sleepTime);
  		else
  			Dbms_output.Put_line('��' || i || '���洢����' || v_proName || '�����ڣ�����ִ����һ��');
  		end if;
    end loop;
    --v_str := 'CALL RWA_DEV.PRO_RWA_EI_GUARANTEE_STD(:param1,:param2,:param3)';

    --EXECUTE IMMEDIATE v_str USING IN p_data_dt_str, OUT v_po_rtncode, OUT v_po_rtnmsg;

    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽�����Code = '|| v_po_rtncode ||',Msg = '|| v_po_rtnmsg ||' ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_PRO_RECORD WHERE SCHEDULENO = v_scheduleNo;
    Dbms_output.Put_line('����RWA_DEV.RWA_PRO_RECORD����¼���ȱ��Ϊ' || v_scheduleNo || '�ļ�¼��Ϊ: ' || v_count || ' ��');

    Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := 'ETL����('|| v_pro_name ||')ִ��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ETL_SCHEDULE;
/

