CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_WS_GATHER(
														p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_WS_GATHER
    ʵ�ֹ���:RWAϵͳ-����-��¼��ر�����
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-21
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA.RWA_WP_SUPPTMPL|��¼���ñ�
    Ŀ���  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_WS_GATHER';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  --����varchar2(100)����
  type varchartab is table of varchar2(100) index by binary_integer;
  v_tab varchartab;
  --����������󳤶�
  v_tab_len INTEGER := 1;
  --�����α�,Դϵͳ���嵥
  cursor cc_stab is
  	select upper(suppTable) as tableName from RWA.RWA_WP_SUPPTMPL where enableFlag = '01' and suppTable is not null;

  v_cc_stab cc_stab%rowtype;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --1.��ȡ������ı��嵥
    for v_cc_stab in cc_stab loop
    	v_tab(v_tab_len) := v_cc_stab.tableName;
    	v_tab_len := v_tab_len + 1;
    end loop;

    --2.ִ����������
    for i in 1..v_tab.count loop
    	dbms_stats.gather_table_stats(ownname => 'RWA',tabname => v_tab(i),cascade => true);
    end loop;

		--������¼�ַ���
		dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WP_DATASUPPLEMENT',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    v_count := v_tab.count;
    --Dbms_output.Put_line('����Դϵͳ����Ϣ����Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '��¼��ر���Ϣ����('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_WS_GATHER;
/

