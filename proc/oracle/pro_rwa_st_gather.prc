CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ST_GATHER(
														p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ST_GATHER
    ʵ�ֹ���:RWAϵͳ-����-�ӿڲ�Դϵͳ������
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-21
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.DATAMART_MATCH|Դϵͳ��������ñ�
    Ŀ���  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ST_GATHER';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  --����varchar2(100)����
  type varchartab is table of varchar2(100) index by binary_integer;
  v_tab varchartab;
  --����������󳤶�
  v_tab_len INTEGER := 1;
  --�����α�,Դϵͳ���嵥��Ĭ���µ�ȫ�������ı�
  cursor cc_stab is
  	select upper(tableName) as tableName from RWA_DEV.DATAMART_MATCH where status = '1' and batchType = '01' and tableName is not null;

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
    	begin
    	--dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => v_tab(i),estimate_percent => null,method_opt => 'for all indexed columns',cascade => true);
    	--dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => v_tab(i),partname => 'SRC'||p_data_dt_str,granularity => 'PARTITION',cascade => true);
    	dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => v_tab(i),cascade => true);

    	/*
    	EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-20000') THEN
          --���������ڻ����-20000�쳣������
         	p_po_rtncode := sqlcode;
         	p_po_rtnmsg  := 'Դϵͳ����Ϣ����('|| v_tab(i) ||')���������(SRC'|| p_data_dt_str ||')ʧ�ܡ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         	RETURN;
        ELSE
        	--û�и÷����ͷ���ȫ��
        	dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => v_tab(i),cascade => true);
        END IF;
      */
    END;
    end loop;



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
   			 p_po_rtnmsg  := 'Դϵͳ����Ϣ����('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ST_GATHER;
/

