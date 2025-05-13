CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_FESPOTPOSITION(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_FESPOTPOSITION
    ʵ�ֹ���:��������ֻ�ͷ���������������ֻ�ͷ����Ϣ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-07-07
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_WH_FESPOTPOSITION|����ϵͳ����ֻ�ͷ���
    Ŀ���  :RWA_DEV.RWA_EI_FESPOTPOSITION|��������ֻ�ͷ���
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_FESPOTPOSITION';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_FESPOTPOSITION DROP PARTITION FESPOTPOSITION' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --�״η���truncate�����2149�쳣
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '��������ֻ�ͷ���('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --����һ����ǰ�����µķ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_FESPOTPOSITION ADD PARTITION FESPOTPOSITION' || p_data_dt_str || ' VALUES(TO_DATE(' || p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*��������ֻ�ͷ����Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_FESPOTPOSITION(
                DATADATE                               --��������
                ,POSITIONID                            --ͷ��ID
                ,ACCORGID                              --�������ID
                ,INSTRUMENTSTYPE                       --���ڹ�������
                ,ACCSUBJECTS                           --��ƿ�Ŀ
                ,BOOKTYPE                              --�˻����
                ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ���
                ,CURRENCY                              --����
                ,POSITION                              --ͷ��
                ,POSITIONTYPE                          --ͷ������

    )
    SELECT
                DATADATE                                     AS DATADATE                 --��������
                ,POSITIONID													         AS POSITIONID               --ͷ��ID
                ,ACCORGID                   				 		     AS ACCORGID                 --�������ID    										��¼
                ,INSTRUMENTSTYPE			                       AS INSTRUMENTSTYPE          --���ڹ�������											Ĭ��Ϊ ��0501�� ������ֻ���
                ,ACCSUBJECTS    	                           AS ACCSUBJECTS              --��ƿ�Ŀ      										��¼
                ,BOOKTYPE					                           AS BOOKTYPE                 --�˻����      										ͨ����ƿ�Ŀӳ�䣬����Ŀ��Ϊ1101-�����Խ����ʲ�Ϊ�����˻�(02)������Ĭ��Ϊ�����˻�(01)
                ,STRUCTURALEXPOFLAG			 						         AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���										ͨ����ƿ�Ŀӳ�䣬����Ŀ��Ϊ4001-�ɱ�����Ŀ10030102-����������п���-����������з���׼����Ϊ�ṹ�Գ���(1)������Ĭ��Ϊ��(0)
                ,CURRENCY			 						               		 AS CURRENCY                 --����          										������ǿ�����������֣���������������
                ,POSITION                            				 AS POSITION                 --ͷ��          										������ǿ�����������������������
                ,POSITIONTYPE		                             AS POSITIONTYPE             --ͷ������      										������ǿ���Ϊ��ͷ(01)������Ϊ��ͷ(02)

    FROM				RWA_DEV.RWA_WH_FESPOTPOSITION
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  ;

    COMMIT;

    --�������Ϣ
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_FESPOTPOSITION',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_FESPOTPOSITION',partname => 'FESPOTPOSITION'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_FESPOTPOSITION WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_FESPOTPOSITION��ǰ����Ĺ���ϵͳ-����ֻ�(�г�����)-����ֻ�ͷ���¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '����ֻ�ͷ��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_FESPOTPOSITION;
/

