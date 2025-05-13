CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_CMRELEVENCE(
			 											P_DATA_DT_STR	IN	VARCHAR2,		--��������
       											P_PO_RTNCODE	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														P_PO_RTNMSG		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_CMRELEVENCE
    ʵ�ֹ���:���ܺ�ͬ�������,�������к�ͬ���������Ϣ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-06-01
    ��  λ	:�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1	:RWA_DEV.RWA_XD_CMRELEVENCE|�Ŵ���ͬ�����������
    Դ  ��2	:RWA_DEV.RWA_PJ_CMRELEVENCE|Ʊ�ݺ�ͬ�����������
    Դ  ��3	:RWA_DEV.RWA_LC_CMRELEVENCE|��ƺ�ͬ�����������
    Դ  ��4	:RWA_DEV.RWA_TZ_CMRELEVENCE|Ͷ�ʺ�ͬ�����������
    Դ  ��5	:RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE|�ʲ�֤ȯ����ͬ�����������
    Ŀ���	:RWA_DEV.RWA_EI_CMRELEVENCE|��ͬ�����������Ϣ���ܱ�
    ������	:��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
  */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_CMRELEVENCE';
  --�����ж�ֵ����
  v_count INTEGER;
  --�����쳣����
  v_raise EXCEPTION;
    --������ʱ����
  v_tabname VARCHAR2(200);
  --���崴�����
  v_create VARCHAR2(1000);

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

   BEGIN
    --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_CMRELEVENCE DROP PARTITION CMRELEVENCE' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --�״η���truncate�����2149�쳣
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '���ܺ�ͬ��������Ϣ��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --����һ����ǰ�����µķ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_CMRELEVENCE ADD PARTITION CMRELEVENCE' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*�����Ŵ��ĺ�ͬ�����������Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_CMRELEVENCE(
         				DATADATE       									  --��������
         				,DATANO                           --������ˮ��
         				,CONTRACTID                       --��ͬ����
         				,MITIGATIONID                     --���������
         				,MITIGCATEGORY                    --����������
         				,SGUARCONTRACTID                  --Դ������ͬ����
         				,GROUPID                          --������
    )
    SELECT
         				DATADATE       										          AS	datadate       									--��������
         				,DATANO         							              AS	datano              						--������ˮ��
         				,CONTRACTID     								            AS	contractid          						--��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
         				,MITIGATIONID        												AS 	mitigationid        						--���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
         				,MITIGCATEGORY  							              AS	mitigcategory       						--����������
         				,SGUARCONTRACTID									          AS	sguarcontractid     						--Դ������ͬ����(�������)
         				,GROUPID        													  AS	groupid             						--������
    FROM  			RWA_DEV.RWA_XD_CMRELEVENCE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*����Ʊ�ݵĺ�ͬ�����������Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_CMRELEVENCE(
         				DATADATE       									  --��������
         				,DATANO                           --������ˮ��
         				,CONTRACTID                       --��ͬ����
         				,MITIGATIONID                     --���������
         				,MITIGCATEGORY                    --����������
         				,SGUARCONTRACTID                  --Դ������ͬ����
         				,GROUPID                          --������
    )
    SELECT
         				DATADATE       										          AS	datadate       									--��������
         				,DATANO         							              AS	datano              						--������ˮ��
         				,CONTRACTID     								            AS	contractid          						--��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
         				,MITIGATIONID        												AS 	mitigationid        						--���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
         				,MITIGCATEGORY  							              AS	mitigcategory       						--����������
         				,SGUARCONTRACTID									          AS	sguarcontractid     						--Դ������ͬ����(�������)
         				,GROUPID        													  AS	groupid             						--������
    FROM  			RWA_DEV.RWA_PJ_CMRELEVENCE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*������Ƶĺ�ͬ�����������Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_CMRELEVENCE(
         				DATADATE       									  --��������
         				,DATANO                           --������ˮ��
         				,CONTRACTID                       --��ͬ����
         				,MITIGATIONID                     --���������
         				,MITIGCATEGORY                    --����������
         				,SGUARCONTRACTID                  --Դ������ͬ����
         				,GROUPID                          --������
    )
    SELECT
         				DATADATE       										          AS	datadate       									--��������
         				,DATANO         							              AS	datano              						--������ˮ��
         				,CONTRACTID     								            AS	contractid          						--��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
         				,MITIGATIONID        												AS 	mitigationid        						--���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
         				,MITIGCATEGORY  							              AS	mitigcategory       						--����������
         				,SGUARCONTRACTID									          AS	sguarcontractid     						--Դ������ͬ����(�������)
         				,GROUPID        													  AS	groupid             						--������
    FROM  			RWA_DEV.RWA_LC_CMRELEVENCE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*����Ͷ�ʵĺ�ͬ�����������Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_CMRELEVENCE(
         				DATADATE       									  --��������
         				,DATANO                           --������ˮ��
         				,CONTRACTID                       --��ͬ����
         				,MITIGATIONID                     --���������
         				,MITIGCATEGORY                    --����������
         				,SGUARCONTRACTID                  --Դ������ͬ����
         				,GROUPID                          --������
    )
    SELECT
         				DATADATE       										          AS	datadate       									--��������
         				,DATANO         							              AS	datano              						--������ˮ��
         				,CONTRACTID     								            AS	contractid          						--��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
         				,MITIGATIONID        												AS 	mitigationid        						--���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
         				,MITIGCATEGORY  							              AS	mitigcategory       						--����������
         				,SGUARCONTRACTID									          AS	sguarcontractid     						--Դ������ͬ����(�������)
         				,GROUPID        													  AS	groupid             						--������
    FROM  			RWA_DEV.RWA_TZ_CMRELEVENCE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*�����ʲ�֤ȯ���ĺ�ͬ�����������Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_CMRELEVENCE(
         				DATADATE       									  --��������
         				,DATANO                           --������ˮ��
         				,CONTRACTID                       --��ͬ����
         				,MITIGATIONID                     --���������
         				,MITIGCATEGORY                    --����������
         				,SGUARCONTRACTID                  --Դ������ͬ����
         				,GROUPID                          --������
    )
    SELECT
         				DATADATE       										          AS	datadate       									--��������
         				,DATANO         							              AS	datano              						--������ˮ��
         				,CONTRACTID     								            AS	contractid          						--��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
         				,MITIGATIONID        												AS 	mitigationid        						--���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
         				,MITIGCATEGORY  							              AS	mitigcategory       						--����������
         				,SGUARCONTRACTID									          AS	sguarcontractid     						--Դ������ͬ����(�������)
         				,GROUPID        													  AS	groupid             						--������
    FROM  			RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;

     /*����ع��ĺ�ͬ�����������Ϣ*/
    INSERT INTO RWA_DEV.RWA_EI_CMRELEVENCE(
         				DATADATE       									  --��������
         				,DATANO                           --������ˮ��
         				,CONTRACTID                       --��ͬ����
         				,MITIGATIONID                     --���������
         				,MITIGCATEGORY                    --����������
         				,SGUARCONTRACTID                  --Դ������ͬ����
         				,GROUPID                          --������
    )
    SELECT
         				DATADATE       										          AS	datadate       									--��������
         				,DATANO         							              AS	datano              						--������ˮ��
         				,CONTRACTID     								            AS	contractid          						--��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
         				,MITIGATIONID        												AS 	mitigationid        						--���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
         				,MITIGCATEGORY  							              AS	mitigcategory       						--����������
         				,SGUARCONTRACTID									          AS	sguarcontractid     						--Դ������ͬ����(�������)
         				,GROUPID        													  AS	groupid             						--������
    FROM  			RWA_DEV.RWA_HG_CMRELEVENCE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;

    --�������Ϣ
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_CMRELEVENCE',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_CMRELEVENCE',partname => 'CMRELEVENCE'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*Ŀ�������ͳ��*/
    SELECT COUNT(1) INTO v_count FROM RWA_EI_CMRELEVENCE WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_EI_CMRELEVENCE��ǰ��������ݼ�¼Ϊ:' || v_count1 || '��');

		--Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

		P_PO_RTNCODE := '1';
	  P_PO_RTNMSG  := '�ɹ�'||'-'||v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 P_PO_RTNCODE := sqlcode;
   			 P_PO_RTNMSG  := '���ܺ�ͬ�뻺�������(pro_rwa_ei_cmrelevence)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_CMRELEVENCE;
/

