CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_FEFORWARDSSWAP(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_FEFORWARDSSWAP
    ʵ�ֹ���:�������Զ�ڵ���
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-08-02
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_WH_FEFORWARDSSWAP|�������Զ�ڵ��ڱ����ڣ�
    Ŀ���  :RWA_DEV.RWA_EI_FEFORWARDSSWAP|���Զ�ڵ��ڱ����ڣ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_FEFORWARDSSWAP';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_FEFORWARDSSWAP DROP PARTITION FEFORWARDSSWAP' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
      	IF (SQLCODE <> '-2149') THEN
          --�״η���truncate�����2149�쳣
        	p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '���Զ�ڵ��ڱ����ڣ���('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         	RETURN;
        END IF;
    END;

    --����һ����ǰ�����µķ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_FEFORWARDSSWAP ADD PARTITION FEFORWARDSSWAP' || p_data_dt_str || ' VALUES(TO_DATE(' || p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ϵͳ-���Զ�ڵ��ڱ����ڣ���
    INSERT INTO RWA_DEV.RWA_EI_FEFORWARDSSWAP(
                DATADATE                               --��������
                ,TRANID                                --����ID
                ,TRANORGID                             --���׻���ID
                ,ACCORGID                              --�������ID
                ,INSTRUMENTSTYPE                       --���ڹ�������
                ,ACCSUBJECTS                           --��ƿ�Ŀ
                ,BOOKTYPE                              --�˻����
                ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ���
                ,BUYCURRENCY                           --�������
                ,BUYAMOUNT                             --������
                ,SELLCURRENCY      										 --��������
                ,SELLAMOUNT                            --�������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,BUYZERORATE                           --���������Ϣ����
                ,BUYDISCOUNTRATE                       --������������
                ,SELLZERORATE                          --����������Ϣ����
                ,SELLDISCOUNTRATE                      --������������

    )
    SELECT
                DATADATE            													AS DATADATE                 --��������
                ,TRANID            											     	AS TRANID                   --����ID
                ,TRANORGID                       				 		 	AS TRANORGID                --���׻���ID
                ,ACCORGID                                 		AS ACCORGID                 --�������ID
                ,INSTRUMENTSTYPE                              AS INSTRUMENTSTYPE          --���ڹ�������
                ,ACCSUBJECTS       													 	AS ACCSUBJECTS              --��ƿ�Ŀ
                ,BOOKTYPE          													 	AS BOOKTYPE                 --�˻����
                ,STRUCTURALEXPOFLAG													 	AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���
                ,BUYCURRENCY       													 	AS BUYCURRENCY              --�������
                ,BUYAMOUNT         													 	AS BUYAMOUNT                --������
                ,SELLCURRENCY                                 AS SELLCURRENCY      			 	--��������
                ,SELLAMOUNT                                   AS SELLAMOUNT               --�������
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                					                                    AS STARTDATE                --��ʼ����
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                				                                      AS DUEDATE                  --��������
                ,ORIGINALMATURITY                             AS ORIGINALMATURITY         --ԭʼ����
                ,RESIDUALM                                    AS RESIDUALM                --ʣ������
                ,BUYZERORATE                                  AS BUYZERORATE              --���������Ϣ����
                ,BUYDISCOUNTRATE                              AS BUYDISCOUNTRATE          --������������
                ,SELLZERORATE                                 AS SELLZERORATE             --����������Ϣ����
                ,SELLDISCOUNTRATE                             AS SELLDISCOUNTRATE         --������������

    FROM				RWA_DEV.RWA_WH_FEFORWARDSSWAP
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  ;

    COMMIT;

    --�������Ϣ
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_FEFORWARDSSWAP',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_FEFORWARDSSWAP',partname => 'FEFORWARDSSWAP'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_FEFORWARDSSWAP WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_FEFORWARDSSWAP��ǰ����Ĺ���ϵͳ-��㽻��(�г�����)-���Զ�ڵ��ڼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '���Զ�ڵ���('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_FEFORWARDSSWAP;
/

