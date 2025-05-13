CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_PJ_CMRELEVENCE(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_PJ_CMRELEVENCE
    ʵ�ֹ���:����ϵͳ-Ʊ������-��ͬ���������(������Դ����ϵͳ��Ʊ�����������Ϣȫ������RWAƱ�����ֽӿڱ��ͬ�������������)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-21
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.BRD_BILL|Ʊ����Ϣ

    Ŀ���  :RWA_DEV.RWA_PJ_CMRELEVENCE|����ϵͳƱ���������ͬ�����������
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_PJ_CMRELEVENCE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_PJ_CMRELEVENCE';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 Ʊ��ת����
    INSERT INTO RWA_DEV.RWA_PJ_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                       	 	 --��ͬID
                ,MITIGATIONID                     	 	 --������ID
                ,MITIGCATEGORY                    	 	 --����������
                ,SGUARCONTRACTID                  	 	 --Դ������ͬID
                ,GROUPID                          	 	 --������
    )    
    SELECT
          TO_DATE(p_data_dt_str,'YYYYMMDD') , --��������
          p_data_dt_str , --������ˮ��
          T1.ACCT_NO  , --��ͬID
          'PJ' || T1.CRDT_BIZ_ID  , --������ID
          '03'  , --����������
          'PJ' || T1.CRDT_BIZ_ID  , --Դ������ͬID
          ''   --������
    FROM  BRD_BILL T1
    WHERE T1.ATL_PAY_AMT <> 0 --ȡ�Ķ��Ǳ���
            AND SUBSTR(T1.SBJT_CD, 1, 6) IN (
                '130101', --�����ʲ�-���гжһ�Ʊ����
                '130103', --�����ʲ�-���гжһ�Ʊת����
                '130102' --��ҵ��Ʊ����         ת������ȡ���ж��У�����ҪƱ����Ϊ����
            )
            AND T1.DATANO=p_data_dt_str;
    COMMIT;    

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_PJ_CMRELEVENCE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_PJ_CMRELEVENCE;

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '��ͬ���������('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_PJ_CMRELEVENCE;
/

