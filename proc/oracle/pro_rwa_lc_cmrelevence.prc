CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_CMRELEVENCE(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_LC_CMRELEVENCE
    ʵ�ֹ���:���ϵͳ-�ʹܼƻ����Ͷ��-��ͬ���������(������Դ���ϵͳ���ʹܼƻ����Ͷ�������Ϣȫ������RWA���Ͷ�ʽӿڱ��ͬ�������������)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-21
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.ZGS_INVESTASSETDETAIL|�ʲ������
    Դ  ��2 :RWA_DEV.ZGS_FINANCING_INFO|��Ʒ��Ϣ��
    Դ  ��3 :RWA_DEV.ZGS_ATINTRUST_PLAN|�ʲ�����ƻ���
    --Դ	 ��4 :RWA.RWA_WS_FCII_PLAN|�ʹܼƻ����Ͷ�ʲ�¼�� ����
    --Դ  ��5 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ�� ����
    Ŀ���  :RWA_DEV.RWA_LC_CMRELEVENCE|����ϵͳƱ���������ͬ�����������
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_CMRELEVENCE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_CMRELEVENCE';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1���²�¼��Ļ�����ID
    UPDATE RWA.RWA_WS_FCII_PLAN SET MITIGATIONID = p_data_dt_str || 'LC' || lpad(rownum, 10, '0') WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;
    
    /*
    --2.2 ���ϵͳ-�ʹܼƻ�Ͷ��
    INSERT INTO RWA_DEV.RWA_LC_CMRELEVENCE(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,CONTRACTID                       	 	 --��ͬID
                ,MITIGATIONID                     	 	 --������ID
                ,MITIGCATEGORY                    	 	 --����������
                ,SGUARCONTRACTID                  	 	 --Դ������ͬID
                ,GROUPID                          	 	 --������
    )
    WITH TEMP_INVESTASSETDETAIL AS (
        SELECT T3.FLD_FINANC_CODE					AS FLD_FINANC_CODE
        			,T3.FLD_ASSET_CODE					AS FLD_ASSET_CODE
          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
           --AND T4.FLD_INCOME_TYPE <> '3'			--3���ų��Ǳ������� --20190625 --���������˵��²�ѯ���Ϊ0
           AND T4.DATANO = p_data_dt_str
         WHERE T3.FLD_ASSET_TYPE = '24'				-- 2��ծȯ��24���ʲ�����ƻ�
           AND T3.FLD_ASSET_STATUS = '1' 			--1��״̬����
           AND T3.FLD_ASSET_FLAG = '1'   			--1����Ʋ�Ʒ
           AND T3.FLD_DATE  = p_data_dt_str		--��Ч����Ʋ�Ʒ���ֵ����ÿ�ո��� 
           AND T3.DATANO = p_data_dt_str
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str													     AS DATANO                   --������ˮ��
                ,T1.FLD_FINANC_CODE || T1.FLD_ASSET_CODE		 AS CONTRACTID               --��ͬID
                ,T2.C_PRD_CODE		     											 AS MITIGATIONID     	 	 	 	 --������ID
                ,CASE WHEN T2.C_GUARANTEE_FOURTH IN ('004001004001','004001005001','004001006001','004001006002')     					--����֤����������֤�������Ա������������Ա�������Ϊ��֤
                			  OR T2.C_GUARANTEE_FIRST = '010' THEN '02'												 --��֤
                ELSE '03'																																 --����ѺƷ
                END	                                         AS MITIGCATEGORY            --����������
                ,''																			     AS SGUARCONTRACTID          --Դ������ͬID
                ,''                         				 				 AS GROUPID                	 --������

    FROM				TEMP_INVESTASSETDETAIL T1																					--������ϸ������¼�¼
    INNER JOIN	RWA_DEV.ZGS_ATINTRUST_PLAN T2																			--�ʹܼƻ���
    ON					T1.FLD_ASSET_CODE = T2.C_PRD_CODE																	--���б��Ψһ�����Դ��ֶι���
    AND					nvl(T2.C_GUARANTEE_FIRST,1) <> '005'		                          --�ų�����(005) 
    AND					T2.DATANO = p_data_dt_str
		;

    COMMIT;
    */

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_CMRELEVENCE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_CMRELEVENCE;
    --Dbms_output.Put_line('RWA_DEV.RWA_LC_CMRELEVENCE��ǰ��������ϵͳ-�ʹܼƻ�Ͷ�����ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

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
END PRO_RWA_LC_CMRELEVENCE;
/

