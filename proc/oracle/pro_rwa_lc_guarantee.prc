CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_GUARANTEE(
			 											p_data_dt_str	IN	VARCHAR2,		--��������
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_LC_GUARANTEE
    ʵ�ֹ���:���ϵͳ-Ͷ��-��֤(������Դ���ϵͳ���ʹܼƻ�Ͷ�������Ϣȫ������RWA���Ͷ����ӿڱ�֤����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2015-05-26
    ��  λ	:�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.ZGS_INVESTASSETDETAIL|�ʲ������
    Դ  ��2 :RWA_DEV.ZGS_FINANCING_INFO|��Ʒ��Ϣ��
    Դ  ��3 :RWA_DEV.ZGS_ATINTRUST_PLAN|�ʲ�����ƻ���
    --Դ	 ��4 :RWA.RWA_WS_FCII_PLAN|�ʹܼƻ����Ͷ�ʲ�¼�� ����
    --Դ  ��5 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ�� ����
    Ŀ���	 :RWA_DEV.RWA_LC_GUARANTEE|����ϵͳͶ���ౣ֤��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
  */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_GUARANTEE';
  --�����쳣����
  v_raise EXCEPTION;
  --�����ж�ֵ����
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_GUARANTEE';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    INSERT INTO RWA_DEV.RWA_LC_GUARANTEE(
         				 DataDate          												  --��������
								,DataNo                                     --������ˮ��
								,GuaranteeID                                --��֤ID
								,SSysID                                     --ԴϵͳID
								,GuaranteeConID                             --��֤��ͬID
								,GuarantorID                                --��֤��ID
								,CreditRiskDataType                         --���÷�����������
								,GuaranteeWay                            		--������ʽ
								,QualFlagSTD                            		--Ȩ�ط��ϸ��ʶ
								,QualFlagFIRB                               --�����������ϸ��ʶ
								,GuaranteeTypeSTD                           --Ȩ�ط���֤����
								,GuarantorSdvsSTD                           --Ȩ�ط���֤��ϸ��
								,GuaranteeTypeIRB                           --��������֤����
								,GuaranteeAmount                            --��֤�ܶ�
								,Currency                                   --����
								,StartDate                                  --��ʼ����
								,DueDate                                    --��������
								,OriginalMaturity                           --ԭʼ����
								,ResidualM                                  --ʣ������
								,GuarantorIRating                           --��֤���ڲ�����
								,GuarantorPD                                --��֤��ΥԼ����
								,GroupID                                    --������
    )
    WITH TEMP_INVESTASSETDETAIL AS (
        SELECT  DISTINCT
        				T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
           --AND T4.FLD_INCOME_TYPE <> '3'	--3���ų��Ǳ������� 20190625 ��2�������˵��²�ѯ���Ϊ0
           AND T4.DATANO = p_data_dt_str
         WHERE T3.FLD_ASSET_TYPE = '24'																			-- 2��ծȯ��24���ʲ�����ƻ�
           AND T3.FLD_ASSET_STATUS = '1' 																		--1��״̬����
           AND T3.FLD_ASSET_FLAG = '1'   																		--1����Ʋ�Ʒ
           AND T3.FLD_DATE  = p_data_dt_str																	--��Ч����Ʋ�Ʒ���ֵ����ÿ�ո���
           AND T3.DATANO = p_data_dt_str
    )
    , TMP_CUST_IRATING AS (
   								SELECT CUSTID,
									       CUSTNAME,
									       ORGCERTCODE,
									       MODELID,
									       PDLEVEL,
									       PDADJLEVEL,
									       PD,
									       PDVAVLIDDATE
									  FROM RWA_DEV.RWA_TEMP_PDLEVEL
									 WHERE ROWID IN
									       (SELECT MAX(ROWID) FROM RWA_DEV.RWA_TEMP_PDLEVEL GROUP BY ORGCERTCODE)
   	)
    SELECT
         				 TO_DATE(p_data_dt_str,'YYYYMMDD')								AS  DATADATE          	 --��������
         				,p_data_dt_str														 		    AS	DATANO               --������ˮ��
         				,T2.C_PRD_CODE																		AS	GUARANTEEID          --��֤ID
								,'LC'																						  AS	SSYSID               --ԴϵͳID
								,T2.C_PRD_CODE																		AS	GUARANTEECONID       --��֤��ͬID
								,'LC' || T2.C_GUARANTOR_PAPERTYPE || T2.C_GUARANTOR_NO
																																	AS	GUARANTORID          --��֤��ID
								,'01'																							AS	CREDITRISKDATATYPE   --���÷�����������      					Ĭ�ϣ�һ�������(01)
								,CASE WHEN T2.C_GUARANTEE_FIRST = '010' THEN '010'
								 ELSE T2.C_GUARANTEE_FOURTH
								 END																						  AS	GUARANTEEWAY       	 --������ʽ
								,''																								AS	QUALFLAGSTD          --Ȩ�ط��ϸ��ʶ
								,''																								AS	QUALFLAGFIRB         --�����������ϸ��ʶ
								,''																								AS	GUARANTEETYPESTD     --Ȩ�ط���֤����
								,''																								AS	GUARANTORSDVSSTD     --Ȩ�ط���֤��ϸ��
								,''																								AS	GUARANTEETYPEIRB     --��������֤����
								,T2.F_GUARANTEE_AMT																AS	GUARANTEEAMOUNT      --��֤�ܶ�
								,NVL(T2.C_GUARANTEE_CURR,'CNY')										AS	CURRENCY             --����
                ,T2.D_VALUE_DATE                             			AS StartDate             --��ʼ����
        				,T2.D_END_DATE                               			AS DueDate               --��������
        				,CASE WHEN (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(T2.D_VALUE_DATE,'YYYYMMDD')) / 365 < 0
        				      THEN 0
        				      ELSE (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(T2.D_VALUE_DATE,'YYYYMMDD')) / 365
        				 END																				 			AS OriginalMaturity      --ԭʼ����
        				,CASE WHEN (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0
        				      THEN 0
        				      ELSE (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
        				 END																		     			AS ResidualM             --ʣ������
								,T4.PDADJLEVEL																		AS	GUARANTORIRATING     --��֤���ڲ�����
								,T4.PD																						AS	GUARANTORPD          --��֤��ΥԼ����
								,''																								AS	GROUPID              --������
    FROM				TEMP_INVESTASSETDETAIL T1																					--������ϸ������¼�¼
    INNER JOIN	RWA_DEV.ZGS_ATINTRUST_PLAN T2																			--�ʹܼƻ���
    ON					T1.FLD_ASSET_CODE = T2.C_PRD_CODE																	--���б��Ψһ�����Դ��ֶι���
    --AND					(T2.C_GUARANTEE_FIRST = '010'																			--��֤(010)
    --OR					T2.C_GUARANTEE_FOURTH IN ('004001004001','004001005001','004001006001','004001006002'))     					--����֤����������֤�������Ա������������Ա�������Ϊ��֤
    						--20190625 ���������˵��²�ѯ���Ϊ0
    AND					T2.DATANO = p_data_dt_str
    LEFT JOIN		TMP_CUST_IRATING T4
    ON					REPLACE(T2.C_GUARANTOR_NO,'-','') = REPLACE(T4.ORGCERTCODE,'-','')
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_GUARANTEE',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_GUARANTEE;
    --Dbms_output.Put_line('RWA_DEV.RWA_LC_GUARANTEE��ǰ��������ϵͳ-�ʹܼƻ�Ͷ�����ݼ�¼Ϊ:' || v_count || '��');


		--Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '���Ͷ���ౣ֤('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_LC_GUARANTEE;
/

