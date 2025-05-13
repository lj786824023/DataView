CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_HG_COLLATERAL(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_HG_COLLATERAL
    ʵ�ֹ���:����ϵͳ-�ع�-����ѺƷ(������Դ����ϵͳ���ع��������Ϣȫ������RWA�ع���ӿڱ����ѺƷ����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-21
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.BRD_REPO_PORTFOLIO|ծȯ�ع���Ѻ��Ϣ
    Դ  ��2 :RWA_DEV.BRD_SECURITY_POSI|ծȯͷ��
    Դ  ��3 :RWA_DEV.BRD_BOND|ծȯ��Ϣ
    Դ  ��4 :RWA_DEV.BRD_REPO|ծȯ�ع�
    Դ  ��5 :RWA_DEV.BRD_BILL_REPO_PORTF|Ʊ�ݻع���Ѻ��Ϣ
    Դ  ��5 :RWA_DEV.BRD_BILL_REPO|Ʊ�ݻع���Ϣ 
    Դ  ��5 :RWA_DEV.BRD_BILL|Ʊ����Ϣ 
    Դ  ��5 :RWA.RWA_WP_COUNTRYRATING|����������Ϣ��      
    
    Ŀ���  :RWA_DEV.RWA_HG_COLLATERAL|����ϵͳ�ع������ѺƷ��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    pxl 2019/04/15 ��ȥ��¼��Ϣ���Ϻ�����ر�
    
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_HG_COLLATERAL';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  --v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_HG_COLLATERAL';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ���뷵��ծȯ�ع�-��Ѻʽ
    INSERT INTO RWA_DEV.RWA_HG_COLLATERAL(
                 DataDate                              --��������
                ,DataNo                                --������ˮ��
                ,CollateralID                       	 --����ѺƷID
                ,SSysID                             	 --ԴϵͳID
                ,SGuarContractID                    	 --Դ������ͬID
                ,SCollateralID                      	 --Դ����ѺƷID
                ,CollateralName                     	 --����ѺƷ����
                ,IssuerID                           	 --������ID
                ,ProviderID                         	 --�ṩ��ID
                ,CreditRiskDataType                 	 --���÷�����������
                ,GuaranteeWay                       	 --������ʽ
                ,SourceColType                      	 --Դ����ѺƷ����
                ,SourceColSubType                   	 --Դ����ѺƷС��
                ,SpecPurpBondFlag                   	 --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,QualFlagSTD                        	 --Ȩ�ط��ϸ��ʶ
                ,QualFlagFIRB                 				 --�����������ϸ��ʶ
                ,CollateralTypeSTD                  	 --Ȩ�ط�����ѺƷ����
                ,CollateralSdvsSTD                  	 --Ȩ�ط�����ѺƷϸ��
                ,CollateralTypeIRB                  	 --����������ѺƷ����
                ,CollateralAmount                   	 --��Ѻ�ܶ�
                ,Currency                           	 --����
                ,StartDate                          	 --��ʼ����
                ,DueDate                            	 --��������
                ,OriginalMaturity                   	 --ԭʼ����
                ,ResidualM                          	 --ʣ������
                ,InteHaircutsFlag                   	 --���й����ۿ�ϵ����ʶ
                ,InternalHc                         	 --�ڲ��ۿ�ϵ��
                ,FCType                             	 --������ѺƷ����
                ,ABSFlag                            	 --�ʲ�֤ȯ����ʶ
                ,RatingDurationType                 	 --������������
                ,FCIssueRating     										 --������ѺƷ���еȼ�
                ,FCIssuerType                          --������ѺƷ���������
                ,FCIssuerState                         --������ѺƷ������ע�����
                ,FCResidualM                           --������ѺƷʣ������
                ,RevaFrequency                         --�ع�Ƶ��
                ,GroupID                               --������
                ,RCERating														 --�����˾���ע����ⲿ����
    )
  SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --��������        
         p_data_dt_str, --������ˮ��       
         T1.REPO_REFERENCE || T1.SECURITY_REFERENCE, --����ѺƷID        
         'HG', --ԴϵͳID       
         'MRFSZQ' || T3.ACCT_NO, --Դ������ͬID       
         T1.REPO_REFERENCE || T1.SECURITY_REFERENCE, --Դ����ѺƷID       
         T4.BOND_FULL_NAME, --����ѺƷ����        
         T4.ISSUER_CODE, --������ID       
         T4.ISSUER_CODE, --�ṩ��ID       δ�ҵ�
         '01', --���÷�����������        Ĭ�� 01 һ�������
         '060', --������ʽ        Ĭ�� 060 ��Ѻ���� ���CODENO=GuarantyType
         '001003', --Դ����ѺƷ����       Ĭ�� 001003 ծȯ ���CMS_COLLATERALTYPE_INFO
         '001003', --Դ����ѺƷС��       Ĭ�� 001004 ծȯ ���CMS_COLLATERALTYPE_INFO
         '0', --�Ƿ�Ϊ�չ��������в�����������е�ծȯ       δ�ҵ�ծȯ����Ŀ�ģ�Ĭ�ϣ���
         '', --Ȩ�ط��ϸ��ʶ       
         '', --�����������ϸ��ʶ       
         '', --Ȩ�ط�����ѺƷ����       
         '', --Ȩ�ط�����ѺƷϸ��       
         '', --����������ѺƷ����       
         T1.FACE_AMOUNT, --��Ѻ�ܶ�        
         T1.CCY_CD, --����        
         T4.ISSUE_DATE, --��ʼ����        
         T4.MATU_DT, --��������        
         CASE
           WHEN (TO_DATE(T4.MATU_DT, 'YYYYMMDD') -
                TO_DATE(T4.ISSUE_DATE, 'YYYYMMDD')) / 365 < 0 THEN
            0
           ELSE
            (TO_DATE(T4.MATU_DT, 'YYYYMMDD') -
            TO_DATE(T4.ISSUE_DATE, 'YYYYMMDD')) / 365
         END, --ԭʼ����        
         CASE
           WHEN (TO_DATE(T4.MATU_DT, 'YYYYMMDD') -
                TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
            0
           ELSE
            (TO_DATE(T4.MATU_DT, 'YYYYMMDD') -
            TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
         END, --ʣ������        
         '0', --���й����ۿ�ϵ����ʶ        
         NULL, --�ڲ��ۿ�ϵ��        
         '', --������ѺƷ����       
         '0', --�ʲ�֤ȯ����ʶ       Ĭ�� ��
         '01', --������������        Ĭ�ϣ�01 ������������ �������������������������
         '01', --������ѺƷ���еȼ�       ƥ���ջ����������
         '02', --������ѺƷ���������        ������ѺƷ���������01 ��Ȩ02 ����������
         '01', --������ѺƷ������ע�����        ������ѺƷ������ע����ң�01 �й� 02 ���й�
         CASE
           WHEN (TO_DATE(T4.MATU_DT, 'YYYYMMDD') -
                TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
            0
           ELSE
            (TO_DATE(T4.MATU_DT, 'YYYYMMDD') -
            TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
         END, --������ѺƷʣ������       
         1, --�ع�Ƶ��        
         '',
         T5.RATINGRESULT --�����˾���ע����ⲿ����        ͨ��������ע��ع���ƥ��      
    FROM BRD_REPO_PORTFOLIO T1 --ծȯ�ع���Ѻ��Ϣ
   INNER JOIN BRD_REPO T3 --ծȯ�ع�
      ON T1.REPO_REFERENCE = T3.ACCT_NO --�ع����ױ��
     AND T1.DATANO = T3.DATANO
    LEFT JOIN BRD_BOND T4 --ծȯ��Ϣ
      ON T1.SECURITY_REFERENCE = T4.BOND_ID
     AND T1.DATANO = T4.DATANO
    LEFT JOIN RWA.RWA_WP_COUNTRYRATING T5
      ON '������ע��ع���' = T5.COUNTRYCODE
     AND T5.ISINUSE = '1'
  
   WHERE T3.CASH_NOMINAL <> 0
        --AND T3.CLIENT_PROPRIETARY = 'F'  --��Ѻʽ
     AND T3.REPO_TYPE IN ('4', 'RB') --���뷵��
     AND T3.PRINCIPAL_GLNO LIKE '111103%' --���뷵��ծȯ�ع�-��Ѻʽ-ծȯ
     AND T4.ISSUER_CODE IS NOT NULL
     AND T3.ACCT_NO IS NOT NULL
     AND T1.DATANO = p_data_dt_str
    ;
  
    COMMIT;

    --2.2 ���뷵��Ʊ�ݻع�-��Ѻʽ
    INSERT INTO RWA_DEV.RWA_HG_COLLATERAL(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,COLLATERALID                          --����ѺƷID
                ,SSYSID                                --ԴϵͳID
                ,SGUARCONTRACTID                       --Դ������ͬID
                ,SCOLLATERALID                         --Դ����ѺƷID
                ,COLLATERALNAME                        --����ѺƷ����
                ,ISSUERID                              --������ID
                ,PROVIDERID                            --�ṩ��ID
                ,CREDITRISKDATATYPE                    --���÷�����������
                ,QUALFLAGSTD                           --Ȩ�ط��ϸ��ʶ
                ,QUALFLAGFIRB                          --�����������ϸ��ʶ
                ,COLLATERALAMOUNT                      --��Ѻ�ܶ�
                ,CURRENCY                              --����
                ,GUARANTEEWAY                          --������ʽ
                ,SOURCECOLTYPE                         --Դ����ѺƷ����
                ,SOURCECOLSUBTYPE                      --Դ����ѺƷС��
                ,COLLATERALTYPEIRB                     --����������ѺƷ����
                ,COLLATERALTYPESTD                     --Ȩ�ط�����ѺƷ����
                ,COLLATERALSDVSSTD                     --Ȩ�ط�����ѺƷϸ��
                ,SPECPURPBONDFLAG                      --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,INTEHAIRCUTSFLAG                      --���й����ۿ�ϵ����ʶ
                ,INTERNALHC                            --�ڲ��ۿ�ϵ��
                ,FCTYPE                                --������ѺƷ����
                ,ABSFLAG                               --�ʲ�֤ȯ����ʶ
                ,RATINGDURATIONTYPE                    --������������
                ,FCISSUERATING                         --������ѺƷ���еȼ�
                ,FCISSUERTYPE                          --������ѺƷ���������
                ,FCISSUERSTATE                         --������ѺƷ������ע�����
                ,FCRESIDUALM                           --������ѺƷʣ������
                ,REVAFREQUENCY                         --�ع�Ƶ��
                ,GROUPID                               --������
                ,RCERating                             --�����˾���ע����ⲿ����
    )
    SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --��������        
           p_data_dt_str, --������ˮ��       
           T1.REPO_REFERENCE, --����ѺƷID        
           'HG', --ԴϵͳID       
           'MRFSPJ' || T2.ACCT_NO, --Դ������ͬID       
           T1.SECURITY_REFERENCE, --Դ����ѺƷID       
           CASE
             WHEN T3.SBJT_CD IN
                  ('11110201', '11110203', '11110206', '11110208') THEN
              '���гжһ�Ʊ'
             ELSE
              '��ҵ�жһ�Ʊ'
           END, --����ѺƷ����        Ʊ�����ƣ���������֤
           --T2.CUST_NO          , --������ID       
           N.CUSTOMERID,
           T1.REMITTER_CUST_NO, --�ṩ��ID       ��Ʊ�ˣ���������֤
           '01', --���÷�����������        Ĭ�� 01 һ�������
           NULL, --Ȩ�ط��ϸ��ʶ
           NULL, --�����������ϸ��ʶ
           T1.BILL_AMOUNT, --��Ѻ�ܶ�        
           T2.CASH_CCY_CD, --����   
           '060', --������ʽ        Ĭ�� 060 ��Ѻ���� ���CODENO=GuarantyType
           '001004', --Դ����ѺƷ����       Ĭ�ϣ�Ʊ��(001004)
           '001004', --Դ����ѺƷС��       Ĭ�ϣ�Ʊ��(001004)
           NULL, --����������ѺƷ����
           NULL, --Ȩ�ط�����ѺƷ����
           NULL, --Ȩ�ط�����ѺƷϸ��
           '0', --�Ƿ�Ϊ�չ��������в�����������е�ծȯ       Ĭ�ϣ���
           T1.OUT_BILL_DT, --��ʼ����        
           T1.END_BILL_DT, --��������        
           CASE
             WHEN (TO_DATE(T1.END_BILL_DT, 'YYYYMMDD') -
                  TO_DATE(T1.OUT_BILL_DT, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T1.END_BILL_DT, 'YYYYMMDD') -
              TO_DATE(T1.OUT_BILL_DT, 'YYYYMMDD')) / 365
           END, --ԭʼ����        
           CASE
             WHEN (TO_DATE(T1.END_BILL_DT, 'YYYYMMDD') -
                  TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T1.END_BILL_DT, 'YYYYMMDD') -
              TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
           END, --ʣ������        
           '0', --���й����ۿ�ϵ����ʶ        
           NULL, --�ڲ��ۿ�ϵ��        
           '09', --������ѺƷ����       Ĭ�ϣ�����(09)
           '0', --�ʲ�֤ȯ����ʶ       Ĭ�� ��
           '', --������������        Ĭ�ϣ���
           '', --������ѺƷ���еȼ�       Ĭ�ϣ���
           '02', --������ѺƷ���������        ��Ҫ��ȷ���ж��пͻ������жϹ���01 ��Ȩ 02 ����������
           '01', --������ѺƷ������ע�����        ��Ҫ��ȷ���ж��й����жϹ���
           CASE
             WHEN (TO_DATE(T1.END_BILL_DT, 'YYYYMMDD') -
                  TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T1.END_BILL_DT, 'YYYYMMDD') -
              TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
           END, --������ѺƷʣ������       
           1, --�ع�Ƶ��        
           '', --������        
           '' --�����˾���ע����ⲿ����        Ŀǰ��
      FROM BRD_BILL_REPO_PORTF T1 --Ʊ�ݻع���Ѻ��Ϣ   
     INNER JOIN BRD_BILL_REPO T2 --Ʊ�ݻع���Ϣ
        ON T1.REPO_REFERENCE = T2.ACCT_NO
        AND T1.DATANO=T2.DATANO
       AND T1.SECURITY_REFERENCE = T2.SECURITY_REFERENCE
      LEFT JOIN NCM_CUSTOMER_INFO N
        ON T2.CUST_NO = N.MFCUSTOMERID
        AND T1.DATANO=N.DATANO
      LEFT JOIN BRD_BILL T3 --Ʊ����Ϣ 
        ON T1.REPO_REFERENCE = T3.ACCT_NO
        AND T1.DATANO=T3.DATANO
       AND T1.SECURITY_REFERENCE = T3.BILL_NO
      LEFT JOIN RWA.RWA_WP_COUNTRYRATING T4
        ON '������ע��ع���' = T4.COUNTRYCODE
       AND T4.ISINUSE = '1'
     WHERE T2.CASH_NOMINAL <> 0
          --AND T2.CLIENT_PROPRIETARY = 'Y'  --��Ѻʽ
       AND T2.REPO_TYPE IN ('4', 'RB') --���뷵�� 
       AND SUBSTR(T2.PRINCIPAL_GLNO, 1, 6) = '111102' --���뷵�۽����ʲ�-���뷵��Ʊ��    
       AND T1.DATANO = p_data_dt_str
    ;

    COMMIT;
    
    --2.2 ���뷵��Ʊ�ݻع�-���׶��ֲ������е���Ҫ��Ʊ����Ϊ����
    INSERT INTO RWA_DEV.RWA_HG_COLLATERAL(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,COLLATERALID                          --����ѺƷID
                ,SSYSID                                --ԴϵͳID
                ,SGUARCONTRACTID                       --Դ������ͬID
                ,SCOLLATERALID                         --Դ����ѺƷID
                ,COLLATERALNAME                        --����ѺƷ����
                ,ISSUERID                              --������ID
                ,PROVIDERID                            --�ṩ��ID
                ,CREDITRISKDATATYPE                    --���÷�����������
                ,QUALFLAGSTD                           --Ȩ�ط��ϸ��ʶ
                ,QUALFLAGFIRB                          --�����������ϸ��ʶ
                ,COLLATERALAMOUNT                      --��Ѻ�ܶ�
                ,CURRENCY                              --����
                ,GUARANTEEWAY                          --������ʽ
                ,SOURCECOLTYPE                         --Դ����ѺƷ����
                ,SOURCECOLSUBTYPE                      --Դ����ѺƷС��
                ,COLLATERALTYPEIRB                     --����������ѺƷ����
                ,COLLATERALTYPESTD                     --Ȩ�ط�����ѺƷ����
                ,COLLATERALSDVSSTD                     --Ȩ�ط�����ѺƷϸ��
                ,SPECPURPBONDFLAG                      --�Ƿ�Ϊ�չ��������в�����������е�ծȯ
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,INTEHAIRCUTSFLAG                      --���й����ۿ�ϵ����ʶ
                ,INTERNALHC                            --�ڲ��ۿ�ϵ��
                ,FCTYPE                                --������ѺƷ����
                ,ABSFLAG                               --�ʲ�֤ȯ����ʶ
                ,RATINGDURATIONTYPE                    --������������
                ,FCISSUERATING                         --������ѺƷ���еȼ�
                ,FCISSUERTYPE                          --������ѺƷ���������
                ,FCISSUERSTATE                         --������ѺƷ������ע�����
                ,FCRESIDUALM                           --������ѺƷʣ������
                ,REVAFREQUENCY                         --�ع�Ƶ��
                ,GROUPID                               --������
                ,RCERating                             --�����˾���ע����ⲿ����
    )
   SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --��������        
          p_data_dt_str, --������ˮ��       
          T1.SECURITY_REFERENCE, --����ѺƷID        
          'HG', --ԴϵͳID       
          'MRFSPJ' || T1.ACCT_NO, --Դ������ͬID       
          T1.SECURITY_REFERENCE, --Դ����ѺƷID       
          CASE
            WHEN T1.PRINCIPAL_GLNO IN
                 ('11110201', '11110203', '11110206', '11110208') THEN
             '���гжһ�Ʊ'
            ELSE
             '��ҵ�жһ�Ʊ'
          END, --����ѺƷ����        Ʊ�����ƣ���������֤
          --T2.CUST_NO          , --������ID       
          T1.CUST_NO,
          T1.CUST_NO, --�ṩ��ID       ��Ʊ�ˣ���������֤
          '01', --���÷�����������        Ĭ�� 01 һ�������
          NULL, --Ȩ�ط��ϸ��ʶ
          NULL, --�����������ϸ��ʶ
          T1.CASH_NOMINAL, --��Ѻ�ܶ�        
          T1.CASH_CCY_CD, --����   
          '060', --������ʽ        Ĭ�� 060 ��Ѻ���� ���CODENO=GuarantyType
          '001004', --Դ����ѺƷ����       Ĭ�ϣ�Ʊ��(001004)
          '001004', --Դ����ѺƷС��       Ĭ�ϣ�Ʊ��(001004)
          NULL, --����������ѺƷ����
          NULL, --Ȩ�ط�����ѺƷ����
          NULL, --Ȩ�ط�����ѺƷϸ��
          '0', --�Ƿ�Ϊ�չ��������в�����������е�ծȯ       Ĭ�ϣ���
          T1.START_DT, --��ʼ����        
          T1.MATU_DT, --��������        
          CASE
            WHEN (TO_DATE(T1.MATU_DT, 'YYYYMMDD') -
                 TO_DATE(T1.START_DT, 'YYYYMMDD')) / 365 < 0 THEN
             0
            ELSE
             (TO_DATE(T1.MATU_DT, 'YYYYMMDD') -
             TO_DATE(T1.START_DT, 'YYYYMMDD')) / 365
          END, --ԭʼ����        
          CASE
            WHEN (TO_DATE(T1.MATU_DT, 'YYYYMMDD') -
                 TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
             0
            ELSE
             (TO_DATE(T1.MATU_DT, 'YYYYMMDD') -
             TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
          END, --ʣ������        
          '0', --���й����ۿ�ϵ����ʶ        
          NULL, --�ڲ��ۿ�ϵ��        
          '09', --������ѺƷ����       Ĭ�ϣ�����(09)
          '0', --�ʲ�֤ȯ����ʶ       Ĭ�� ��
          '', --������������        Ĭ�ϣ���
          '', --������ѺƷ���еȼ�       Ĭ�ϣ���
          '02', --������ѺƷ���������        ��Ҫ��ȷ���ж��пͻ������жϹ���01 ��Ȩ 02 ����������
          '01', --������ѺƷ������ע�����        ��Ҫ��ȷ���ж��й����жϹ���
          CASE
            WHEN (TO_DATE(T1.MATU_DT, 'YYYYMMDD') -
                 TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
             0
            ELSE
             (TO_DATE(T1.MATU_DT, 'YYYYMMDD') -
             TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
          END, --������ѺƷʣ������       
          1, --�ع�Ƶ��        
          '', --������        
          '' --�����˾���ע����ⲿ����        Ŀǰ��
     FROM RWA_DEV.BRD_BILL_REPO T1 --Ʊ�ݻع�            
     LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO T3
       ON T1.CUST_NO = T3.MFCUSTOMERID
       AND T1.DATANO=T3.DATANO
      AND T3.CUSTOMERTYPE NOT LIKE '03%' --�Թ��ͻ�                   
    WHERE T1.CASH_NOMINAL <> 0 --������Ч����
      AND T1.PRINCIPAL_GLNO IS NOT NULL --��ALM���з���  ��ĿΪ�յ����ݲ�����Ϊ��ʷ����
      AND SUBSTR(T1.PRINCIPAL_GLNO, 1, 6) = '111102' --���뷵�۽����ʲ�-���뷵��Ʊ��
      AND (T1.CLIENT_PROPRIETARY <> 'N' OR T1.CLIENT_PROPRIETARY IS NULL) --�Ƿ��������Ѻ NΪ���ʽ  ��N��Ѻʽ
      AND T3.CUSTOMERNAME NOT LIKE '%����%' --���׶��ֲ������в���Ҫ��Ʊ��������
      AND T1.DATANO = p_data_dt_str;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_HG_COLLATERAL',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_HG_COLLATERAL;
    --Dbms_output.Put_line('RWA_DEV.RWA_HG_COLLATERAL��ǰ����ĺ���ϵͳ-ծȯ�ع����ݼ�¼Ϊ: ' || (v_count1 - v_count) || ' ��');




    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '����ѺƷ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_HG_COLLATERAL;
/

