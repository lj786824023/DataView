CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_HG_SFTDETAIL_BAK(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_HG_SFTDETAIL_BAK
    ʵ�ֹ���:����ϵͳ-�ع�-֤ȯ���ʽ��������Ϣ(������Դ����ϵͳ��ծȯ��Ϣȫ������RW�ع��ӿڱ�֤ȯ���ʽ��������Ϣ����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-18
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.BRD_REPO|ծȯ�ع�
    Դ  ��2 :RWA_DEV.BRD_SECURITY_POSI|ծȯͷ����Ϣ
    Դ  ��3 :RWA_DEV.BRD_BOND|ծȯ��Ϣ

    Ŀ���  :RWA_DEV.RWA_HG_SFTDETAIL|����ϵͳ�ع���֤ȯ���ʽ��������Ϣ��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    PXL 2019/04/15 ȥ���Ϻ���ϵͳ��ر�ȥ����¼��ر�
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_HG_SFTDETAIL_BAK';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  --v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_HG_SFTDETAIL';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ϵͳ-���뷵��ծȯ�ع�-���ʽ-�ʽ�
    INSERT INTO RWA_DEV.RWA_HG_SFTDETAIL(
                 DataDate                              --��������
                ,DataNo                          	 		 --������ˮ��
                ,SFTDetailID                     	 		 --֤ȯ���ʽ�����ϸID
                ,SecuID                          	 		 --֤ȯID
                ,SSysID                          	 		 --ԴϵͳID
                ,ExposureID                      	 		 --���ձ�¶ID
                ,MasterNetAgreeID                	 		 --���������Э��ID
                ,BookType                        	 		 --�˻����
                ,TranRole                        	 		 --���׽�ɫ
                ,TradingAssetType                	 		 --�����ʲ�����
                ,ClaimsLevel                     	 		 --ծȨ����
                ,QualFlagSTD                     	 		 --Ȩ�ط��ϸ��ʶ
                ,QualFlagFIRB                    	 		 --�����������ϸ��ʶ
                ,CollateralSdvsSTD              	 		 --Ȩ�ط�����ѺƷϸ��
                ,StartDate                 				 		 --��ʼ����
                ,DueDate                         	 		 --��������
                ,OriginalMaturity                	 		 --ԭʼ����
                ,ResidualM                       	 		 --ʣ������
                ,AssetBalance                    	 		 --�ʲ����
                ,AssetCurrency                   	 		 --�ʲ�����
                ,AppZeroHaircutsFlag             	 		 --�Ƿ��������ۿ�ϵ��
                ,InteHaircutsFlag                	 		 --���й����ۿ�ϵ����ʶ
                ,InternalHc                        		 --�ڲ��ۿ�ϵ��
                ,SecuIssuerID                    	 		 --֤ȯ������ID
                ,BondIssueIntent                 	 		 --ծȯ����Ŀ��
                ,FCType                          	 		 --������ѺƷ����
                ,ABSFlag                         	 		 --�ʲ�֤ȯ����ʶ
                ,RatingDurationType              	 		 --������������
                ,SecuIssueRating                 	 		 --֤ȯ���еȼ�
                ,SecuRevaFrequency               	 		 --֤ȯ�ع�Ƶ��
                ,RCERating														 --�����˾���ע����ⲿ����
    )
    SELECT    
    TO_DATE(p_data_dt_str,'YYYYMMDD') , --��������        
    p_data_dt_str , --������ˮ��       
    T1.ACCT_NO || 'MRFSZJ'  , --֤ȯ���ʽ�����ϸID        
    T1.ACCT_NO || 'MRFSZJ'  , --֤ȯID        
    'HG'  , --ԴϵͳID       
    T1.ACCT_NO  , --���ձ�¶ID        
    ''  , --���������Э��ID       
    '01'  , --�˻����        Ĭ�� �����˻�(01)
    '01'  , --���׽�ɫ        Ĭ�� ���ձ�¶(01)
    '01'  , --�����ʲ�����        Ĭ�� �ʽ�(01)
    '01'  , --ծȨ����        Ĭ�� �߼�ծȨ(01)
    '1' , --Ȩ�ط��ϸ��ʶ       Ĭ�� �ϸ�(1)
    '1' , --�����������ϸ��ʶ       Ĭ�� �ϸ�(1)
    '1' , --Ȩ�ط�����ѺƷϸ��       Ĭ�� �ֽ����ʲ�(01)
    T1.START_DT , --��ʼ����        
    T1.MATU_DT  , --��������        
    CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365<0
                          THEN 0
                          ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365
                    END  , --ԭʼ����        ��λ����
    CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                  THEN 0
                  ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
            END  , --ʣ������        ��λ����
    T1.CASH_NOMINAL , --�ʲ����        �����ʲ������=�������+Ӧ�շ���+������Ϣ+����ǷϢ, ����֤ȯ���ʽ��ף���Ϊ֤ȯ��ֵ��ع����
    T1.CASH_CCY_CD  , --�ʲ�����        
    '0' , --�Ƿ��������ۿ�ϵ��       Ĭ�� ��(0)
    '0' , --���й����ۿ�ϵ����ʶ        Ĭ�� ��(0)
    NULL  , --�ڲ��ۿ�ϵ��        Ĭ�� ��
    T3.ISSUER_CODE  , --֤ȯ������ID       Ĭ�� ��
    '02'  , --ծȯ����Ŀ��        Ĭ�� ����(02)
    '01'  , --������ѺƷ����       Ĭ�� �ֽ��ֽ�ȼ���(01)
    '0'   , --�ʲ�֤ȯ����ʶ       Ĭ�� ��(0)
    ''  , --������������        Ĭ�� ��
    ''  , --֤ȯ���еȼ�        Ĭ�� ��
    1 , --֤ȯ�ع�Ƶ��        Ĭ�� 1��
    ''  --�����˾���ע����ⲿ����        Ĭ�� ��
    FROM  RWA_DEV.BRD_REPO      T1      
    LEFT JOIN RWA_DEV.BRD_SECURITY_POSI     T2  ON T1.ACCT_NO = T2.ACCT_NO    
    LEFT JOIN RWA_DEV.BRD_BOND      T3  ON T2.SECURITY_REFERENCE = T3.BOND_ID   
   WHERE T1.CASH_NOMINAL <> 0
     --AND T1.CLIENT_PROPRIETARY = 'T'  --���ʽ  Դ��Ϊ�գ���ʱ����
     AND T1.REPO_TYPE IN ( '4', 'RB')  --���뷵��
     AND T1.PRINCIPAL_GLNO LIKE '111103%'  --���뷵��ծȯ�ع�-���ʽ-�ʽ�
     ;

    COMMIT;

    --2.2 ���뷵��ծȯ�ع�-���ʽ-֤ȯ
    INSERT INTO RWA_DEV.RWA_HG_SFTDETAIL(
                 DataDate                              --��������
                ,DataNo                          	 		 --������ˮ��
                ,SFTDetailID                     	 		 --֤ȯ���ʽ�����ϸID
                ,SecuID                          	 		 --֤ȯID
                ,SSysID                          	 		 --ԴϵͳID
                ,ExposureID                      	 		 --���ձ�¶ID
                ,MasterNetAgreeID                	 		 --���������Э��ID
                ,BookType                        	 		 --�˻����
                ,TranRole                        	 		 --���׽�ɫ
                ,TradingAssetType                	 		 --�����ʲ�����
                ,ClaimsLevel                     	 		 --ծȨ����
                ,QualFlagSTD                     	 		 --Ȩ�ط��ϸ��ʶ
                ,QualFlagFIRB                    	 		 --�����������ϸ��ʶ
                ,CollateralSdvsSTD              	 		 --Ȩ�ط�����ѺƷϸ��
                ,StartDate                 				 		 --��ʼ����
                ,DueDate                         	 		 --��������
                ,OriginalMaturity                	 		 --ԭʼ����
                ,ResidualM                       	 		 --ʣ������
                ,AssetBalance                    	 		 --�ʲ����
                ,AssetCurrency                   	 		 --�ʲ�����
                ,AppZeroHaircutsFlag             	 		 --�Ƿ��������ۿ�ϵ��
                ,InteHaircutsFlag                	 		 --���й����ۿ�ϵ����ʶ
                ,InternalHc                        		 --�ڲ��ۿ�ϵ��
                ,SecuIssuerID                    	 		 --֤ȯ������ID
                ,BondIssueIntent                 	 		 --ծȯ����Ŀ��
                ,FCType                          	 		 --������ѺƷ����
                ,ABSFlag                         	 		 --�ʲ�֤ȯ����ʶ
                ,RatingDurationType              	 		 --������������
                ,SecuIssueRating                 	 		 --֤ȯ���еȼ�
                ,SecuRevaFrequency               	 		 --֤ȯ�ع�Ƶ��
                ,RCERating														 --�����˾���ע����ⲿ����
    )
    SELECT
    TO_DATE(p_data_dt_str,'YYYYMMDD') , --��������        
    p_data_dt_str , --������ˮ��       
    T1.ACCT_NO || 'MRFSZQ'  , --֤ȯ���ʽ�����ϸID        
    T1.ACCT_NO || 'MRFSZQ'  , --֤ȯID        
    'HG'  , --ԴϵͳID       
    T1.ACCT_NO  , --���ձ�¶ID        
    ''  , --���������Э��ID       
    '01'  , --�˻����        Ĭ�� �����˻�(01)
    '02'  , --���׽�ɫ        Ĭ�� 02
    '02'  , --�����ʲ�����        Ĭ�� 02
    '01'  , --ծȨ����        Ĭ�� �߼�ծȨ(01)
    '1' , --Ȩ�ط��ϸ��ʶ       Ĭ�� �ϸ�(1)
    '1' , --�����������ϸ��ʶ       Ĭ�� �ϸ�(1)
    '1' , --Ȩ�ط�����ѺƷϸ��       Ĭ�� �ֽ����ʲ�(01)
    T1.START_DT , --��ʼ����        
    T1.MATU_DT  , --��������        
    CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365<0
                          THEN 0
                          ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365
                    END  , --ԭʼ����        ��λ����
    CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                  THEN 0
                  ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
            END  , --ʣ������        ��λ����
    T1.CASH_NOMINAL , --�ʲ����        �����ʲ������=�������+Ӧ�շ���+������Ϣ+����ǷϢ, ����֤ȯ���ʽ��ף���Ϊ֤ȯ��ֵ��ع����
    T1.CASH_CCY_CD  , --�ʲ�����        
    '0' , --�Ƿ��������ۿ�ϵ��       Ĭ�� ��(0)
    '0' , --���й����ۿ�ϵ����ʶ        Ĭ�� ��(0)
    NULL  , --�ڲ��ۿ�ϵ��        Ĭ�� ��
    T3.ISSUER_CODE  , --֤ȯ������ID       Ĭ�� ��
    '02'  , --ծȯ����Ŀ��        Ĭ�� ����(02)
    '09'  , --������ѺƷ����       Ĭ�� �ֽ��ֽ�ȼ���(01) 
    '0'   , --�ʲ�֤ȯ����ʶ       Ĭ�� ����(09)
    ''  , --������������        Ĭ�� ��
    ''  , --֤ȯ���еȼ�        Ĭ�� ��
    1 , --֤ȯ�ع�Ƶ��        Ĭ�� 1��
    ''  --�����˾���ע����ⲿ����        Ĭ�� ��
    FROM  RWA_DEV.BRD_REPO T1      
    LEFT JOIN RWA_DEV.BRD_SECURITY_POSI T2  ON T1.ACCT_NO = T2.ACCT_NO    
    LEFT JOIN RWA_DEV.BRD_BOND T3  ON T2.SECURITY_REFERENCE = T3.BOND_ID   
    WHERE T1.CASH_NOMINAL <> 0
         --AND T1.CLIENT_PROPRIETARY = 'T'  --���ʽ  Դ��Ϊ�գ���ʱ����
         AND T1.REPO_TYPE IN ( '4', 'RB')  --���뷵��
         AND T1.PRINCIPAL_GLNO LIKE '111103%'  --���뷵��ծȯ�ع�-���ʽ-ծȯ
    ;

    COMMIT;

    --2.3 �����ع�ծȯ�ع�-���ʽ-�ʽ�
    INSERT INTO RWA_DEV.RWA_HG_SFTDETAIL(
                 DataDate                              --��������
                ,DataNo                          	 		 --������ˮ��
                ,SFTDetailID                     	 		 --֤ȯ���ʽ�����ϸID
                ,SecuID                          	 		 --֤ȯID
                ,SSysID                          	 		 --ԴϵͳID
                ,ExposureID                      	 		 --���ձ�¶ID
                ,MasterNetAgreeID                	 		 --���������Э��ID
                ,BookType                        	 		 --�˻����
                ,TranRole                        	 		 --���׽�ɫ
                ,TradingAssetType                	 		 --�����ʲ�����
                ,ClaimsLevel                     	 		 --ծȨ����
                ,QualFlagSTD                     	 		 --Ȩ�ط��ϸ��ʶ
                ,QualFlagFIRB                    	 		 --�����������ϸ��ʶ
                ,CollateralSdvsSTD              	 		 --Ȩ�ط�����ѺƷϸ��
                ,StartDate                 				 		 --��ʼ����
                ,DueDate                         	 		 --��������
                ,OriginalMaturity                	 		 --ԭʼ����
                ,ResidualM                       	 		 --ʣ������
                ,AssetBalance                    	 		 --�ʲ����
                ,AssetCurrency                   	 		 --�ʲ�����
                ,AppZeroHaircutsFlag             	 		 --�Ƿ��������ۿ�ϵ��
                ,InteHaircutsFlag                	 		 --���й����ۿ�ϵ����ʶ
                ,InternalHc                        		 --�ڲ��ۿ�ϵ��
                ,SecuIssuerID                    	 		 --֤ȯ������ID
                ,BondIssueIntent                 	 		 --ծȯ����Ŀ��
                ,FCType                          	 		 --������ѺƷ����
                ,ABSFlag                         	 		 --�ʲ�֤ȯ����ʶ
                ,RatingDurationType              	 		 --������������
                ,SecuIssueRating                 	 		 --֤ȯ���еȼ�
                ,SecuRevaFrequency               	 		 --֤ȯ�ع�Ƶ��
                ,RCERating														 --�����˾���ע����ⲿ����
    )
    SELECT
          TO_DATE(p_data_dt_str,'YYYYMMDD') , --��������        
          p_data_dt_str , --������ˮ��       
          T1.ACCT_NO || 'MCHGZJ'  , --֤ȯ���ʽ�����ϸID        
          T1.ACCT_NO || 'MCHGZJ'  , --֤ȯID        
          'HG'  , --ԴϵͳID       
          T1.ACCT_NO  , --���ձ�¶ID        
          ''  , --���������Э��ID       
          '01'  , --�˻����        Ĭ�� �����˻�(01)
          '01'  , --���׽�ɫ        Ĭ�� ���ձ�¶(01)
          '01'  , --�����ʲ�����        Ĭ�� �ʽ�(01)
          '01'  , --ծȨ����        Ĭ�� �߼�ծȨ(01)
          '1' , --Ȩ�ط��ϸ��ʶ       Ĭ�� �ϸ�(1)
          '1' , --�����������ϸ��ʶ       Ĭ�� �ϸ�(1)
          '1' , --Ȩ�ط�����ѺƷϸ��       Ĭ�� �ֽ����ʲ�(01)
          T1.START_DT , --��ʼ����        
          T1.MATU_DT  , --��������        
          CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365<0
                                THEN 0
                                ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365
                          END  , --ԭʼ����        ��λ����
          CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                        THEN 0
                        ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                  END  , --ʣ������        ��λ����
          T1.CASH_NOMINAL , --�ʲ����        �����ʲ������=�������+Ӧ�շ���+������Ϣ+����ǷϢ, ����֤ȯ���ʽ��ף���Ϊ֤ȯ��ֵ��ع����
          T1.CASH_CCY_CD  , --�ʲ�����        
          '0' , --�Ƿ��������ۿ�ϵ��       Ĭ�� ��(0)
          '0' , --���й����ۿ�ϵ����ʶ        Ĭ�� ��(0)
          NULL  , --�ڲ��ۿ�ϵ��        Ĭ�� ��
          T3.ISSUER_CODE  , --֤ȯ������ID       Ĭ�� ��
          '02'  , --ծȯ����Ŀ��        Ĭ�� ����(02)
          '09'  , --������ѺƷ����       Ĭ�� ����(09)
          '0'   , --�ʲ�֤ȯ����ʶ       Ĭ�� ��(0)
          ''  , --������������        Ĭ�� ��
          ''  , --֤ȯ���еȼ�        Ĭ�� ��
          1 , --֤ȯ�ع�Ƶ��        Ĭ�� 1��
          ''  --�����˾���ע����ⲿ����        Ĭ�� ��
      FROM  BRD_REPO      T1      
      LEFT JOIN   BRD_SECURITY_POSI     T2  ON T1.ACCT_NO = T2.ACCT_NO    
      LEFT JOIN   BRD_BOND      T3  ON T2.SECURITY_REFERENCE = T3.BOND_ID   
      WHERE T1.CASH_NOMINAL <> 0
           --AND T1.CLIENT_PROPRIETARY = 'T'  --���ʽ  Դ��Ϊ�գ���ʱ����
           AND T1.REPO_TYPE IN ( '2', 'RS')  --���ع�
           AND T1.PRINCIPAL_GLNO LIKE '211103%'  --�����ع�ծȯ�ع�-���ʽ-�ʽ�
           ;

    COMMIT;

    --2.4 �����ع�ծȯ�ع�-���ʽ-֤ȯ
    INSERT INTO RWA_DEV.RWA_HG_SFTDETAIL(
                 DataDate                              --��������
                ,DataNo                          	 		 --������ˮ��
                ,SFTDetailID                     	 		 --֤ȯ���ʽ�����ϸID
                ,SecuID                          	 		 --֤ȯID
                ,SSysID                          	 		 --ԴϵͳID
                ,ExposureID                      	 		 --���ձ�¶ID
                ,MasterNetAgreeID                	 		 --���������Э��ID
                ,BookType                        	 		 --�˻����
                ,TranRole                        	 		 --���׽�ɫ
                ,TradingAssetType                	 		 --�����ʲ�����
                ,ClaimsLevel                     	 		 --ծȨ����
                ,QualFlagSTD                     	 		 --Ȩ�ط��ϸ��ʶ
                ,QualFlagFIRB                    	 		 --�����������ϸ��ʶ
                ,CollateralSdvsSTD              	 		 --Ȩ�ط�����ѺƷϸ��
                ,StartDate                 				 		 --��ʼ����
                ,DueDate                         	 		 --��������
                ,OriginalMaturity                	 		 --ԭʼ����
                ,ResidualM                       	 		 --ʣ������
                ,AssetBalance                    	 		 --�ʲ����
                ,AssetCurrency                   	 		 --�ʲ�����
                ,AppZeroHaircutsFlag             	 		 --�Ƿ��������ۿ�ϵ��
                ,InteHaircutsFlag                	 		 --���й����ۿ�ϵ����ʶ
                ,InternalHc                        		 --�ڲ��ۿ�ϵ��
                ,SecuIssuerID                    	 		 --֤ȯ������ID
                ,BondIssueIntent                 	 		 --ծȯ����Ŀ��
                ,FCType                          	 		 --������ѺƷ����
                ,ABSFlag                         	 		 --�ʲ�֤ȯ����ʶ
                ,RatingDurationType              	 		 --������������
                ,SecuIssueRating                 	 		 --֤ȯ���еȼ�
                ,SecuRevaFrequency               	 		 --֤ȯ�ع�Ƶ��
                ,RCERating														 --�����˾���ע����ⲿ����
    )
    SELECT
      TO_DATE(p_data_dt_str,'YYYYMMDD') , --��������        
      p_data_dt_str , --������ˮ��       
      T1.ACCT_NO || 'MCHGZQ'  , --֤ȯ���ʽ�����ϸID        
      T1.ACCT_NO || 'MCHGZQ'  , --֤ȯID        
      'HG'  , --ԴϵͳID       
      T1.ACCT_NO  , --���ձ�¶ID        
      ''  , --���������Э��ID       
      '01'  , --�˻����        Ĭ�� �����˻�(01)
      '02'  , --���׽�ɫ        Ĭ�� 02
      '02'  , --�����ʲ�����        Ĭ�� 02
      '01'  , --ծȨ����        Ĭ�� �߼�ծȨ(01)
      '1' , --Ȩ�ط��ϸ��ʶ       Ĭ�� �ϸ�(1)
      '1' , --�����������ϸ��ʶ       Ĭ�� �ϸ�(1)
      '1' , --Ȩ�ط�����ѺƷϸ��       Ĭ�� �ֽ����ʲ�(01)
      T1.START_DT , --��ʼ����        
      T1.MATU_DT  , --��������        
      CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365<0
                            THEN 0
                            ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365
                      END  , --ԭʼ����        ��λ����
      CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                    THEN 0
                    ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
              END  , --ʣ������        ��λ����
      T1.CASH_NOMINAL , --�ʲ����        �����ʲ������=�������+Ӧ�շ���+������Ϣ+����ǷϢ, ����֤ȯ���ʽ��ף���Ϊ֤ȯ��ֵ��ع����
      T1.CASH_CCY_CD  , --�ʲ�����        
      '0' , --�Ƿ��������ۿ�ϵ��       Ĭ�� ��(0)
      '0' , --���й����ۿ�ϵ����ʶ        Ĭ�� ��(0)
      NULL  , --�ڲ��ۿ�ϵ��        Ĭ�� ��
      T3.ISSUER_CODE  , --֤ȯ������ID       Ĭ�� ��
      '02'  , --ծȯ����Ŀ��        Ĭ�� ����(02)
      '01'  , --������ѺƷ����       Ĭ�� �ֽ��ֽ�ȼ���(01) 
      '0'   , --�ʲ�֤ȯ����ʶ       Ĭ�� ��(0)
      ''  , --������������        Ĭ�� ��
      ''  , --֤ȯ���еȼ�        Ĭ�� ��
      1 , --֤ȯ�ع�Ƶ��        Ĭ�� 1��
      ''  --�����˾���ע����ⲿ����        Ĭ�� ��
    FROM RWA_DEV.BRD_REPO      T1      
    LEFT JOIN RWA_DEV.BRD_SECURITY_POSI     T2  ON T1.ACCT_NO = T2.ACCT_NO    
    LEFT JOIN RWA_DEV.BRD_BOND      T3  ON T2.SECURITY_REFERENCE = T3.BOND_ID   
    WHERE T1.CASH_NOMINAL <> 0
     --AND T1.CLIENT_PROPRIETARY = 'T'  --���ʽ  --���ʽ  Դ��Ϊ�գ���ʱ����
     AND T1.REPO_TYPE IN ( '2', 'RS')  --���ع�
     AND T1.PRINCIPAL_GLNO LIKE '211103%'  --�����ع�ծȯ�ع�-���ʽ-֤ȯ				
    ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_HG_SFTDETAIL',cascade => true);


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_HG_SFTDETAIL;
    --Dbms_output.Put_line('RWA_DEV.RWA_HG_SFTDETAIL��ǰ����ĺ���ϵͳ-��ϻع�-֤ȯ-ծȯ-��ع����ݼ�¼Ϊ: ' || (v_count1 - v_count) || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '֤ȯ���ʽ���('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_HG_SFTDETAIL_BAK;
/

