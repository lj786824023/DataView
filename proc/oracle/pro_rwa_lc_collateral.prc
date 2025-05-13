CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_COLLATERAL(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_LC_COLLATERAL
    ʵ�ֹ���:���ϵͳ-�ʹܼƻ�Ͷ��-����ѺƷ(������Դ���ϵͳ���ʹܼƻ�Ͷ�������Ϣȫ������RWA���Ͷ����ӿڱ����ѺƷ����)
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
    Ŀ���  :RWA_DEV.RWA_LC_COLLATERAL|���ϵͳͶ�������ѺƷ��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_COLLATERAL';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_COLLATERAL';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    INSERT INTO RWA_DEV.RWA_LC_COLLATERAL(
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
    WITH TEMP_INVESTASSETDETAIL AS (
        SELECT  DISTINCT
        				T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
           --AND T4.FLD_INCOME_TYPE <> '3'	--3���ų��Ǳ�������  --20190625���������˵��²�ѯ���Ϊ0
           AND T4.DATANO = p_data_dt_str
         WHERE T3.FLD_ASSET_TYPE = '24'																			-- 2��ծȯ��24���ʲ�����ƻ�
           AND T3.FLD_ASSET_STATUS = '1' 																		--1��״̬����
           AND T3.FLD_ASSET_FLAG = '1'   																		--1����Ʋ�Ʒ
           AND T3.FLD_DATE  = p_data_dt_str																	--��Ч����Ʋ�Ʒ���ֵ����ÿ�ո���
           AND T3.DATANO = p_data_dt_str
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str													     AS DATANO                   --������ˮ��
                ,T2.C_PRD_CODE															 AS COLLATERALID           	 --����ѺƷID
                ,'LC'                  											 AS SSYSID              	 	 --ԴϵͳID
                ,T2.C_PRD_CODE															 AS SGUARCONTRACTID        	 --Դ������ͬID
                ,T2.C_PRD_CODE															 AS SCOLLATERALID          	 --Դ����ѺƷID
                ,''		  	                         			 		 AS COLLATERALNAME         	 --����ѺƷ����                             ����ѺƷĿ¼ӳ��
                ,'LC' || T2.C_GUARANTOR_PAPERTYPE || T2.C_GUARANTOR_NO
                																             AS ISSUERID             	 	 --������ID                                 ��¼
                ,'LC' || T2.C_COUNTERPARTY_PAPERTYPE || T2.C_COUNTERPARTY_PAPERNO
                						                             	 	 AS PROVIDERID             	 --�ṩ��ID
                ,'01'                                  			 AS CREDITRISKDATATYPE     	 --���÷�����������                         Ĭ�ϣ�һ�������(01)
                ,T3.ATTRIBUTE1   									 			 		 AS GUARANTEEWAY           	 --������ʽ
                ,T2.C_GUARANTEE_SECOND          			 			 AS SOURCECOLTYPE     	     --Դ����ѺƷ����                        		�Խ���ѺƷĿ¼��Ĭ��Ϊ��Ѻ����(020)
                ,NVL(T2.C_GUARANTEE_FOURTH,NVL(T2.C_GUARANTEE_THIRD,T2.C_GUARANTEE_SECOND))
                											              				 AS SOURCECOLSUBTYPE         --Դ����ѺƷС��                        		�Խ���ѺƷĿ¼����Ʊ�����������гжһ�Ʊ����Ĭ��Ϊ����Ѻ-���гжһ�Ʊ(020210)��������Ĭ��Ϊ����Ѻ-��Ʊ����Ʊ��֧Ʊ(020220)��
               	,'0'																				 AS SPECPURPBONDFLAG  			 --�Ƿ�Ϊ�չ��������в�����������е�ծȯ		Ĭ�ϣ���(0)
                ,''                              		 				 AS QUALFLAGSTD            	 --Ȩ�ط��ϸ��ʶ                           RWA����
                ,''                                 				 AS QUALFLAGFIRB           	 --�����������ϸ��ʶ                       RWA����
                ,''								 		 									 		 AS COLLATERALTYPESTD 			 --Ȩ�ط�����ѺƷ����                    		���Ŵ�����ǰ��RWA����ӳ�䣨Ȩ�ط�-��ѺƷĿ¼�����Ŵ����ߺ�RWA����ӳ�䣨Ȩ�ط�-��ѺƷĿ¼��
                ,''									 				 								 AS COLLATERALSDVSSTD 		 	 --Ȩ�ط�����ѺƷϸ��                    		���Ŵ�����ǰ��RWA����ӳ�䣨Ȩ�ط�-��ѺƷĿ¼�����Ŵ����ߺ�RWA����ӳ�䣨Ȩ�ط�-��ѺƷĿ¼��
                ,''			                          					 AS COLLATERALTYPEIRB      	 --����������ѺƷ����                       RWA����ӳ�䣨������-��ѺƷĿ¼��
                ,T2.F_GUARANTEE_AMT										       AS COLLATERALAMOUNT     	 	 --��Ѻ�ܶ�
                ,NVL(T2.C_GUARANTEE_CURR,'CNY')						   AS CURRENCY               	 --����
                ,T2.D_VALUE_DATE                             AS StartDate             	 --��ʼ����
        				,T2.D_END_DATE                               AS DueDate               	 --��������
        				,CASE WHEN (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(T2.D_VALUE_DATE,'YYYYMMDD')) / 365 < 0
        				      THEN 0
        				      ELSE (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(T2.D_VALUE_DATE,'YYYYMMDD')) / 365
        				 END																				 AS OriginalMaturity      	 --ԭʼ����
        				,CASE WHEN (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0
        				      THEN 0
        				      ELSE (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
        				 END																		     AS ResidualM             	 --ʣ������
                ,'0'                                         AS INTEHAIRCUTSFLAG    	 	 --���й����ۿ�ϵ����ʶ                     Ĭ�ϣ���(0)
                ,NULL                                        AS INTERNALHC          	 	 --�ڲ��ۿ�ϵ��                             Ĭ�ϣ���
                ,''	                                         AS FCTYPE                 	 --������ѺƷ����                           Ĭ�ϣ���
                ,'0'                                         AS ABSFLAG             	 	 --�ʲ�֤ȯ����ʶ                           Ĭ�ϣ���(0)
                ,''                                          AS RATINGDURATIONTYPE  	 	 --������������                             Ĭ�ϣ���
                ,''                                          AS FCISSUERATING     			 --������ѺƷ���еȼ�                    		Ĭ�ϣ���
                ,CASE WHEN T2.C_GUARANTOR_TYPE = '01' THEN '01'
                 ELSE '02'
                 END                                         AS FCISSUERTYPE             --������ѺƷ���������                  		Ĭ�ϣ�����������(02)
                ,CASE WHEN T2.C_GUARANTOR_COUNTRY = 'CHN' THEN '01'
                      ELSE '02'
                 END                                       	 AS FCISSUERSTATE            --������ѺƷ������ע�����              		��¼
                ,CASE WHEN (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS FCRESIDUALM              --������ѺƷʣ������                    		Ĭ�ϣ���
                ,1                                           AS REVAFREQUENCY            --�ع�Ƶ��                              		Ĭ�ϣ�1
                ,''                                          AS GROUPID                  --������                              		Ĭ�ϣ���
                ,T4.RATINGRESULT														 AS RCERating								 --�����˾���ע����ⲿ����

    FROM				TEMP_INVESTASSETDETAIL T1																					--������ϸ������¼�¼
    INNER JOIN	RWA_DEV.ZGS_ATINTRUST_PLAN T2																			--�ʹܼƻ���
    ON					T1.FLD_ASSET_CODE = T2.C_PRD_CODE																	--���б��Ψһ�����Դ��ֶι���
    --20190625 ��2�������˵��²�ѯ���Ϊ0
    --AND					T2.C_GUARANTEE_FIRST NOT IN ('005','010')													--�ų�����(005)����֤(010)
    --AND					T2.C_GUARANTEE_FOURTH NOT IN ('004001004001','004001005001','004001006001','004001006002')     					--����֤����������֤�������Ա������������Ա�������Ϊ��֤
    AND					T2.DATANO = p_data_dt_str
    LEFT JOIN		RWA_DEV.NCM_COLLATERALTYPE_INFO T3
    ON					NVL(T2.C_GUARANTEE_FOURTH,NVL(T2.C_GUARANTEE_THIRD,T2.C_GUARANTEE_SECOND)) = T3.GUARANTYTYPE
    AND					T3.DATANO = p_data_dt_str
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T4
    ON					T2.C_GUARANTOR_COUNTRY = T4.COUNTRYCODE
    AND					T4.ISINUSE = '1'
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_COLLATERAL',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_COLLATERAL;
    --Dbms_output.Put_line('RWA_DEV.RWA_LC_COLLATERAL��ǰ��������ϵͳ-�ʹܼƻ�Ͷ�����ݼ�¼Ϊ: ' || v_count || ' ��');




    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '���Ͷ�������ѺƷ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_LC_COLLATERAL;
/

