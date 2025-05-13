CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_CLIENT(p_data_dt_str IN  VARCHAR2, --��������
                                              p_po_rtncode  OUT VARCHAR2, --���ر��
                                              p_po_rtnmsg   OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_LC_CLIENT
    ʵ�ֹ���:����ʹ�ϵͳ���������Ϣȫ������RWA�ӿڱ����������)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2016-04-14
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.ZGS_ATBOND|ծȯ��Ϣ��
    Դ  ��2 :RWA_DEV.ZGS_ATINTRUST_PLAN|�ʲ�����ƻ���
    Դ  ��3 :RWA_DEV.ZGS_INVESTASSETDETAIL|������ϸ��
    Դ  ��4 :RWA_DEV.ZGS_FINANCING_INFO|��Ʒ��Ϣ��
    Ŀ���1 :RWA_DEV.RWA_LC_CLIENT|RWA����������Ϣ��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_CLIENT';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --���Ŀ����е�ԭ�м�¼
    --DELETE FROM RWA_DEV.RWA_LC_CLIENT WHERE DATADATE=TO_DATE(p_data_dt_str,'yyyyMMdd');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_CLIENT';

    --DBMS_OUTPUT.PUT_LINE('��ʼ������1�������롾��������-ծȯ��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --����1 ���롾��������-ծȯ��
    INSERT INTO RWA_DEV.RWA_LC_CLIENT(
         DataDate                   --��������
        ,DataNo                     --������ˮ��
        ,ClientID                   --��������ID
        ,SourceClientID             --Դ��������ID
        ,SSysID                     --ԴϵͳID
        ,ClientName                 --������������
        ,SOrgID                     --Դ����ID
        ,SOrgName                   --Դ��������
        ,OrgSortNo                  --�������������
        ,OrgID                      --��������ID
        ,OrgName                    --������������
        ,IndustryID                 --������ҵ����
        ,IndustryName               --������ҵ����
        ,ClientType                 --�����������
        ,ClientSubType              --��������С��
        ,RegistState                --ע����һ����
        ,RCERating                  --����ע����ⲿ����
        ,RCERAgency                 --����ע����ⲿ��������
        ,OrganizationCode           --��֯��������
        ,ConsolidatedSCFlag         --�Ƿ񲢱��ӹ�˾
        ,SLClientFlag               --רҵ����ͻ���ʶ
        ,SLClientType               --רҵ����ͻ�����
        ,ExpoCategoryIRB            --��������¶���
        ,ModelID                    --ģ��ID
        ,ModelIRating               --ģ���ڲ�����
        ,ModelPD                    --ģ��ΥԼ����
        ,IRating                    --�ڲ�����
        ,PD                         --ΥԼ����
        ,DefaultFlag                --ΥԼ��ʶ
        ,NewDefaultFlag             --����ΥԼ��ʶ
        ,DefaultDate                --ΥԼʱ��
        ,ClientERating              --���������ⲿ����
        ,CCPFlag                    --���뽻�׶��ֱ�ʶ
        ,QualCCPFlag                --�Ƿ�ϸ����뽻�׶���
        ,ClearMemberFlag            --�����Ա��ʶ
        ,CompanySize                --��ҵ��ģ
        ,SSMBFlag                   --��׼С΢��ҵ��ʶ
        ,SSMBFLAGSTD         				--Ȩ�ط���׼С΢��ҵ��ʶ
        ,AnnualSale                 --��˾�ͻ������۶�
        ,CountryCode                --ע����Ҵ���
        ,MSMBFlag										--���Ų�΢С��ҵ��ʶ
    )
  	WITH TEMP_INVESTASSETDETAIL AS (
							        SELECT  DISTINCT
							        				T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
							          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
							    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
							            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
							           AND T4.FLD_INCOME_TYPE <> '3'																		--3���ų��Ǳ�������
							           AND T4.DATANO = p_data_dt_str
                         AND T3.DATANO=T4.DATANO
							         WHERE T3.FLD_ASSET_TYPE = '2'																			-- 2��ծȯ��24���ʲ�����ƻ�
							           AND T3.FLD_ASSET_STATUS = '1' 																		--1��״̬����
							           AND T3.FLD_ASSET_FLAG = '1'   																		--1����Ʋ�Ʒ
							           AND (T3.C_ACC_TYPE <> 'D' OR T3.C_ACC_TYPE IS NULL)							--D�������࣬�ų���������
							           AND T3.FLD_DATE = p_data_dt_str																	--��Ч����Ʋ�Ʒ���ֵ����ÿ�ո���
							           AND T3.DATANO = p_data_dt_str
    )
    , TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID
    											,CUSTOMERNAME
    											,ORGID
    											,CERTTYPE
    											,CERTID
    											,INDUSTRYTYPE
    											,COUNTRYCODE
    											,RWACUSTOMERTYPE
    											,NEWDEFAULTFLAG
    											,DEFAULTDATE
    											,SCOPE
    											,ISSUPERVISESTANDARSMENT
    											,AVEFINANCESUM
    									FROM RWA_DEV.NCM_CUSTOMER_INFO
    								 WHERE ROWID IN (SELECT MAX(ROWID) FROM RWA_DEV.NCM_CUSTOMER_INFO WHERE DATANO = p_data_dt_str AND CERTTYPE IN ('Ent01','Ent02') GROUP BY CERTID)
    								 	 AND DATANO = p_data_dt_str
    )
 		, TMP_BND_CUST_INFO AS ( --��ծĬ�Ϸ�����Ϊ�й���������
 										SELECT CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN 'ZGZYZF'
 													 ELSE 'LC' || T1.C_ISSUER_IDENTIFICATION_TYPE || T1.C_ISSUER_IDENTIFICATION_NO
 													 END														 AS CUSTOMERID
 													,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN '�й���������'
 													 ELSE NVL(T1.C_RWA_PUBLISHNAME,T3.C_ORG_NAME)
 													 END														 AS CUSTOMERNAME
 													,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN '0' --��֯��������
 													 ELSE T1.C_ISSUER_IDENTIFICATION_TYPE
 													 END														 AS CERTTYPE
 													,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN 'ZGZYZFZZJGDM'
 													 ELSE T1.C_ISSUER_IDENTIFICATION_NO
 													 END														 AS CERTID
 													,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN 'CHN'
 													 ELSE T1.C_ISSUER_REGCOUNTRY_CODE
 													 END														 AS COUNTRYCODE
 													,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN '999999'
 													 ELSE DECODE(T1.C_ISSUER_INDUSTRY_CODE,NULL,'J66','','J66',T1.C_ISSUER_INDUSTRY_CODE)
 													 END														 AS INDUSTRYTYPE
 													,T1.C_ISSUER_ENTERPRISE_SIZE		 AS SCOPE
 													,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN '01'
 													 ELSE T1.C_SCORE_TYPE
 													 END														 AS ERATINGORG
 													,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN (SELECT RATINGRESULT FROM RWA.RWA_WP_COUNTRYRATING WHERE COUNTRYCODE = 'CHN' AND ISINUSE = '1')
 													 ELSE T4.DESCRATING
 													 END														 AS ERATING
 													,T1.C_ISSUER_RELEASE_DATE				 AS ERATINGDATE
 													,T1.C_ISSUERTYPE_1							 AS CLIENTTYPE
 													,T1.C_ISSUERTYPE_2							 AS CLIENTSUBTYPE
 											FROM RWA_DEV.ZGS_ATBOND T1
 								INNER JOIN TEMP_INVESTASSETDETAIL T2
 												ON T1.C_BOND_CODE = T2.FLD_ASSET_CODE
                        AND T1.DATANO=p_data_dt_str
 								 LEFT JOIN RWA_DEV.ZGS_ATTYORG T3
 								 				ON T1.C_PUBLISHER = T3.C_ORG_ID
    									 AND T3.DATANO = p_data_dt_str
    						 LEFT JOIN RWA_DEV.RWA_CD_RATING_MAPPING T4
    										ON T1.C_SCORE_TYPE = T4.SRCRATINGORG
    									 AND T1.C_BODY_SCORE = T4.SRCRATING
    									 AND T4.MAPPINGTYPE = 'LCI' 
 										 WHERE T1.DATANO = p_data_dt_str
 											 AND T1.ROWID IN (SELECT MAX(T3.ROWID)
 											 										FROM RWA_DEV.ZGS_ATBOND T3
 											 							INNER JOIN TEMP_INVESTASSETDETAIL T4
 											 											ON T3.C_BOND_CODE = T4.FLD_ASSET_CODE
 											 										 AND T3.DATANO = p_data_dt_str
 											 								GROUP BY CASE WHEN T3.C_BOND_TYPE IN ('01','17','19') THEN '0' ELSE T3.C_ISSUER_IDENTIFICATION_TYPE END,CASE WHEN T3.C_BOND_TYPE IN ('01','17','19') THEN 'ZGZYZFZZJGDM' ELSE T3.C_ISSUER_IDENTIFICATION_NO END)
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
        DISTINCT
        				 TO_DATE(p_data_dt_str,'YYYYMMDD')     																	AS DataDate            		--��������
        				,p_data_dt_str                         																	AS DataNo              		--������ˮ��
        				,'LC' || T1.CERTTYPE || T1.CERTID																				AS ClientID            		--��������ID
        				,'LC' || T1.CERTTYPE || T1.CERTID																				AS SourceClientID      		--Դ��������ID
        				,'LC'                                 																	AS SSysID              		--ԴϵͳID
        				,T1.CUSTOMERNAME																												AS ClientName          		--������������
        				,'9998'							              																	AS SOrgID              		--Դ����ID
        				,'��������'								      																	AS SOrgName            		--Դ��������
        				,'1'							              																	AS OrgSortNo           		--�������������
        				,'9998'								             																	AS OrgID               		--��������ID
        				,'��������'								      																	AS OrgName             		--������������
        				,T1.INDUSTRYTYPE										  																	AS IndustryID          		--������ҵ����
        				,T4.ITEMNAME								          																	AS IndustryName        		--������ҵ����
        				,T10.DITEMNO																														AS ClientType          		--�����������
        				,T11.DITEMNO																														AS ClientSubType       		--��������С��
        				,CASE WHEN T1.COUNTRYCODE = 'CHN' THEN '01'
        				 ELSE '02'
        				 END	                                 																	AS RegistState         		--ע����һ����
        				,T7.RATINGRESULT                       																	AS RCERating           		--����ע����ⲿ����
        				,'01'                                  																	AS RCERAgency          		--����ע����ⲿ��������
        				,T1.CERTID							              																	AS OrganizationCode    		--��֯��������
        				,CASE WHEN T1.CERTID = '91522301573318868K' OR REPLACE(T1.CERTID,'-','') = '573318868' THEN '1'
        				 ELSE '0'
        				 END	                                																	AS ConsolidatedSCFlag  		--�Ƿ񲢱��ӹ�˾
        				,'0'                                  																	AS SLClientFlag        		--רҵ����ͻ���ʶ
        				,''	                                  																	AS SLClientType        		--רҵ����ͻ�����
        				,''                                   																	AS ExpoCategoryIRB     		--��������¶���
        				,T8.MODELID                           																	AS ModelID             		--ģ��ID
        				,T8.PDLEVEL											                         								AS MODELIRATING        		--ģ���ڲ�����
                ,T8.PD									                                 								AS MODELPD             		--ģ��ΥԼ����
                ,T8.PDADJLEVEL										                      								AS IRATING             		--�ڲ�����
                ,T8.PD									                                 								AS PD                  		--ΥԼ����
        				,CASE WHEN T8.PDADJLEVEL = '0116' THEN '1' ELSE '0' END									AS DefaultFlag         		--ΥԼ��ʶ
        				,DECODE(NVL(T2.NEWDEFAULTFLAG,'1'),'0','1','0')													AS NewDefaultFlag      		--����ΥԼ��ʶ
        				,CASE WHEN T8.PDADJLEVEL = '0116' THEN TO_DATE(T8.PDVAVLIDDATE,'YYYYMMDD')
                 ELSE NULL
                 END														      																	AS DefaultDate         		--ΥԼʱ��
        				,T1.ERATING											      																	AS ClientERating       		--���������ⲿ����
        				,'0'                                  																	AS CCPFlag             		--���뽻�׶��ֱ�ʶ
        				,'0'                                   																	AS QualCCPFlag         		--�Ƿ�ϸ����뽻�׶���
        				,'0'                                   																	AS ClearMemberFlag     		--�����Ա��ʶ
        				,T1.SCOPE							                																	AS CompanySize         		--��ҵ��ģ
        				,NVL(T2.ISSUPERVISESTANDARSMENT,'0')   																	AS SSMBFlag            		--��׼С΢��ҵ��ʶ
        				,'0'											            																	AS SSMBFlagSTD         		--Ȩ�ط���׼С΢��ҵ��ʶ
        				,T2.AVEFINANCESUM                      																	AS AnnualSale          		--��˾�ͻ������۶�
        				,T1.COUNTRYCODE																													AS CountryCode            --ע����Ҵ���
        				,''																																			AS MSMBFlag								--���Ų�΢С��ҵ��ʶ

    FROM 				TMP_BND_CUST_INFO T1
    LEFT JOIN		TEMP_CUST_INFO T2
    ON					REPLACE(T1.CERTID,'-','') = REPLACE(T2.CERTID,'-','')
    LEFT JOIN		RWA.CODE_LIBRARY T4
    ON					T1.INDUSTRYTYPE = T4.ITEMNO
    AND					T4.CODENO = 'IndustryType'
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T7
    ON					T1.COUNTRYCODE = T7.COUNTRYCODE
    AND					T7.ISINUSE = '1'
    LEFT JOIN		TMP_CUST_IRATING T8
    ON					REPLACE(T1.CERTID,'-','') = REPLACE(T8.ORGCERTCODE,'-','')
    LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T10
	  ON					T1.CLIENTTYPE = T10.SITEMNO
	  AND					T10.SCODENO = 'ClientCategory'
	  AND					T10.SYSID = 'LC'
	  LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T11
	  ON					T1.CLIENTSUBTYPE = T11.SITEMNO
	  AND					T11.SCODENO = 'ClientCategory'
	  AND					T11.SYSID = 'LC'
   	;

		COMMIT;

    --����2 ���롾��������-�ʹܡ�
    INSERT INTO RWA_DEV.RWA_LC_CLIENT(
         DataDate                   --��������
        ,DataNo                     --������ˮ��
        ,ClientID                   --��������ID
        ,SourceClientID             --Դ��������ID
        ,SSysID                     --ԴϵͳID
        ,ClientName                 --������������
        ,SOrgID                     --Դ����ID
        ,SOrgName                   --Դ��������
        ,OrgSortNo                  --�������������
        ,OrgID                      --��������ID
        ,OrgName                    --������������
        ,IndustryID                 --������ҵ����
        ,IndustryName               --������ҵ����
        ,ClientType                 --�����������
        ,ClientSubType              --��������С��
        ,RegistState                --ע����һ����
        ,RCERating                  --����ע����ⲿ����
        ,RCERAgency                 --����ע����ⲿ��������
        ,OrganizationCode           --��֯��������
        ,ConsolidatedSCFlag         --�Ƿ񲢱��ӹ�˾
        ,SLClientFlag               --רҵ����ͻ���ʶ
        ,SLClientType               --רҵ����ͻ�����
        ,ExpoCategoryIRB            --��������¶���
        ,ModelID                    --ģ��ID
        ,ModelIRating               --ģ���ڲ�����
        ,ModelPD                    --ģ��ΥԼ����
        ,IRating                    --�ڲ�����
        ,PD                         --ΥԼ����
        ,DefaultFlag                --ΥԼ��ʶ
        ,NewDefaultFlag             --����ΥԼ��ʶ
        ,DefaultDate                --ΥԼʱ��
        ,ClientERating              --���������ⲿ����
        ,CCPFlag                    --���뽻�׶��ֱ�ʶ
        ,QualCCPFlag                --�Ƿ�ϸ����뽻�׶���
        ,ClearMemberFlag            --�����Ա��ʶ
        ,CompanySize                --��ҵ��ģ
        ,SSMBFlag                   --��׼С΢��ҵ��ʶ
        ,SSMBFLAGSTD         				--Ȩ�ط���׼С΢��ҵ��ʶ
        ,AnnualSale                 --��˾�ͻ������۶�
        ,CountryCode                --ע����Ҵ���
        ,MSMBFlag										--���Ų�΢С��ҵ��ʶ
    )
  	WITH TEMP_INVESTASSETDETAIL AS (
							        SELECT  DISTINCT
							        				T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
							          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
							    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
							            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
							           --AND T4.FLD_INCOME_TYPE <> '3' --3���ų��Ǳ������� --20190625 --���������˵��²�ѯ���Ϊ0
							           AND T4.DATANO = p_data_dt_str
                         AND T3.DATANO=T4.DATANO
							         WHERE T3.FLD_ASSET_TYPE = '24'																			-- 2��ծȯ��24���ʲ�����ƻ�
							           AND T3.FLD_ASSET_STATUS = '1' 																		--1��״̬����
							           AND T3.FLD_ASSET_FLAG = '1'   																		--1����Ʋ�Ʒ
							           AND T3.FLD_DATE = p_data_dt_str																	--��Ч����Ʋ�Ʒ���ֵ����ÿ�ո���
							           AND T3.DATANO = p_data_dt_str
    )
    , TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID
    											,CUSTOMERNAME
    											,ORGID
    											,CERTTYPE
    											,CERTID
    											,INDUSTRYTYPE
    											,COUNTRYCODE
    											,RWACUSTOMERTYPE
    											,NEWDEFAULTFLAG
    											,DEFAULTDATE
    											,SCOPE
    											,ISSUPERVISESTANDARSMENT
    											,AVEFINANCESUM
    									FROM RWA_DEV.NCM_CUSTOMER_INFO
    								 WHERE ROWID IN (SELECT MAX(ROWID) FROM RWA_DEV.NCM_CUSTOMER_INFO WHERE DATANO = p_data_dt_str AND CERTTYPE IN ('Ent01','Ent02') GROUP BY CERTID)
    								 	 AND DATANO = p_data_dt_str
    )
 		, TMP_PLN_CUST_INFO AS (
 										SELECT T1.C_COUNTERPARTY_NAME					 AS CUSTOMERNAME
 													,T1.C_COUNTERPARTY_PAPERTYPE		 AS CERTTYPE
 													,T1.C_COUNTERPARTY_PAPERNO			 AS CERTID
 													,T1.C_COUNTERPARTY_COUNTRYCODE	 AS COUNTRYCODE
 													,T1.C_COUNTERPARTY_INDUSTRYCODE	 AS INDUSTRYTYPE
 													,T1.C_COUNTERPARTY_LNSIZE				 AS SCOPE
 													,T1.C_COUNTERPARTY_FIRST				 AS CLIENTTYPE
 													,T1.C_COUNTERPARTY_SECOND				 AS CLIENTSUBTYPE
 											FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1
 								INNER JOIN TEMP_INVESTASSETDETAIL T2
 												ON T1.C_PRD_CODE = T2.FLD_ASSET_CODE
 										 WHERE T1.DATANO = p_data_dt_str
 											 AND T1.ROWID IN (SELECT MAX(T3.ROWID)
 											 										FROM RWA_DEV.ZGS_ATINTRUST_PLAN T3
 											 							INNER JOIN TEMP_INVESTASSETDETAIL T4
 											 											ON T3.C_PRD_CODE = T4.FLD_ASSET_CODE
 											 										 AND T3.DATANO = p_data_dt_str
 											 								GROUP BY T3.C_COUNTERPARTY_PAPERTYPE,T3.C_COUNTERPARTY_PAPERNO)
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
        				 TO_DATE(p_data_dt_str,'YYYYMMDD')     																	AS DataDate            		--��������
        				,p_data_dt_str                         																	AS DataNo              		--������ˮ��
        				,'LC' || T1.CERTTYPE || T1.CERTID																				AS ClientID            		--��������ID
        				,'LC' || T1.CERTTYPE || T1.CERTID																				AS SourceClientID      		--Դ��������ID
        				,'LC'                                 																	AS SSysID              		--ԴϵͳID
        				,T1.CUSTOMERNAME																												AS ClientName          		--������������
        				,'9998'							              																	AS SOrgID              		--Դ����ID
        				,'��������'								      																	AS SOrgName            		--Դ��������
        				,'1'							              																	AS OrgSortNo           		--�������������
        				,'9998'								             																	AS OrgID               		--��������ID
        				,'��������'								      																	AS OrgName             		--������������
        				,T1.INDUSTRYTYPE										  																	AS IndustryID          		--������ҵ����
        				,T4.ITEMNAME								          																	AS IndustryName        		--������ҵ����
        				,T10.DITEMNO																														AS ClientType          		--�����������
        				,T11.DITEMNO																														AS ClientSubType       		--��������С��
        				,CASE WHEN T1.COUNTRYCODE = 'CHN' THEN '01'
        				 ELSE '02'
        				 END	                                 																	AS RegistState         		--ע����һ����
        				,T7.RATINGRESULT                       																	AS RCERating           		--����ע����ⲿ����
        				,'01'                                  																	AS RCERAgency          		--����ע����ⲿ��������
        				,T1.CERTID								             																	AS OrganizationCode    		--��֯��������
        				,CASE WHEN T1.CERTID = '91522301573318868K' OR REPLACE(T1.CERTID,'-','') = '57331886-8' THEN '1'
        				 ELSE '0'
        				 END	                                																	AS ConsolidatedSCFlag  		--�Ƿ񲢱��ӹ�˾
        				,'0'                                  																	AS SLClientFlag        		--רҵ����ͻ���ʶ
        				,''	                                  																	AS SLClientType        		--רҵ����ͻ�����
        				,''                                   																	AS ExpoCategoryIRB     		--��������¶���
        				,T8.MODELID                           																	AS ModelID             		--ģ��ID
        				,T8.PDLEVEL											                         								AS MODELIRATING        		--ģ���ڲ�����
                ,T8.PD									                                 								AS MODELPD             		--ģ��ΥԼ����
                ,T8.PDADJLEVEL										                      								AS IRATING             		--�ڲ�����
                ,T8.PD									                                 								AS PD                  		--ΥԼ����
        				,CASE WHEN T8.PDADJLEVEL = '0116' THEN '1' ELSE '0' END									AS DefaultFlag         		--ΥԼ��ʶ
        				,DECODE(NVL(T2.NEWDEFAULTFLAG,'1'),'0','1','0')													AS NewDefaultFlag      		--����ΥԼ��ʶ
        				,CASE WHEN T8.PDADJLEVEL = '0116' THEN TO_DATE(T8.PDVAVLIDDATE,'YYYYMMDD')
                 ELSE NULL
                 END														      																	AS DefaultDate         		--ΥԼʱ��
        				,''															      																	AS ClientERating       		--���������ⲿ����
        				,'0'                                  																	AS CCPFlag             		--���뽻�׶��ֱ�ʶ
        				,'0'                                   																	AS QualCCPFlag         		--�Ƿ�ϸ����뽻�׶���
        				,'0'                                   																	AS ClearMemberFlag     		--�����Ա��ʶ
        				,T1.SCOPE							                																	AS CompanySize         		--��ҵ��ģ
        				,NVL(T2.ISSUPERVISESTANDARSMENT,'0')   																	AS SSMBFlag            		--��׼С΢��ҵ��ʶ
        				,'0'											            																	AS SSMBFlagSTD         		--Ȩ�ط���׼С΢��ҵ��ʶ
        				,T2.AVEFINANCESUM                      																	AS AnnualSale          		--��˾�ͻ������۶�
        				,T1.COUNTRYCODE																													AS CountryCode            --ע����Ҵ���
        				,''																																			AS MSMBFlag								--���Ų�΢С��ҵ��ʶ

    FROM 				TMP_PLN_CUST_INFO T1
    LEFT JOIN		TEMP_CUST_INFO T2
    ON					REPLACE(T1.CERTID,'-','') = REPLACE(T2.CERTID,'-','')
    LEFT JOIN		RWA.CODE_LIBRARY T4
    ON					T1.INDUSTRYTYPE = T4.ITEMNO
    AND					T4.CODENO = 'IndustryType'
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T7
    ON					T1.COUNTRYCODE = T7.COUNTRYCODE
    AND					T7.ISINUSE = '1'
    LEFT JOIN		TMP_CUST_IRATING T8
    ON					REPLACE(T1.CERTID,'-','') = REPLACE(T8.ORGCERTCODE,'-','')
    LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T10
	  ON					T1.CLIENTTYPE = T10.SITEMNO
	  AND					T10.SCODENO = 'ClientCategory'
	  AND					T10.SYSID = 'LC'
	  LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T11
	  ON					T1.CLIENTSUBTYPE = T11.SITEMNO
	  AND					T11.SCODENO = 'ClientCategory'
	  AND					T11.SYSID = 'LC'
    WHERE				'LC' || T1.CERTTYPE || T1.CERTID NOT IN (SELECT CLIENTID FROM RWA_DEV.RWA_LC_CLIENT)
    ;

		COMMIT;

		--����3 ���롾������-�ʹܡ�
		INSERT INTO RWA_DEV.RWA_LC_CLIENT(
         DataDate                   --��������
        ,DataNo                     --������ˮ��
        ,ClientID                   --��������ID
        ,SourceClientID             --Դ��������ID
        ,SSysID                     --ԴϵͳID
        ,ClientName                 --������������
        ,SOrgID                     --Դ����ID
        ,SOrgName                   --Դ��������
        ,OrgSortNo                  --�������������
        ,OrgID                      --��������ID
        ,OrgName                    --������������
        ,IndustryID                 --������ҵ����
        ,IndustryName               --������ҵ����
        ,ClientType                 --�����������
        ,ClientSubType              --��������С��
        ,RegistState                --ע����һ����
        ,RCERating                  --����ע����ⲿ����
        ,RCERAgency                 --����ע����ⲿ��������
        ,OrganizationCode           --��֯��������
        ,ConsolidatedSCFlag         --�Ƿ񲢱��ӹ�˾
        ,SLClientFlag               --רҵ����ͻ���ʶ
        ,SLClientType               --רҵ����ͻ�����
        ,ExpoCategoryIRB            --��������¶���
        ,ModelID                    --ģ��ID
        ,ModelIRating               --ģ���ڲ�����
        ,ModelPD                    --ģ��ΥԼ����
        ,IRating                    --�ڲ�����
        ,PD                         --ΥԼ����
        ,DefaultFlag                --ΥԼ��ʶ
        ,NewDefaultFlag             --����ΥԼ��ʶ
        ,DefaultDate                --ΥԼʱ��
        ,ClientERating              --���������ⲿ����
        ,CCPFlag                    --���뽻�׶��ֱ�ʶ
        ,QualCCPFlag                --�Ƿ�ϸ����뽻�׶���
        ,ClearMemberFlag            --�����Ա��ʶ
        ,CompanySize                --��ҵ��ģ
        ,SSMBFlag                   --��׼С΢��ҵ��ʶ
        ,SSMBFLAGSTD         				--Ȩ�ط���׼С΢��ҵ��ʶ
        ,AnnualSale                 --��˾�ͻ������۶�
        ,CountryCode                --ע����Ҵ���
        ,MSMBFlag										--���Ų�΢С��ҵ��ʶ
    )
  	WITH TEMP_INVESTASSETDETAIL AS (
							        SELECT  DISTINCT
							        				T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
							          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
							    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
							            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
							           AND T4.FLD_INCOME_TYPE <> '3'																		--3���ų��Ǳ�������
							           AND T4.DATANO = p_data_dt_str
							         WHERE T3.FLD_ASSET_TYPE = '24'																			-- 2��ծȯ��24���ʲ�����ƻ�
							           AND T3.FLD_ASSET_STATUS = '1' 																		--1��״̬����
							           AND T3.FLD_ASSET_FLAG = '1'   																		--1����Ʋ�Ʒ
							           AND T3.FLD_DATE = p_data_dt_str																	--��Ч����Ʋ�Ʒ���ֵ����ÿ�ո���
							           AND T3.DATANO = p_data_dt_str
    )
    , TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID
    											,CUSTOMERNAME
    											,ORGID
    											,CERTTYPE
    											,CERTID
    											,INDUSTRYTYPE
    											,COUNTRYCODE
    											,RWACUSTOMERTYPE
    											,NEWDEFAULTFLAG
    											,DEFAULTDATE
    											,SCOPE
    											,ISSUPERVISESTANDARSMENT
    											,AVEFINANCESUM
    									FROM RWA_DEV.NCM_CUSTOMER_INFO
    								 WHERE ROWID IN (SELECT MAX(ROWID) FROM RWA_DEV.NCM_CUSTOMER_INFO WHERE DATANO = p_data_dt_str AND CERTTYPE IN ('Ent01','Ent02') GROUP BY CERTID)
    								 	 AND DATANO = p_data_dt_str
    )
 		, TMP_PLN_CUST_INFO AS (
 										SELECT T1.C_GUARANTOR_NAME						 AS CUSTOMERNAME
 													,T1.C_GUARANTOR_PAPERTYPE				 AS CERTTYPE
 													,T1.C_GUARANTOR_NO							 AS CERTID  --20190625 ZGS_ATINTRUST_PLAN��C_GUARANTOR_NOȫΪ��
 													,T1.C_GUARANTOR_COUNTRY					 AS COUNTRYCODE
 													,'999999'												 AS INDUSTRYTYPE
 													,T1.C_GUARANTOR_TYPE						 AS CLIENTTYPE
 													,T1.C_GUARANTOR_TYPETWO					 AS CLIENTSUBTYPE
 											FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1
 								INNER JOIN TEMP_INVESTASSETDETAIL T2
 												ON T1.C_PRD_CODE = T2.FLD_ASSET_CODE
 										 WHERE T1.DATANO = p_data_dt_str
 											 AND T1.ROWID IN (SELECT MAX(T3.ROWID)
 											 										FROM RWA_DEV.ZGS_ATINTRUST_PLAN T3
 											 							INNER JOIN TEMP_INVESTASSETDETAIL T4
 											 											ON T3.C_PRD_CODE = T4.FLD_ASSET_CODE
 											 										 AND T3.DATANO = p_data_dt_str
 											 										 AND T3.C_GUARANTOR_NO IS NOT NULL ----20190625���������˵��²�ѯ���Ϊ0 
 											 								GROUP BY T3.C_GUARANTOR_PAPERTYPE,T3.C_GUARANTOR_NO)
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
        				 TO_DATE(p_data_dt_str,'YYYYMMDD')     																	AS DataDate            		--��������
        				,p_data_dt_str                         																	AS DataNo              		--������ˮ��
        				,'LC' || T1.CERTTYPE || T1.CERTID																				AS ClientID            		--��������ID
        				,'LC' || T1.CERTTYPE || T1.CERTID																				AS SourceClientID      		--Դ��������ID
        				,'LC'                                 																	AS SSysID              		--ԴϵͳID
        				,T1.CUSTOMERNAME																												AS ClientName          		--������������
        				,'9998'							              																	AS SOrgID              		--Դ����ID
        				,'��������'								      																	AS SOrgName            		--Դ��������
        				,'1'							              																	AS OrgSortNo           		--�������������
        				,'9998'								             																	AS OrgID               		--��������ID
        				,'��������'								      																	AS OrgName             		--������������
        				,T1.INDUSTRYTYPE										  																	AS IndustryID          		--������ҵ����
        				,T4.ITEMNAME								          																	AS IndustryName        		--������ҵ����
        				,T10.DITEMNO																														AS ClientType          		--�����������
        				,T11.DITEMNO																														AS ClientSubType       		--��������С��
        				,CASE WHEN T1.COUNTRYCODE = 'CHN' THEN '01'
        				 ELSE '02'
        				 END	                                 																	AS RegistState         		--ע����һ����
        				,T7.RATINGRESULT                       																	AS RCERating           		--����ע����ⲿ����
        				,'01'                                  																	AS RCERAgency          		--����ע����ⲿ��������
        				,T1.CERTID							              																	AS OrganizationCode    		--��֯��������
        				,CASE WHEN T1.CERTID = '91522301573318868K' OR REPLACE(T1.CERTID,'-','') = '57331886-8' THEN '1'
        				 ELSE '0'
        				 END	                                																	AS ConsolidatedSCFlag  		--�Ƿ񲢱��ӹ�˾
        				,'0'                                  																	AS SLClientFlag        		--רҵ����ͻ���ʶ
        				,''	                                  																	AS SLClientType        		--רҵ����ͻ�����
        				,''                                   																	AS ExpoCategoryIRB     		--��������¶���
        				,T8.MODELID                           																	AS ModelID             		--ģ��ID
        				,T8.PDLEVEL											                         								AS MODELIRATING        		--ģ���ڲ�����
                ,T8.PD									                                 								AS MODELPD             		--ģ��ΥԼ����
                ,T8.PDADJLEVEL										                      								AS IRATING             		--�ڲ�����
                ,T8.PD                            									     								AS PD                  		--ΥԼ����
        				,CASE WHEN T8.PDADJLEVEL = '0116' THEN '1' ELSE '0' END									AS DefaultFlag         		--ΥԼ��ʶ
        				,DECODE(NVL(T2.NEWDEFAULTFLAG,'1'),'0','1','0')													AS NewDefaultFlag      		--����ΥԼ��ʶ
        				,CASE WHEN T8.PDADJLEVEL = '0116' THEN TO_DATE(T8.PDVAVLIDDATE,'YYYYMMDD')
                 ELSE NULL
                 END														      																	AS DefaultDate         		--ΥԼʱ��
        				,''															      																	AS ClientERating       		--���������ⲿ����
        				,'0'                                  																	AS CCPFlag             		--���뽻�׶��ֱ�ʶ
        				,'0'                                   																	AS QualCCPFlag         		--�Ƿ�ϸ����뽻�׶���
        				,'0'                                   																	AS ClearMemberFlag     		--�����Ա��ʶ
        				,T2.SCOPE							                																	AS CompanySize         		--��ҵ��ģ
        				,NVL(T2.ISSUPERVISESTANDARSMENT,'0')   																	AS SSMBFlag            		--��׼С΢��ҵ��ʶ
        				,'0'											            																	AS SSMBFlagSTD         		--Ȩ�ط���׼С΢��ҵ��ʶ
        				,T2.AVEFINANCESUM                      																	AS AnnualSale          		--��˾�ͻ������۶�
        				,T1.COUNTRYCODE																													AS CountryCode            --ע����Ҵ���
        				,''																																			AS MSMBFlag								--���Ų�΢С��ҵ��ʶ

    FROM 				TMP_PLN_CUST_INFO T1
    LEFT JOIN		TEMP_CUST_INFO T2
    ON					REPLACE(T1.CERTID,'-','') = REPLACE(T2.CERTID,'-','')
    LEFT JOIN		RWA.CODE_LIBRARY T4
    ON					T1.INDUSTRYTYPE = T4.ITEMNO
    AND					T4.CODENO = 'IndustryType'
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T7
    ON					T1.COUNTRYCODE = T7.COUNTRYCODE
    AND					T7.ISINUSE = '1'
    LEFT JOIN		TMP_CUST_IRATING T8
    ON					REPLACE(T1.CERTID,'-','') = REPLACE(T8.ORGCERTCODE,'-','')
    LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T10
	  ON					T1.CLIENTTYPE = T10.SITEMNO
	  AND					T10.SCODENO = 'ClientCategory'
	  AND					T10.SYSID = 'LC'
	  LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T11
	  ON					T1.CLIENTSUBTYPE = T11.SITEMNO
	  AND					T11.SCODENO = 'ClientCategory'
	  AND					T11.SYSID = 'LC'
    WHERE				'LC' || T1.CERTTYPE || T1.CERTID NOT IN (SELECT CLIENTID FROM RWA_DEV.RWA_LC_CLIENT)
    ;

		COMMIT;

		dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_CLIENT',cascade => true);


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_CLIENT;
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_LC_CLIENT���в�������Ϊ��' || v_count || '��');
    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;

    commit;
    --�����쳣
    EXCEPTION WHEN OTHERS THEN
    		--DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '����ʹ�ϵͳ-��������('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
				RETURN;
END PRO_RWA_LC_CLIENT;
/

