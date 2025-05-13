CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZQ_MARKETEXPOSURESTD(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_ZQ_MARKETEXPOSURESTD
    ʵ�ֹ���:����ϵͳ-ծȯ-�г�����-��׼����¶��(������Դ����ծȯͷ�����ծȯ��Ϣ��ȫ������RWA�г�����ծȯ�ӿڱ�ծȯ��׼����¶����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-12
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_ZQ_TRADBONDPOSITION|����ծȯͷ���
    Դ  ��2 :RWA_DEV.RWA_ZQ_BONDINFO|ծȯ��Ϣ��
    Դ  ��3 :RWA.ORG_INFO|RWA������
    Դ  ��4 :RWA_DEV.FNS_BND_INFO_B|����ϵͳծȯ��Ϣ��
    Դ  ��5 :RWA.RWA_WP_COUNTRYRATING|����������
    Դ  ��6 :RWA.RWA_WS_BONDTRADE|ծȯͶ�ʲ�¼��Ϣ��
    Դ  ��7 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Ŀ���  :RWA_DEV.RWA_ZQ_MARKETEXPOSURESTD|����ϵͳծȯ��׼����¶��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZQ_MARKETEXPOSURESTD';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZQ_MARKETEXPOSURESTD';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ϵͳ-ծȯ
    INSERT INTO RWA_DEV.RWA_ZQ_MARKETEXPOSURESTD(
                 DataDate                              --��������
                ,DataNo                                --������ˮ��
                ,ExposureID                            --���ձ�¶ID
                ,BookType                              --�˻����
                ,InstrumentsID                         --���ڹ���ID
                ,InstrumentsType                       --���ڹ�������
                ,OrgSortNo                             --�������������
                ,OrgID                                 --��������ID
                ,OrgName                               --������������
                ,OrgType                               --������������
                ,MarketRiskType                        --�г���������
                ,InteRateRiskType                      --���ʷ�������
                ,EquityRiskType                        --��Ʊ��������
                ,ExchangeRiskType                      --����������
                ,CommodityName                         --��Ʒ��������
                ,OptionRiskType                        --��Ȩ��������
                ,IssuerID                              --������ID
                ,IssuerName                            --����������
                ,IssuerType                            --�����˴���
                ,IssuerSubType                         --������С��
                ,IssuerRegistState                     --������ע�����
                ,IssuerRCERating                       --�����˾���ע����ⲿ����
                ,SMBFlag                               --С΢��ҵ��ʶ
                ,UnderBondFlag                         --�Ƿ����ծȯ
                ,PaymentDate                           --�ɿ���
                ,SecuritiesType      								   --֤ȯ���
                ,BondIssueIntent                       --ծȯ����Ŀ��
                ,ClaimsLevel                           --ծȨ����
                ,ReABSFlag                             --���ʲ�֤ȯ����ʶ
                ,OriginatorFlag                        --�Ƿ������
                ,SecuritiesERating                     --֤ȯ�ⲿ����
                ,StockCode                             --��Ʊ/��ָ����
                ,StockMarket                           --�����г�
                ,ExchangeArea                          --���׵���
                ,StructuralExpoFlag                    --�Ƿ�ṹ�Գ���
                ,OptionUnderlyingFlag                  --�Ƿ���Ȩ��������
                ,OptionUnderlyingName                  --��Ȩ������������
                ,OptionID                              --��Ȩ����ID
                ,Volatility                            --������
                ,StartDate                             --��ʼ����
                ,DueDate                               --��������
                ,OriginalMaturity                      --ԭʼ����
                ,ResidualM                             --ʣ������
                ,NextRepriceDate                       --�´��ض�����
                ,NextRepriceM                          --�´��ض�������
                ,RateType                              --��������
                ,CouponRate                            --Ʊ������
                ,ModifiedDuration                      --��������
                ,PositionType                          --ͷ������
                ,Position                              --ͷ��
                ,Currency            								   --����
                ,OptionUnderlyingType									 --��Ȩ������������
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                	--��������
                ,p_data_dt_str												     	 AS DataNo                  	--������ˮ��
                ,T1.POSITIONID                    				 	 AS ExposureID              	--���ձ�¶ID
                ,'02'					                           		 AS BookType                	--�˻����                					 Ĭ�ϣ������˻�(02)
                ,T1.BONDID         	                         AS InstrumentsID           	--���ڹ���ID
                ,T1.INSTRUMENTSTYPE                    			 AS InstrumentsType         	--���ڹ�������
                ,T3.SORTNO														 			 AS OrgSortNo        			 		--�������������
                ,T1.TRANORGID			 						               AS OrgID                   	--��������ID
                ,T3.ORGNAME										               AS OrgName                 	--������������
                ,'01'						                             AS OrgType                 	--������������            					 Ĭ�ϣ����ڻ���(01)
                ,'01'			                              		 AS MarketRiskType          	--�г���������            					 Ĭ�ϣ����ʷ���(01)
                ,'01'								                         AS InteRateRiskType        	--���ʷ�������            					 Ĭ�ϣ�ծȯ(01)
                ,''												                   AS EquityRiskType          	--��Ʊ��������            					 Ĭ�ϣ���
                ,''							                             AS ExchangeRiskType        	--����������            					 Ĭ�ϣ���
                ,''																					 AS CommodityName      		 		--��Ʒ��������            					 Ĭ�ϣ���
                ,''                                          AS OptionRiskType          	--��Ȩ��������            					 Ĭ�ϣ���
                ,T2.ISSUERID                            		 AS IssuerID                	--������ID
                ,T2.ISSUERNAME	                             AS IssuerName              	--����������
                ,T2.ISSUERTYPE	                             AS IssuerType              	--�����˴���
                ,T2.ISSUERSUBTYPE                            AS IssuerSubType           	--������С��
                ,T2.ISSUERREGISTSTATE                        AS IssuerRegistState       	--������ע�����
                ,NVL(T5.RATINGRESULT,'0124')                 AS IssuerRCERating         	--�����˾���ע����ⲿ����
                ,T2.ISSUERSMBFLAG	                           AS SMBFlag                 	--С΢��ҵ��ʶ
                ,'0'                                         AS UnderBondFlag           	--�Ƿ����ծȯ            					 Ĭ�ϣ���(0)
                ,''	                                         AS PaymentDate             	--�ɿ���                  					 Ĭ�ϣ���
                ,T2.BONDTYPE                            		 AS SecuritiesType          	--֤ȯ���
                ,T2.BONDISSUEINTENT													 AS BondIssueIntent    		 		--ծȯ����Ŀ��
                ,CASE WHEN T6.BOND_TYPE2 = '20' THEN '02'
                ELSE '01'
                END	                                         AS ClaimsLevel               --ծȨ����                					 ծȯ����2���μ�ծȯ(20)����ծȨ���𣽴μ�ծȨ(02)������Ϊ�߼�ծȨ(01)
                ,T2.REABSFLAG                                AS ReABSFlag                 --���ʲ�֤ȯ����ʶ
                ,T2.ORIGINATORFLAG                           AS OriginatorFlag            --�Ƿ������
                ,T2.ERATING                                  AS SecuritiesERating         --֤ȯ�ⲿ����
                ,''                                          AS StockCode                 --��Ʊ/��ָ����           					 Ĭ�ϣ���
                ,''                                          AS StockMarket               --�����г�                					 Ĭ�ϣ���
                ,''                                          AS ExchangeArea              --���׵���                					 Ĭ�ϣ���
                ,''                                          AS StructuralExpoFlag        --�Ƿ�ṹ�Գ���          					 Ĭ�ϣ���
                ,'0'                                         AS OptionUnderlyingFlag      --�Ƿ���Ȩ��������        					 Ĭ�ϣ���(0)
                ,''                                          AS OptionUnderlyingName      --��Ȩ������������        					 Ĭ�ϣ���
                ,''                                          AS OptionID                  --��Ȩ����ID              					 Ĭ�ϣ���
                ,NULL                                        AS Volatility                --������                  					 Ĭ�ϣ���
                ,T2.STARTDATE                                AS StartDate                 --��ʼ����
                ,T2.DUEDATE                                  AS DueDate                   --��������
                ,T2.ORIGINALMATURITY                         AS OriginalMaturity          --ԭʼ����
                ,T2.RESIDUALM                                AS ResidualM                 --ʣ������
                ,T2.NEXTREPRICEDATE                          AS NextRepriceDate           --�´��ض�����
                ,T2.NEXTREPRICEM                             AS NextRepriceM              --�´��ض�������
                ,T2.RATETYPE                                 AS RateType                  --��������
                ,T2.EXECUTIONRATE                            AS CouponRate                --Ʊ������
                ,T2.MODIFIEDDURATION                         AS ModifiedDuration          --��������
                ,'01'                                        AS PositionType              --ͷ������                					 Ĭ�ϣ���ͷ(01)
                ,T1.BOOKBALANCE                              AS Position                  --ͷ��
                ,T1.CURRENCY                                 AS Currency                  --����
                ,''																					 AS OptionUnderlyingType		 --��Ȩ������������

    FROM				RWA_DEV.RWA_ZQ_TRADBONDPOSITION T1	             		 					--����ծȯͷ����Ϣ��
	  INNER JOIN 	RWA_DEV.RWA_ZQ_BONDINFO T2											 							--ծȯ��Ϣ��
	  ON 					T1.BONDID = T2.BONDID
	  AND					T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  LEFT  JOIN	RWA.ORG_INFO T3																								--RWA������Ϣ��
	  ON					T1.TRANORGID = T3.ORGID
	  LEFT JOIN		RWA_DEV.NCM_CUSTOMER_INFO T4																	--ͳһ�ͻ���Ϣ��
		ON					T2.ISSUERID = T4.CUSTOMERID
		AND					T4.DATANO = p_data_dt_str
	  LEFT  JOIN	RWA.RWA_WP_COUNTRYRATING T5																		--����������
	  ON					T4.COUNTRYCODE = T5.COUNTRYCODE
	  AND					T5.ISINUSE = '1'
	  LEFT	JOIN	RWA_DEV.FNS_BND_INFO_B T6																			--����ϵͳծȯ��Ϣ��
	  ON					T1.POSITIONID = T6.BOND_ID
	  AND					T6.DATANO = p_data_dt_str
	  WHERE				T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZQ_MARKETEXPOSURESTD',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZQ_MARKETEXPOSURESTD;
    --Dbms_output.Put_line('RWA_DEV.RWA_ZQ_MARKETEXPOSURESTD��ǰ����Ĳ���ϵͳ-ծȯ(�г�����)-��׼����¶��¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '�г����ձ�׼����¶��Ϣ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZQ_MARKETEXPOSURESTD;
/

