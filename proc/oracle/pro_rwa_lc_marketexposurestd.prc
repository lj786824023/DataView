CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_MARKETEXPOSURESTD(
														p_data_dt_str IN  VARCHAR2, --��������
                            p_po_rtncode  OUT VARCHAR2, --���ر��
                            p_po_rtnmsg   OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_LC_MARKETEXPOSURESTD
    ʵ�ֹ���:���ϵͳ-ծȯ���Ͷ��-�г�����-��׼����¶��(������Դ����ծȯͷ�����ծȯ��Ϣ��ȫ������RWA�г�������ƽӿڱ�ծȯ��׼����¶����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2016-04-14
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_LC_TRADBONDPOSITION|����ծȯͷ���
    Դ  ��2 :RWA_DEV.RWA_LC_BONDINFO|ծȯ��Ϣ��
    Դ  ��3 :RWA.RWA_WS_FCII_BOND|ծȯ���Ͷ�ʲ�¼��
    Դ	��4 :RWA.ORG_INFO|RWA������Ϣ��
    Դ  ��5 :RWA.RWA_WP_COUNTRYRATING|����������
    Դ  ��6 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Ŀ���1 :RWA_DEV.RWA_LC_MARKETEXPOSURESTD|�г����ձ�׼�����ձ�¶��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_MARKETEXPOSURESTD';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --���Ŀ����е�ԭ�м�¼
    --DELETE FROM RWA_EI_MARKETEXPOSURESTD WHERE DATADATE=TO_DATE(p_data_dt_str,'yyyyMMdd');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_MARKETEXPOSURESTD';


    --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾�г����ձ�׼�����ձ�¶��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    INSERT INTO RWA_DEV.RWA_LC_MARKETEXPOSURESTD(
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
        				TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str											 				 AS DATANO                   --������ˮ��
        				,T1.POSITIONID                   						 AS EXPOSUREID               --���ձ�¶ID(ͷ��ID:ֱ��ӳ��)
        				,'02'                            						 AS BOOKTYPE                 --�˻����(Ĭ�ϣ������˻� BookType �˻����02 �����˻�)
        				,T1.BONDID                       						 AS INSTRUMENTSID            --���ڹ���ID(ծȯID:ֱ��ӳ��)
        				,T1.INSTRUMENTSTYPE              						 AS INSTRUMENTSTYPE          --���ڹ�������(���ڹ�������:ֱ��ӳ��)
        				,T4.SORTNO														 			 AS ORGSORTNO								 --���������
        				,T1.TRANORGID                    						 AS ORGID                    --��������ID(���׻���ID:ֱ��ӳ��)
        				,T4.ORGNAME	                    						 AS ORGNAME                  --������������(���׻���ID:ת��ӳ��)
        				,'01'                            						 AS ORGTYPE                  --������������(Ĭ�ϣ�01 ���ڻ���"01 ���ڻ���02 �������")
        				,'01'                              					 AS MARKETRISKTYPE           --�г���������(����ӳ��)��Ĭ�ϣ����ʷ���(01)
        				,'01'                            						 AS INTERATERISKTYPE         --���ʷ�������(Ĭ�ϣ�ծȯ InteRateRiskType ���ʷ�������:01 ծȯ)
        				,''                              						 AS EQUITYRISKTYPE           --��Ʊ��������(Ĭ�ϣ�NULL)
        				,''                              						 AS EXCHANGERISKTYPE         --����������(Ĭ�ϣ�NULL)
        				,''                              						 AS COMMODITYNAME            --��Ʒ��������(Ĭ�ϣ�NULL)
        				,''                              						 AS OPTIONRISKTYPE           --��Ȩ��������(Ĭ�ϣ�NULL)
        				,T2.ISSUERID                     						 AS ISSUERID                 --������ID(������ID:ֱ��ӳ��
        				,T2.ISSUERNAME                   						 AS ISSUERNAME               --����������(����������:ֱ��ӳ��)
        				,T2.ISSUERTYPE                   						 AS ISSUERTYPE               --�����˴���(�����˴���:ֱ��ӳ��)
        				,T2.ISSUERSUBTYPE                						 AS ISSUERSUBTYPE            --������С��(������С��:ֱ��ӳ��)
        				,T2.ISSUERREGISTSTATE            						 AS ISSUERREGISTSTATE        --������ע�����(������ע�����:ֱ��ӳ��)
        				,T5.RATINGRESULT		            						 AS ISSUERRCERATING          --�����˾���ע����ⲿ����(������ע�����:���ݷ�����ע����ң�ȡ�����������ж�Ӧ������)
        				,T2.ISSUERSMBFLAG                						 AS SMBFLAG                  --С΢��ҵ��ʶ(������С΢��ҵ��ʶ:ֱ��ӳ��)
        				,'0'                             						 AS UNDERBONDFLAG            --�Ƿ����ծȯ(Ĭ�ϣ��� 1 �� 0 ��)
        				,''                              						 AS PAYMENTDATE              --�ɿ���(Ĭ�ϣ�NULL)
        				,T2.BONDTYPE                     						 AS SECURITIESTYPE           --֤ȯ���(ծȯ����:ת��ӳ��)
        				,T2.BONDISSUEINTENT              						 AS BONDISSUEINTENT          --ծȯ����Ŀ��(ծȯ����Ŀ��:ֱ��ӳ��)
        				,CASE WHEN T3.C_BONDDETAIL_TYPE IN ('02','03','04') THEN '02'
        				 ELSE '01'
        				 END											         			 		 AS CLAIMSLEVEL              --ծȨ����(ծȨ����:ֱ��ӳ��)
        				,T2.REABSFLAG                    						 AS REABSFLAG                --���ʲ�֤ȯ����ʶ(���ʲ�֤ȯ����ʶ:ֱ��ӳ��)
        				,T2.ORIGINATORFLAG               						 AS ORIGINATORFLAG           --�Ƿ������(�Ƿ������:ֱ��ӳ��)
        				,T2.ERATING                      						 AS SECURITIESERATING        --֤ȯ�ⲿ����(�ⲿ����:ֱ��ӳ��)
        				,''                              						 AS STOCKCODE                --��Ʊ/��ָ����(Ĭ�ϣ�NULL)
        				,''                              						 AS STOCKMARKET              --�����г�(Ĭ�ϣ�NULL)
        				,''                              						 AS EXCHANGEAREA             --���׵���(Ĭ�ϣ�NULL)
        				,''                              						 AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���(Ĭ�ϣ�NULL)
        				,'0'                             						 AS OPTIONUNDERLYINGFLAG     --�Ƿ���Ȩ��������(Ĭ�ϣ��� 1 �� 0 ��)
        				,''																		 			 AS OPTIONUNDERLYINGNAME		 --��Ȩ������������
        				,''                              						 AS OPTIONID                 --��Ȩ����ID(Ĭ�ϣ�NULL)
        				,''                              						 AS VOLATILITY               --������(Ĭ�ϣ�NULL)
        				,T2.STARTDATE                    						 AS STARTDATE                --��ʼ����(��ʼ����:ֱ��ӳ��)
        				,T2.DUEDATE                      						 AS DUEDATE                  --��������(��������:ֱ��ӳ��)
        				,T2.ORIGINALMATURITY             						 AS ORIGINALMATURITY         --ԭʼ����(ԭʼ����:ֱ��ӳ��)
        				,T2.RESIDUALM                    						 AS RESIDUALM                --ʣ������(ʣ������:ֱ��ӳ��)
        				,T2.NEXTREPRICEDATE              						 AS NEXTREPRICEDATE          --�´��ض�����(�´��ض�����:ֱ��ӳ��)
        				,T2.NEXTREPRICEM                 						 AS NEXTREPRICEM             --�´��ض�������(�´��ض�������:ֱ��ӳ��)
        				,T2.RATETYPE                     						 AS RATETYPE                 --��������(��������:ֱ��ӳ��)
        				,T2.EXECUTIONRATE                						 AS COUPONRATE               --Ʊ������(ִ������:ֱ��ӳ��)
        				,T2.MODIFIEDDURATION             						 AS MODIFIEDDURATION         --��������(��������:ֱ��ӳ��)
        				,'01'                            						 AS POSITIONTYPE             --ͷ������(Ĭ�ϣ���ͷ PositionType ͷ�����ԣ�01 ��ͷ)
        				,T1.BOOKBALANCE                  						 AS POSITION                 --ͷ��(�������:ֱ��ӳ��)
        				,T1.CURRENCY                     						 AS CURRENCY                 --����(����:ֱ��ӳ��)
        				,''																					 AS OptionUnderlyingType		 --��Ȩ������������

    FROM				RWA_DEV.RWA_LC_TRADBONDPOSITION T1	             		 					--����ծȯͷ����Ϣ��
	  INNER JOIN 	RWA_DEV.RWA_LC_BONDINFO T2											 							--ծȯ��Ϣ��
	  ON 					T1.BONDID = T2.BONDID
	  LEFT JOIN		RWA_DEV.ZGS_ATBOND T3
	  ON					T1.BONDID = T3.C_BOND_CODE
	  AND					T3.DATANO = p_data_dt_str
	  LEFT  JOIN	RWA.ORG_INFO T4																								--RWA������Ϣ��
	  ON					T1.TRANORGID = T4.ORGID
	  LEFT  JOIN	RWA.RWA_WP_COUNTRYRATING T5																		--����������
	  ON					T3.C_ISSUER_REGCOUNTRY_CODE = T5.COUNTRYCODE
	  AND					T5.ISINUSE = '1'
	  ;

	  COMMIT;

	  dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_MARKETEXPOSURESTD',cascade => true);

    --DBMS_OUTPUT.PUT_LINE('���������롾�г����ձ�׼�����ձ�¶��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_MARKETEXPOSURESTD;
    --Dbms_output.Put_line('RWA_DEV.RWA_LC_MARKETEXPOSURESTD��ǰ��������ϵͳ-ծȯ���Ͷ��(�г�����)-��׼����¶��¼Ϊ: ' || v_count || ' ��');



    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;

    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '�г����ձ�׼����¶��Ϣ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_LC_MARKETEXPOSURESTD;
/

